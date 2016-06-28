#!/usr/bin/python3
# -*- coding: utf-8 -*-


# pylint: disable=C0111,
#        Missing docstring

import argparse
import json
import logging
import sys

import dask.bag as db
import pandas as pd
import requests

from copy import deepcopy

from dask.threaded import get
from dask.diagnostics import ProgressBar

from grimoire.utils import config_logging
from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.ocean.elastic import ElasticOcean
from grimoire.elk.git import GitEnrich
from grimoire.elk.sortinghat import SortingHat

from perceval.utils import str_to_datetime, datetime_to_utc


def get_params():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    ElasticOcean.add_params(parser)

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help="show  debug messages")
    parser.add_argument('--index', default="git_enrich", help="Ocean index name (default git_enrich)")
    parser.add_argument('--db-projects-map', help="Projects Mapping DB")
    parser.add_argument('--db-sortinghat', help="SortingHat DB")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

import time

def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print ('%r %2.2f sec' % \
              (method.__name__, te-ts))
        return result

    return timed


@timeit
def identities2sh(items, db_sortinghat = None):
    logging.debug("Uploading identities to SH")

    backend_name = "git"

    if db_sortinghat:
        enrich_backend = GitEnrich(None, None, db_sortinghat)
        # First we add all new identities to SH
        item_count = 0
        new_identities = []

        for item in items:
            item_count += 1
            # Get identities from new items to be added to SortingHat
            identities = enrich_backend.get_identities(item)
            for identity in identities:
                if identity not in new_identities:
                    new_identities.append(identity)
            if item_count % 1000 == 0:
                logging.debug("Processed %i items identities (%i identities)", \
                               item_count, len(new_identities))
        logging.debug("TOTAL ITEMS: %i", item_count)

        logging.info("Total new identities to be checked %i", len(new_identities))

        SortingHat.add_identities(enrich_backend.sh_db, new_identities, backend_name)

    logging.debug("Completed uploading identities to SH.")

    return items


@timeit
def enrich_git_pandas(items):
    logging.debug("Doing Pandas git enrich")
    enriched_items = []  # items from author with new date fields added

    df = pd.DataFrame(items)

    logging.debug("Completed Pandas git enrich.")

    return (enriched_items)


@timeit
def enrich_git(items, db_projects_map=None, db_sortinghat=None):
    logging.debug("Doing git enrich")
    enriched_items = []  # items from author with new date fields added

    git_enricher = GitEnrich(None, db_projects_map, db_sortinghat)

    for item in items:
        enriched_items.append(git_enricher.get_rich_commit(item))

    logging.debug("Completed git enrich.")

    return (enriched_items)

@timeit
def load_data(es, es_index):
    """ Load the data from ES """
    logging.info("Loading data from %s", es_index)
    items = []

    try:
        elastic_ocean = ElasticSearch(es, es_index)
        ocean = ElasticOcean(None)
        ocean.set_elastic(elastic_ocean)

        for item in ocean:
            items.append(item)
            # if len(items) % 100 == 0: # debug
            #   break
            if len(items) % 1000 == 0:
                logging.debug("Items loaded: %i", len(items))
        logging.debug("Items loaded: %i", len(items))

        # logging.info("Size of items: %i", sys.getsizeof(items))
        return items

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

@timeit
def load_data_bag(items):
    """ Build the dask bag with the items """

    # npartitions=100 by default
    return db.from_sequence(items)

@timeit
def process_data_bag(items_bg):
    """ Build the dask bag with the items """

    def get_logins(x):
        """The key for foldby, like a groupby key. Get the Author from a Commit"""
        return x['data']['Author']

    def binop_max(max_date, x):
        """Get the max AuthorDate"""
        x_date = str_to_datetime(x['data']['AuthorDate'])
        if x_date > max_date:
            max_date = x_date
        return max_date

    def combine_max(date1, date2):
        """This combines dates from commits"""
        max_date = date1
        if date2 > date1:
            max_date = date2
        return max_date

    def binop_min(min_date, x):
        """Get the max AuthorDate"""
        x_date = str_to_datetime(x['data']['AuthorDate'])
        if not min_date or x_date < min_date:
            min_date = x_date
        return min_date

    def combine_min(date1, date2):
        """This combines dates from commits"""
        min_date = date1
        if date2 < date1:
            min_date = date2
        return min_date


    dates_max = items_bg.foldby(get_logins, binop_max, str_to_datetime('1970-01-01'), combine=combine_max)
    dates_min = items_bg.foldby(get_logins, binop_min, None, combine=combine_min)

    with ProgressBar():
        dates_max.compute()
    with ProgressBar():
        dates_min.compute()


@timeit
def lowercase_fields(items):
    new_items = []
    for item in items:
        new_item = deepcopy(item)
        for key in item.keys():
            new_item[key.lower()] = new_item.pop(key)
        for dkey in item['data'].keys():
            if dkey.lower() in ['commit', 'author', 'authordate', 'commitdate']:
                # We have Commit and commit in raw index!
                # Author and AuthorDate and CommitDate are used in SH
                continue
            new_item['data'][dkey.lower()] = new_item['data'].pop(dkey)
        new_items.append(new_item)
    return new_items

@timeit
def utc_dates(items):
    # First step, detect all date fields in data
    dates_fields = []
    for dkey in items[0]['data']:
        try:
            str_to_datetime(items[0]['data'][dkey])
            dates_fields.append(dkey)
        except Exception:
            pass

    # Update all items converting the dates to UTC + tz
    new_items = []
    for item in items:
        new_item = deepcopy(item)
        for dkey in item['data'].keys():
            if dkey in dates_fields:
                # date in utc
                date_field = str_to_datetime(new_item['data'][dkey])
                new_item['data'][dkey+"_utc"] = datetime_to_utc(date_field).isoformat()
                # timezone in seconds
                new_item['data'][dkey+"_tz"] = date_field.strftime("%z")
        new_items.append(new_item)

    return items

@timeit
def write_data(es, es_index, eitems):
    """ Write the data to ES """
    logging.info("Writing %i items data with demography info to %s", len(eitems), es_index)


    try:
        print("WRITE: ", es, es_index)
        elastic = ElasticSearch(es, es_index)
        url = elastic.url+'/items/_bulk'
        max_items = elastic.max_items_bulk
        current = 0
        bulk_json = ""


        for eitem in eitems:
            if current >= max_items:
                requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0
            data_json = json.dumps(eitem)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (eitem["ocean-unique-id"])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        requests.put(url, data = bulk_json)
    except Exception as ex:
        logging.error("Can't write data to %s", es)
        print(ex)



if __name__ == '__main__':
    args = get_params()
    config_logging(args.debug)

    ES_IN = args.elastic_url
    ES_IN_INDEX = args.index
    ES_OUT = ES_IN
    ES_OUT_INDEX = ES_IN_INDEX
    SORTINGHAT_DB = args.db_sortinghat
    PROJECTS_DB = args.db_projects_map

    DASK_GRAPH = {
        'es_read': ES_IN,
        'es_write': ES_OUT,
        'es_index_read': ES_IN_INDEX,
        'es_es_index_write': ES_OUT_INDEX,
        'es_pandas_index_write': ES_OUT+"_pandas",
        'load': (load_data, 'es_read', 'es_index_read'),
        'load_bag': (load_data_bag, 'load'),
        'process_data_bag': (process_data_bag, 'load_bag'),
        'lowercase_fields' : (lowercase_fields, 'load'),
        'utc_dates' : (utc_dates, 'lowercase_fields'),
        'sortinghat': (identities2sh, 'utc_dates', SORTINGHAT_DB),
        'enrich_es': (enrich_git, 'sortinghat', PROJECTS_DB, SORTINGHAT_DB),
        'enrich_pandas': (enrich_git_pandas, 'sortinghat'),
        'write_es_es': (write_data, 'es_write','es_es_index_write', 'enrich_es'),
        'write_es_pandas': (write_data, 'es_pandas_index_write', 'enrich_pandas')
    }

    # get(DASK_GRAPH, ['write_es_es','write_es_pandas'])
    # get(DASK_GRAPH, ['write_es_es'])
    get(DASK_GRAPH, ['process_data_bag'])
