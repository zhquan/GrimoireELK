#!/usr/bin/python3
# -*- coding: utf-8 -*-


# pylint: disable=C0111,
#        Missing docstring

import argparse
import json
import logging
import sys

import numpy as np
import pandas as pd


import requests

from copy import deepcopy

from dask.threaded import get

from dateutil import parser

from grimoire.utils import config_logging
from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.ocean.elastic import ElasticOcean
from grimoire.elk.git import GitEnrich
from grimoire.elk.sortinghat import SortingHat


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
            if len(items) % 3 == 0: # debug
                break
            if len(items) % 1000 == 0:
                logging.debug("Items loaded: %i", len(items))
        logging.debug("Items loaded: %i", len(items))

        # logging.info("Size of items: %i", sys.getsizeof(items))
        return items

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

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

def utc_dates(items):
    # First step, detect all date fields in data
    dates_fields = []
    for dkey in items[0]['data']:
        try:
            parser.parse(items[0]['data'][dkey])
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
                new_item['data'][dkey+"_utc"] = new_item['data'][dkey]
                # timezone in seconds
                new_item['data'][dkey+"_tz"] = 0
        new_items.append(new_item)

    return items

def write_data(es_index, eitems):
    """ Write the data to ES """
    logging.info("Writing %i items data with demography info to %s", len(eitems), es_index)

    for item in eitems:
        print (json.dumps(item, indent=4, sort_keys=True))
        break


if __name__ == '__main__':
    args = get_params()
    config_logging(args.debug)

    ES_IN = args.elastic_url
    ES_IN_INDEX = args.index
    SORTINGHAT_DB = args.db_sortinghat
    PROJECTS_DB = args.db_projects_map
    ES_OUT = ''

    DASK_GRAPH = {
        'es_read': ES_IN,
        'es_index_read': ES_IN_INDEX,
        'es_es_index_write': ES_OUT,
        'es_pandas_index_write': ES_OUT+"_pandas",
        'load': (load_data, 'es_read', 'es_index_read'),
        'lowercase_fields' : (lowercase_fields, 'load'),
        'utc_dates' : (utc_dates, 'lowercase_fields'),
        'sortinghat': (identities2sh, 'utc_dates', SORTINGHAT_DB),
        'enrich_es': (enrich_git, 'sortinghat', PROJECTS_DB, SORTINGHAT_DB),
        'enrich_pandas': (enrich_git_pandas, 'sortinghat'),
        'write_es_es': (write_data, 'es_es_index_write', 'enrich_es'),
        'write_es_pandas': (write_data, 'es_pandas_index_write', 'enrich_pandas')
    }

    get(DASK_GRAPH, ['write_es_es','write_es_pandas'])
