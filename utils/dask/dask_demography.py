#!/usr/bin/python3
# -*- coding: utf-8 -*-


# pylint: disable=C0111,
#        Missing docstring

import argparse
import json
import logging
import sys

import dask.dataframe as dd
import dask.bag as db
import numpy as np
import pandas as pd


import requests

from dask.threaded import get

from dateutil import parser

from grimoire.utils import config_logging
from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.ocean.elastic import ElasticOcean

def get_params():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    ElasticOcean.add_params(parser)

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help="show  debug messages")
    parser.add_argument('--index', default="git_enrich", help="Ocean index name (default git_enrich)")

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
def enrich_demography_pandas(items, from_date=None):

    logging.debug("Doing Pandas demography enrich")
    author_items = []  # items from author with new date fields added

    df = pd.DataFrame(items)
    df.info()
    logging.debug("Pandas LOADED items")
    gb = df.groupby("Author").agg({'author_date' : [np.min, np.max]})
    logging.debug("Pandas GROUP BY completed")


    # Time to update items with the min and max data
    for item in items:
        item.update(
            {"author_min_date":gb.loc[item['Author']]['author_date']['amin'],
             "author_max_date":gb.loc[item['Author']]['author_date']['amax']}
        )
        author_items.append(item)
    logging.debug("Pandas UPDATED items")

    return (author_items)

@timeit
def enrich_demography_es(es, es_index, items, from_date=None):
    logging.debug("Doing Elastic demography enrich from %s", es+"/"+es_index)
    ocean = None

    try:
        elastic_ocean = ElasticSearch(es, es_index)
        ocean = ElasticOcean(None)
        ocean.set_elastic(elastic_ocean)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    if from_date:
        logging.debug("Demography since: %s", from_date)

    query = ''
    if from_date:
        date_field = ocean.get_field_date()
        from_date = from_date.isoformat()

        filters = '''
        {"range":
            {"%s": {"gte": "%s"}}
        }
        ''' % (date_field, from_date)

        query = """
        "query": {
            "bool": {
                "must": [%s]
            }
        },
        """ % (filters)


    # First, get the min and max commit date for all the authors
    es_query = """
    {
      %s
      "size": 0,
      "aggs": {
        "author": {
          "terms": {
            "field": "Author",
            "size": 0
          },
          "aggs": {
            "min": {
              "min": {
                "field": "author_date"
              }
            },
            "max": {
              "max": {
                "field": "author_date"
              }
            }
          }
        }
      }
    }
    """ % (query)

    r = requests.post(ocean.elastic.index_url+"/_search", data=es_query)
    logging.debug("ES GROUP BY completed")

    authors = r.json()['aggregations']['author']['buckets']

    author_items = []  # items from author with new date fields added
    nauthors_done = 0
    author_query = """
    {
        "query": {
            "bool": {
                "must": [
                    {"term":
                        { "Author" : ""  }
                    }
                    ]
            }
        }

    }
    """
    author_query_json = json.loads(author_query)

    nauthors_done = 0
    for author in authors:
        author_query_json['query']['bool']['must'][0]['term']['Author'] = author['key']
        author_query_str = json.dumps(author_query_json)
        r = requests.post(ocean.elastic.index_url+"/_search?size=10000", data=author_query_str)

        if "hits" not in r.json():
            logging.error("Can't find commits for %s" , author['key'])
            print(r.json())
            print(author_query)
            continue
        for item in r.json()["hits"]["hits"]:
            new_item = item['_source']
            new_item.update(
                {"author_min_date":author['min']['value_as_string'],
                 "author_max_date":author['max']['value_as_string']}
            )
            author_items.append(new_item)

        # if len(author_items) >= ocean.elastic.max_items_bulk:
        #    ocean.elastic.bulk_upload(author_items, "ocean-unique-id")
        #    author_items = []

        nauthors_done += 1
        if nauthors_done % 10 == 0:
            logging.debug("Authors processed %i/%i", nauthors_done, len(authors))

    logging.debug("ES UPDATED items")

    logging.debug("Authors processed %i/%i", nauthors_done, len(authors))
    logging.debug("Completed demography enrich from %s", ocean.elastic.index_url)

    return (author_items)


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
            #if len(items) % 10 == 0: # debug
            #    break
            if len(items) % 1000 == 0:
                logging.debug("Items loaded: %i" % len(items))
        logging.debug("Items loaded: %i" % len(items))

        logging.info("Size of items: %i", sys.getsizeof(items))
        return items

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)


def write_data(es_index, data):
    """ Write the data to ES """
    logging.info("Writing %i items data with demography info to %s", len(data), es_index)

@timeit
def check(data_es, data_pandas):
    logging.info("Checking data from ES and Pandas are the same")

    if len(data_es) != len(data_pandas):
        logging.error("Data from ES and pandas are different in size: %i %i", len(data_es), len(data_pandas))
        return False
    else:
        logging.info("Data from ES and pandas are of the same size")

        for item_es in data_es:
            for item_pandas in data_pandas:
                if item_es['hash'] == item_pandas['hash']:
                    es_min = parser.parse(item_es['author_min_date']).replace(tzinfo=None)
                    es_max = parser.parse(item_es['author_max_date']).replace(tzinfo=None)
                    pandas_min = parser.parse(item_pandas['author_min_date'])
                    pandas_max = parser.parse(item_pandas['author_max_date'])
                    if es_min == pandas_min and es_max == pandas_max:
                        break
                    else:
                        print(item_es['hash'],item_es['Author'],item_es['author_min_date'],item_es['author_max_date'])
                        print(item_pandas['hash'],item_pandas['Author'],item_pandas['author_min_date'],item_pandas['author_max_date'])
                        logging.info("ITEM KO")
                        return False
    logging.info("ES and Pandas data are the same")
    return True



if __name__ == '__main__':
    args = get_params()
    config_logging(args.debug)

    ES_IN = args.elastic_url
    ES_IN_INDEX = args.index
    ES_OUT = ''

    DASK_GRAPH = {
        'es_read': ES_IN,
        'es_index_read': ES_IN_INDEX,
        'es_es_index_write': ES_OUT,
        'es_pandas_index_write': ES_OUT+"_pandas",
        'load': (load_data, 'es_read', 'es_index_read'),
        'enrich_es': (enrich_demography_es, ES_IN, ES_IN_INDEX, 'load'),
        'enrich_pandas': (enrich_demography_pandas, 'load'),
        'write_es_es': (write_data, 'es_es_index_write', 'enrich_es'),
        'write_es_pandas': (write_data, 'es_pandas_index_write', 'enrich_pandas'),
        'check': (check, 'enrich_es', 'enrich_pandas')
    }


    # get(DASK_GRAPH, ['check'])
    get(DASK_GRAPH, ['write_es_es','write_es_pandas'])
