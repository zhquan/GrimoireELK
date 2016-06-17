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
import requests

from dask.threaded import get

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


def enrich_demography_pandas(items, from_date=None):

    logging.debug("Doing Pandas demography enrich")
    author_items = []  # items from author with new date fields added

    items = items[0:10]
    # print(json.dumps(items, indent=4, sort_keys=True))


    b = db.from_sequence(items)

    df = b.to_dataframe()

    df1 = df.set_index('metadata__timestamp')

    # print(df1.compute())

    # Time to get MAX and MIN dates group by author
    # gb = df.groupby("Author").max()['author_date']
    gb = df.groupby("Author")['author_date'].max()

    print(gb.compute())
    # print(gb.head(1000))
    # print(gb.tail())
    # print(gb.columns)



    return (author_items)


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
                "field": "utc_commit"
              }
            },
            "max": {
              "max": {
                "field": "utc_commit"
              }
            }
          }
        }
      }
    }
    """ % (query)

    r = requests.post(ocean.elastic.index_url+"/_search", data=es_query)
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
            logging.info("Authors processed %i/%i", nauthors_done, len(authors))

    logging.info("Authors processed %i/%i", nauthors_done, len(authors))
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
            if len(items) % 10 == 0: # debug
                break
            if len(items) % 1000 == 0:
                logging.debug("Items loaded: %i" % len(items))
        logging.debug("Items loaded: %i" % len(items))
        return items

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)


def enrich_demography(data):
    """ Add demography data  """
    logging.info("Enriching data with demography info for %i ", len(data))

    # logging.info("Data to enrich: %i", data)

    # Enrich demography using ES

    # Enrich demography using Pandas


def write_data(es_index, data):
    """ Write the data to ES """
    logging.info("Writing %i items data with demography info to %s", len(data), es_index)


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
        'write_es_pandas': (write_data, 'es_pandas_index_write', 'enrich_pandas')
    }


    get(DASK_GRAPH, ['write_es_pandas'])
    # get(DASK_GRAPH, ['write_es_es','write_es_pandas'])
