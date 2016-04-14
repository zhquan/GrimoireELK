#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Arthur2Ocean tool
#
# Copyright (C) 2016 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#

from datetime import datetime
import dateutil.parser

import json
import logging
from os import sys
import redis
import requests
import time


from arthur.arthur import Arthur
from arthur.errors import InvalidDateError

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException

from grimoire.utils import get_params_arthur_parser, config_logging, get_connector_from_name
from grimoire.ocean.elastic import ElasticOcean

repositories = {}  # arthur repositories

def str_to_datetime(ts):
    """Format a string to a datetime object.
    This functions supports several date formats like YYYY-MM-DD,
    MM-DD-YYYY and YY-MM-DD.
    :param ts: string to convert
    :returns: a datetime object
    :raises IvalidDateError: when the given string cannot be converted into
        a valid date
    """
    if not ts:
        raise InvalidDateError(date=str(ts))

    try:
        return dateutil.parser.parse(ts).replace(tzinfo=None)
    except Exception:
        raise InvalidDateError(date=str(ts))


def get_params():
    ''' Get params definition from ElasticOcean and from all the backends '''

    parser = get_params_arthur_parser()

    return parser.parse_args()


def connect_to_redis(redis_url):
    """Create a connection with a Redis database"""

    conn = redis.StrictRedis.from_url(redis_url)
    logging.debug("Redis connection stablished with %s.", redis_url)

    return conn


def feed_items(arthur):
    """ Get items from arthur """

    iter_time = 5  # seconds between iterations

    while True:
        time.sleep(iter_time)
        for item in arthur.items():
            yield (item)

    logging.info("King Arthur completed his quest.")

def get_index_origin(origin):
    index_ = origin
    for repo in repositories['repositories']:
        if repo["origin"] == origin:
            if "elastic_index" in repo:
                index_ = repo["elastic_index"]
                break
    return index_


def get_arthur(redis_url):
    conn = connect_to_redis(redis_url)

    sync_mode = True  # Don't use RQ yet

    arthur = Arthur(conn, sync_mode, base_cache_path=None)

    logging.info("Reading repositories...")
    for repo in repositories['repositories']:
        from_date = repo['args'].get('from_date', None)
        if from_date:
            repo['args']['from_date'] = str_to_datetime(from_date)

        arthur.add(repo['origin'], repo['backend'], repo['args'])
    logging.info("Done. Ready to work!")

    return arthur

def show_report(elastic, items_pool):
    for origin in items_pool:
        # Items processed
        print("%s items processed: %i (%i rounds)" % (origin, items_pool[origin]["number"], items_pool[origin]["rounds"]))
        # Items in ES
        elastic_ocean.set_index(get_index_origin(origin))
        r = requests.get(elastic_ocean.index_url+"/_search?size=1")
        res = r.json()
        if 'hits' in res:
            print("%s items in ES: %i" % (origin, res['hits']['total']))


def enrich_origin(elastic, backend, origin, db_sortinghat=None, db_projects=None):
    """ In the elastic index all items must be of the same backend """

    # Prepare the enrich backend
    enrich_cls = get_connector_from_name(backend.lower())[2]
    enrich_backend = enrich_cls(None, db_projects, db_sortinghat)

    es_index = elastic.index+"_enrich"
    es_mapping = enrich_backend.get_elastic_mappings()
    elastic_enrich = ElasticSearch(elastic.url, es_index, es_mapping)

    enrich_backend.set_elastic(elastic_enrich)
    # We need to enrich from just updated items since last enrichment
    # Always filter by origin to support multi origin indexes
    filter_ = {"name":"origin",
               "value":origin}
    last_enrich = enrich_backend.get_last_update_from_es(filter_)

    logging.info("Last enrich for %s: %s" % (origin, last_enrich))

    ocean = ElasticOcean(None, last_enrich)
    ocean.set_elastic(elastic)

    total = 0

    items_pack = []

    for item in ocean:
        if len(items_pack) >= elastic.max_items_bulk:
            enrich_backend.enrich_items(items_pack)
            items_pack = []
        items_pack.append(item)
        total += 1

    enrich_backend.enrich_items(items_pack)


if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    repositories = json.load(args.repositories)

    config_logging(args.debug)

    # Items per origin data
    items_pool = {}
    uuid_field = ElasticOcean.get_field_unique_id()
    uuid_last = None

    try:
        es_index = None  # it will change with origin
        es_mapping = ElasticOcean.get_elastic_mappings()
        elastic_ocean = ElasticSearch(args.elastic_url, es_index, es_mapping)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    try:
        logging.info("King Arthur is on command. Go!")

        arthur = get_arthur(args.redis)

        for item in feed_items(arthur):
            if item['origin'] not in items_pool:
                items_pool[item['origin']] = \
                    {"bulk": [], # items to add using bulk interface
                     "task_finished": False, # origin arthur task finished
                     "number": 0, # total number of items retrieved,
                     "last_uuid": None, # last item retrieved,
                     "first_uuid": item['uuid'], # first item retrieved,
                     "rounds": 0 # number of rounds done
                     }

            if items_pool[item['origin']]['last_uuid']:
                # If this item is the same than the more recent one
                # the retrieval task has finished (no more updates)
                if item[uuid_field] == items_pool[item['origin']]['last_uuid']:
                    # Normally last_uuid is the most recent one
                    items_pool[item['origin']]["task_finished"] = True
                elif item[uuid_field] == items_pool[item['origin']]['first_uuid']:
                    # In gerrit the first uuid is the most recent one
                    items_pool[item['origin']]["task_finished"] = True
            items_pool[item['origin']]['last_uuid'] = item[uuid_field]
            item = ElasticOcean.add_update_date(item)
            items_pool[item['origin']]['bulk'].append(item)
            if len(items_pool[item['origin']]['bulk']) >= elastic_ocean.max_items_bulk \
                or items_pool[item['origin']]["task_finished"]:
                elastic_ocean.set_index(get_index_origin(item['origin']))
                elastic_ocean.bulk_upload_sync(items_pool[item['origin']]["bulk"],
                    uuid_field)
                items_pool[item['origin']]["number"] += \
                    len(items_pool[item['origin']]["bulk"])
                logging.info("Uploaded %s (%i) to Ocean" % (item['origin'],
                             len(items_pool[item['origin']]["bulk"])))
                if items_pool[item['origin']]["task_finished"]:
                    # Time to enrich incremental if the task has finished
                    enrich_origin(elastic_ocean, item['backend_name'],
                                  item['origin'], args.db_sortinghat)
                    items_pool[item['origin']]["rounds"] += 1
                items_pool[item['origin']]["bulk"] = []
                items_pool[item['origin']]["task_finished"] = False


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        show_report(elastic_ocean, items_pool)
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
