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
import pickle
import redis
import requests
from rq import Queue
from rq import push_connection
import time

import queue
from threading import Thread


from arthur.arthur import Arthur
from arthur.errors import InvalidDateError
from arthur.common import CH_PUBSUB

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException

from grimoire.utils import get_params_arthur_parser, config_logging, get_connector_from_name
from grimoire.ocean.elastic import ElasticOcean



repositories = {}  # arthur repositories
# Items per origin data
items_pool = {}

task_finish_queue = queue.Queue()

class TaskEvents(Thread):
    """Class to receive the task events"""

    def __init__(self, conn, items_pool, async_mode=True):
        super().__init__()
        self.conn = conn
        self.items_pool=items_pool
        self.daemon = True
        self.pubsub = self.conn.pubsub()
        self.pubsub.subscribe(CH_PUBSUB)


    def run(self):
        for msg in self.pubsub.listen():
            if msg['type'] != 'message':
                continue
            data = pickle.loads(msg['data'])
            if data['status'] == 'failed':
                continue

            job = data['result']

            # Send to a shared Queue the job.last_uuid and process it in main

            logging.info("Finished task: %s %s", job.origin, job.backend)
            task_finish_queue.put(job)

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
    push_connection(conn)

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


def get_arthur(redis_url, items_pool):
    conn = connect_to_redis(redis_url)
    async_mode = True  # Use RQ

    TaskEvents(conn, items_pool, async_mode=async_mode).start()


    arthur = Arthur(conn, async_mode, base_cache_path=None)

    logging.info("Reading repositories...")
    backends_all = ['bugzilla','gerrit','git','github','jira','mbox','stackexchange']
    backends_on = ['gerrit']
    for repo in repositories['repositories']:
        if repo['backend'] not in backends_on:
            continue
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

def elastic_get_item(elastic, uuid, origin):
    item = None
    elastic.set_index(get_index_origin(origin))
    r = requests.get(elastic_ocean.index_url+"/items/%s" % (uuid))
    res = r.json()
    if "found" in res and res["found"]:
        item = res["_source"]
    return item

def check_task_finished(elastic_ocean, items_pool):
    try:
        task_finished = task_finish_queue.get_nowait()
        if task_finished.origin not in items_pool:
            add_origin(items_pool, task_finished.origin)

        # Check that the last_uuid has not been processed yet
        last_item_es = elastic_get_item(elastic_ocean, task_finished.last_uuid, task_finished.origin)
        if not last_item_es:
            items_pool[task_finished.origin]["task_event_last_uuid"] = task_finished.last_uuid
            items_pool[task_finished.origin]["task_event_nitems"] = task_finished.nitems
        else:
            # Task last item already in elastic search
            # Check task update >= ES max update
            elastic_ocean.set_index(get_index_origin(task_finished.origin))
            filter_ = {"name":"origin", "value":task_finished.origin}
            last_item_es_date = elastic_ocean.get_last_date("metadata__updated_on", filter_).replace(tzinfo=None)
            # last item for the task update time
            last_item_date = datetime.fromtimestamp(task_finished.last_date)
            if last_item_date >= last_item_es_date:
                # last item in task will update the elasticsearch last item
                items_pool[task_finished.origin]["task_event_last_uuid"] = task_finished.last_uuid
                items_pool[task_finished.origin]["task_event_nitems"] = task_finished.nitems
            else:
                logging.warning ("%s > %s" % (last_item_es_date, last_item_date))
                logging.warning("[TASK EVENT] Task event received after last item.")
                logging.warning("Post task enrichment won't be done!")
    except queue.Empty:
        pass

def add_origin(items_pool, origin):
    items_pool[origin] = \
        {"bulk": [], # items to add using bulk interface
         "task_finished": False, # received all items for the task
         "task_event_last_uuid": None, # last item in the task
         "task_event_nitems": None, # number of items in the task
         "number": 0, # total number of items retrieved
         "rounds": 0, # number of rounds done
         "last_round_nitems": 0 # number of items in last round
         }

if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    repositories = json.load(args.repositories)

    config_logging(args.debug)

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

        arthur = get_arthur(args.redis, items_pool)

        for item in feed_items(arthur):
            logging.info(item['uuid'])
            item = ElasticOcean.add_update_date(item)
            if item['origin'] not in items_pool:
                add_origin(items_pool, item['origin'])

            # Check for finished tasks after every item
            check_task_finished(elastic_ocean, items_pool)

            if items_pool[item['origin']]['task_event_last_uuid']:
                if item[uuid_field] == items_pool[item['origin']]['task_event_last_uuid']:
                    items_pool[item['origin']]["task_finished"] = True
                    logging.info("Last item received: %s", item[uuid_field])
            items_pool[item['origin']]['bulk'].append(item)
            items_pool[item['origin']]["number"] += 1
            items_pool[item['origin']]["last_round_nitems"] += 1
            if len(items_pool[item['origin']]['bulk']) >= elastic_ocean.max_items_bulk \
                or items_pool[item['origin']]["task_finished"]:
                elastic_ocean.set_index(get_index_origin(item['origin']))
                elastic_ocean.bulk_upload_sync(items_pool[item['origin']]["bulk"],
                    uuid_field)
                items_pool[item['origin']]["bulk"] = []
                logging.info("Uploaded %s (%i) to Ocean" % (item['origin'],
                             len(items_pool[item['origin']]["bulk"])))
            if items_pool[item['origin']]["task_finished"]:
                logging.info("[FINISHED] task in %s" % (item['origin']))
                # Time to enrich incremental if the task has finished
                enrich_origin(elastic_ocean, item['backend_name'],
                              item['origin'], args.db_sortinghat)
                if items_pool[item['origin']]["task_event_nitems"] == items_pool[item['origin']]["last_round_nitems"]:
                    logging.info("[ROUND] NUMBER OF ITEMS OK: %i == %i"
                    % (items_pool[item['origin']]["task_event_nitems"], items_pool[item['origin']]["last_round_nitems"]))
                else:
                    logging.error("[ROUND] NUMBER OF ITEMS WRONG %i <> %i"
                    % (items_pool[item['origin']]["task_event_nitems"], items_pool[item['origin']]["last_round_nitems"]))
                items_pool[item['origin']]["rounds"] += 1
                items_pool[item['origin']]["last_round_nitems"] = 0
                items_pool[item['origin']]["task_finished"] = False


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        show_report(elastic_ocean, items_pool)
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
