#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Enrich arthur items in Elastic for using in Kibana
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

import argparse
import logging
import pickle

from datetime import datetime
from os import sys
from threading import Thread
from time import sleep

import dateutil.parser
import redis
import queue
from rq import push_connection

from arthur.common import CH_PUBSUB

from grimoire.arthur import enrich_items, enrich_sortinghat
from grimoire.elk.elastic import ElasticSearch, ElasticConnectException
from grimoire.utils import config_logging, get_connector_from_name
from grimoire.ocean.elastic import ElasticOcean

TIME_TO_CHECK = 1  # seconds to wait for new arthur events
WAIT_INDEX = 2  # seconds to wait to be sure all items are indexed
task_finish_queue = queue.Queue() # shared queue between threads

class TaskEvents(Thread):
    """Class to receive the task events"""

    def __init__(self, conn):
        super().__init__()
        self.conn = conn
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
    """Parse command line arguments"""

    parser = argparse.ArgumentParser()

    ElasticOcean.add_params(parser)

    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help="show  debug messages")
    parser.add_argument("--no_incremental",  action='store_true',
                        help="start item retrieval from start")
    parser.add_argument("--fetch_cache",  action='store_true',
                        help="Use cache for item retrieval")
    parser.add_argument("--redis",  default="redis://redis/8",
                        help="url for the redis server (default: 'redis://redis/8')")
    parser.add_argument('--index', default="ocean", help="Ocean index name (default ocean)")
    parser.add_argument('--db-projects-map', help="Projects Mapping DB")
    parser.add_argument('--db-sortinghat', help="SortingHat DB")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args

def connect_to_redis(redis_url):
    """Create a connection with a Redis database"""

    conn = redis.StrictRedis.from_url(redis_url)
    push_connection(conn)

    logging.debug("Redis connection stablished with %s.", redis_url)

    return conn

def get_index_origin(origin):
    index_ = origin
    return index_

def get_arthur_events(redis_url):
    conn = connect_to_redis(redis_url)
    TaskEvents(conn).start()
    logging.info("Waiting for task events from arthur ...")

    return


def enrich_origin(elastic, backend_name, origin, db_sortinghat=None, db_projects=None):
    """ In the elastic index all items must be of the same backend """

    # Prepare the enrich backend
    enrich_cls = get_connector_from_name(backend_name.lower())[2]

    enrich_backend = enrich_cls(None, db_projects, db_sortinghat)

    # es_index = origin+"_enrich"
    # Share the same enriched index for all items of a data source
    es_index = elastic.index + "_" + backend_name.lower() + "_enrich"
    es_mapping = enrich_backend.get_elastic_mappings()
    elastic_enrich = ElasticSearch(elastic.url, es_index, es_mapping)

    enrich_backend.set_elastic(elastic_enrich)
    # We need to enrich from just updated items since last enrichment
    # Always filter by origin to support multi origin indexes
    filter_ = {"name":"origin",
               "value":origin}
    last_enrich = enrich_backend.get_last_update_from_es(filter_)

    logging.info("Last enrich for %s: %s" % (origin, last_enrich))

    ocean = ElasticOcean(None, from_date=last_enrich, origin=origin)
    ocean.set_elastic(elastic)

    if db_sortinghat:
        enrich_sortinghat(backend_name, ocean, enrich_backend)

    enrich_items(ocean, enrich_backend)


def check_task_finished(elastic_ocean, db_sortinghat, db_projects_map):
    try:
        task_finished = task_finish_queue.get_nowait()
        logging.info("Sleeping %i seconds to be sure all items are already indexed." % (WAIT_INDEX))
        sleep (WAIT_INDEX)
        enrich_origin(elastic_ocean, task_finished.backend, task_finished.origin, db_sortinghat, db_projects_map)
    except queue.Empty:
        pass

if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    config_logging(args.debug)

    try:
        es_index = args.index  # ocean index name
        es_mapping = ElasticOcean.get_elastic_mappings()
        elastic_ocean = ElasticSearch(args.elastic_url, es_index, es_mapping)

    except ElasticConnectException:
        logging.error("Can't connect to Elastic Search. Is it running?")
        sys.exit(1)

    try:
        logging.info("Magician Merlin is doing spells!")

        get_arthur_events(args.redis)

        while(True):
            sleep(TIME_TO_CHECK)
            # Check for finished tasks every second
            check_task_finished(elastic_ocean, args.db_sortinghat, args.db_projects_map)


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
