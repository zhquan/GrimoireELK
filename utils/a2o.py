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
import time


from arthur.arthur import Arthur
from arthur.errors import InvalidDateError

from grimoire.elk.elastic import ElasticSearch, ElasticConnectException

from grimoire.utils import get_params_arthur_parser, config_logging
from grimoire.ocean.elastic import ElasticOcean

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

def get_arthur(redis_url):
    conn = connect_to_redis(redis_url)

    sync_mode = True  # Don't use RQ yet

    arthur = Arthur(conn, sync_mode, base_cache_path=None)

    repositories = """
        {
        "repositories": [
            {
                "args": {
                    "gitpath": "/tmp/arthur_git/",
                    "uri": "https://github.com/grimoirelab/arthur.git"
                },
                "backend": "git",
                "origin": "https://github.com/grimoirelab/arthur.git"
            },
            {
                "args": {
                    "gitpath": "/tmp/perceval_git/",
                    "uri": "https://github.com/grimoirelab/perceval.git"
                },
                "backend": "git",
                "origin": "https://github.com/grimoirelab/perceval.git"
            },
            {
                "args": {
                    "gitpath": "/tmp/GrimoireELK_git/",
                    "uri": "https://github.com/grimoirelab/GrimoireELK.git"
                },
                "backend": "git",
                "origin": "https://github.com/grimoirelab/GrimoireELK.git"
            }
        ]
        }
    """

    repositories = json.loads(repositories)

    logging.info("Reading repositories...")
    for repo in repositories['repositories']:
        from_date = repo['args'].get('from_date', None)
        if from_date:
            repo['args']['from_date'] = str_to_datetime(from_date)

        arthur.add(repo['origin'], repo['backend'], repo['args'])
    logging.info("Done. Ready to work!")

    return arthur

if __name__ == '__main__':

    app_init = datetime.now()

    args = get_params()

    print(args)

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
                     "number": 0 # total number of items retrieved
                     }

            if items_pool[item['origin']]['bulk']:
                if item[uuid_field] == items_pool[item['origin']]['bulk'][-1][uuid_field]:
                    items_pool[item['origin']]["task_finished"] = True
            item = ElasticOcean.add_update_date(item)
            items_pool[item['origin']]['bulk'].append(item)
            if len(items_pool[item['origin']]['bulk']) > elastic_ocean.max_items_bulk \
                or items_pool[item['origin']]["task_finished"]:
                logging.info("Arthur retrieve task finished for %s" % (item['origin']))
                elastic_ocean.set_index(item['origin'])
                elastic_ocean.bulk_upload_sync(items_pool[item['origin']]["bulk"],
                    uuid_field)
                items_pool[item['origin']]["number"] += \
                    len(items_pool[item['origin']]["bulk"])
                items_pool[item['origin']]["bulk"] = []
                items_pool[item['origin']]["task_finished"] = False
            logging.info("%s %s" % (item['origin'], item[uuid_field]))

    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        for origin in items_pool:
            print(origin, items_pool[origin]["number"])
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
