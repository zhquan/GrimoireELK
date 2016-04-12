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

from grimoire.utils import get_elastic
from grimoire.utils import get_params_parser, get_params_arthur_parser, config_logging


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

    args = parser.parse_args()

    return args

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
                    "gitpath": "/tmp/perceval_git/",
                    "uri": "https://github.com/grimoirelab/perceval.git"
                },
                "backend": "git",
                "origin": "https://github.com/grimoirelab/perceval.git"
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

    config_logging(args.debug)

    try:
        logging.info("King Arthur is on command. Go!")

        arthur = get_arthur(args.redis)

        for item in feed_items(arthur):
            logging.info("%s %s" % (item['origin'],item['uuid']))


    except KeyboardInterrupt:
        logging.info("\n\nReceived Ctrl-C or other break signal. Exiting.\n")
        sys.exit(0)


    total_time_min = (datetime.now()-app_init).total_seconds()/60

    logging.info("Finished in %.2f min" % (total_time_min))
