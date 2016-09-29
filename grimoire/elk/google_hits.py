#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#
# Copyright (C) 2015 Bitergia
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

import json
import logging

from dateutil import parser

from grimoire.elk.enrich import Enrich

class GoogleHitsEnrich(Enrich):

    def __init__(self, perceval = None, db_sortinghat=None, db_projects_map = None):
        super().__init__(db_sortinghat, db_projects_map)
        self.elastic = None
        self.perceval_backend = None
        self.index_google_hist = "google_hits"

    def set_elastic(self, elastic):
        self.elastic = elastic

    def get_field_date(self):
        return "created_at"

    def get_field_unique_id(self):
        return "id"

    def get_rich_item(self, item):
        eitem = {}

        if 'keywords' not in item:
            logging.error("keywords not found it %s", item)
            return eitem

        # The real data
        hits = item

        # data fields to copy
        copy_fields = ["hits", "type", "host", "command", "@timestamp"]
        for f in copy_fields:
            if f in hits:
                eitem[f] = hits[f]
            else:
                eitem[f] = None
        # Numeric field
        eitem["hits"] = int(hits["hits"])
        # Date fields
        eitem["created_at"]  = parser.parse(hits["@timestamp"]).isoformat()

        # Fields which names are translated
        map_fields = {"@version": "logstash_version"}
        for f in map_fields:
            if f in hits:
                eitem[map_fields[f]] = hits[f]
            else:
                eitem[map_fields[f]] = None

        eitem['keywords'] = ",". join(hits['keywords'])

        eitem['id'] = eitem['keywords'].replace(",","_")+"_"+eitem['@timestamp']

        eitem.update(self.get_grimoire_fields(eitem["created_at"],'hits'))

        return eitem

    def enrich_items(self, items):
        max_items = self.elastic.max_items_bulk
        current = 0
        bulk_json = ""

        url = self.elastic.index_url+'/items/_bulk'

        logging.debug("Adding items to %s (in %i packs)", url, max_items)

        for item in items:
            if current >= max_items:
                self.requests.put(url, data=bulk_json)
                bulk_json = ""
                current = 0

            rich_item = self.get_rich_item(item)
            if not rich_item:
                continue
            data_json = json.dumps(rich_item)
            bulk_json += '{"index" : {"_id" : "%s" } }\n' % \
                (rich_item[self.get_field_unique_id()])
            bulk_json += data_json +"\n"  # Bulk document
            current += 1
        self.requests.put(url, data = bulk_json)
