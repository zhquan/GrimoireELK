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

class SortingHatDSEnrich(Enrich):

    def get_field_unique_id(self):
        return "uuid"

    def get_elastic_mappings(self):

        mapping = """
        {
            "properties": {
                "name_analyzed": {
                  "type": "string",
                  "index":"analyzed"
                  }
           }
        } """

        return {"items":mapping}

    def get_rich_item(self, item):
        eitem = {}

        # metadata fields to copy
        copy_fields = ["metadata__updated_on","metadata__timestamp","ocean-unique-id","origin"]
        for f in copy_fields:
            if f in item:
                eitem[f] = item[f]
            else:
                eitem[f] = None
        # The real data
        uidentity = item['data']

        # data fields to copy
        copy_fields = ["uuid"]
        for f in copy_fields:
            if f in uidentity:
                eitem[f] = uidentity[f]
            else:
                eitem[f] = None
        # Fields which names are translated
        map_fields = {}
        for fn in map_fields:
            eitem[map_fields[fn]] = uidentity[fn]

        # Enrich dates
        eitem["update_date"] = parser.parse(item["metadata__updated_on"]).isoformat()
        # Identities
        eitem["nidentities"] = len(uidentity["identities"])
        # Enrollments
        if len(uidentity["enrollments"])>0:
            pass
            
        return eitem
