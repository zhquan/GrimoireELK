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

    def get_field_event_unique_id(self):
        return 'id'

    def get_rich_events(self, item):
        """ Create a new eventry (event) per identity in the unique identity """

        identities = []

        # To get values from the task
        uid = self.get_rich_item(item)

        for i in item['data']["identities"]:
            identity = {}
            # Common fields with the item
            # Needed for incremental updates from the item
            identity['metadata__updated_on'] = item['metadata__updated_on']
            identity['origin'] = item['origin']
            # Common fields with the uid
            cfields = ['profile_name', 'profile_email']
            for f in cfields:
                identity[f] = uid[f]

            # enrollments
            identity['enrollments'] = uid['enrollments']
            # Real event data
            copy_fields = ['id', 'uuid', 'email', 'username', 'name', 'source']
            for f in copy_fields:
                identity[f] = i[f]

            identities.append(identity)

        return identities

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
        # Identities: will break down in events analysis
        eitem["nidentities"] = len(uidentity["identities"])
        # Enrollments
        eitem["enrollments"] = None
        if len(uidentity["enrollments"])>0:
            eitem["enrollments"] = ''
            for e in uidentity["enrollments"]:
                eitem["enrollments"] += ","+e['organization']
            eitem["enrollments"] = eitem["enrollments"][1:]
        # Profile
        eitem['profile_name'] = None
        eitem['profile_email'] = None
        if uidentity['profile']:
            eitem['profile_name'] = uidentity['profile']['name']
            eitem['profile_email'] = uidentity['profile']['email']

        return eitem
