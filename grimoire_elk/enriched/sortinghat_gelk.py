# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#   Alvaro del Castillo San Felix <acs@bitergia.com>
#   Quan Zhou <quan@bitergia.com>
#

from datetime import datetime
import logging

from sortinghat.cli.client import (SortingHatClient,
                                   SortingHatClientError,
                                   SortingHatSchema)
from sgqlc.operation import Operation


logger = logging.getLogger(__name__)


MULTI_ORG_NAMES = '_multi_org_names'


class SortingHat(object):

    @classmethod
    def get_uuid_from_id(cls, db, sh_id):
        def _fetch_mk(entities):
            mks = []
            for e in entities:
                mk = e['mk']
                mks.append(mk)

            return mks

        uuid = None
        args = {
            "page": 1,
            "page_size": 10,
            "filters": {
                "uuid": sh_id
            }
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            page_info = op.individuals().page_info()
            page_info.has_next()
            result = db.execute(op)
            entities = result['data']['individuals']['entities']
            mks = _fetch_mk(entities)
            has_next = result['data']['individuals']['pageInfo']['hasNext']
            while has_next:
                args['page'] = args['page'] + 1
                op = Operation(SortingHatSchema.Query)
                op.individuals(**args)
                individual = op.individuals().entities()
                individual.mk()
                individual.profile().name()
                page_info = op.individuals().page_info()
                page_info.has_next()
                result = db.execute(op)
                entities = result['data']['individuals']['entities']
                mks.extend(_fetch_mk(entities))
                has_next = result['data']['individuals']['pageInfo']['hasNext']

            if mks:
                uuid = mks[0]
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error get uuid from id {}: {}".format(sh_id, e.errors[0]['message']))

        return uuid

    @classmethod
    def add_identity(cls, db, identity, backend):
        """ Load and identity list from backend in Sorting Hat """
        uuid = None

        try:
            args = {
                "email": identity['email'],
                "name": identity['name'],
                "source": backend,
                "username": identity['username']
            }
            args = {k: v for k, v in args.items() if v}
            op = Operation(SortingHatSchema.SortingHatMutation)
            add = op.add_identity(**args)
            add.uuid()
            result = db.execute(op)
            uuid = result['data']['addIdentity']['uuid']

            logger.debug("[sortinghat] New identity {} {},{},{} ".format(
                         uuid, identity['username'], identity['name'], identity['email']))

            profile = {"name": identity['name'] if identity['name'] else identity['username'],
                       "email": identity['email']}
            profile = {k: v for k, v in profile.items() if v}

            op = Operation(SortingHatSchema.SortingHatMutation)
            update_args = {
                "data": profile,
                "uuid": uuid
            }
            update = op.update_profile(**update_args)
            update.uuid()
            result = db.execute(op)
            uuid = result['data']['updateProfile']['uuid']

        except SortingHatClientError as ex:
            msg = ex.errors[0]['message']
            uuid = msg.split("'")[1]
        except UnicodeEncodeError:
            logger.warning("[sortinghat] UnicodeEncodeError. Ignoring it. {} {} {}".format(
                           identity['email'], identity['name'], identity['username']))
        except Exception:
            logger.warning("[sortinghat] Unknown exception adding identity. Ignoring it. {} {} {}".format(
                           identity['email'], identity['name'], identity['username']))

        if 'company' in identity and identity['company'] is not None:
            try:
                org_name = cls.add_organization(db, identity['company'])
                logger.debug("[sortinghat] New organization added {}".format(org_name))

                org_name = cls.add_enrollment(db, uuid, identity['company'])
                logger.debug("[sortinghat] New enrollment added {}".format(org_name))
            except SortingHatClientError:
                pass

        return uuid

    @classmethod
    def add_enrollment(cls, db, uuid, organization, from_date=datetime(1900, 1, 1), to_date=datetime(2100, 1, 1)):
        args = {
            "from_date": from_date,
            "organization": organization,
            "to_date": to_date,
            "uuid": uuid
        }
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            add_org = op.enroll(**args)
            add_org.organization().name()
            result = db.execute(op)
            org_name = result['data']['addOrganization']['organization']['name']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error add enrollment {} {} {} {}: {}".
                         format(uuid, organization, from_date, to_date, e.errors[0]['message']))
            org_name = None

        return org_name

    @classmethod
    def add_organization(cls, db, organization):
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            add_org = op.add_organization(dict(name=organization))
            add_org.organization().name()
            result = db.execute(op)
            org_name = result['data']['addOrganization']['organization']['name']
        except SortingHatClientError as e:
            logger.error("[sortinghat] Error add organization {}: {}".format(organization, e.errors[0]['message']))
            org_name = None

        return org_name

    @classmethod
    def add_identities(cls, db, identities, backend):
        """ Load identities list from backend in Sorting Hat """

        logger.debug("[sortinghat] Adding identities")

        total = 0

        for identity in identities:
            try:
                cls.add_identity(db, identity, backend)
                total += 1
            except Exception as e:
                logger.error("[sortinghat] Unexcepted error when adding identities: {}".format(e))
                continue

        logger.debug("[sortinghat] Total identities added: {}".format(total))

    @classmethod
    def remove_identity(cls, sh_db, ident_id):
        """Delete an identity from SortingHat.

        :param sh_db: SortingHat database
        :param ident_id: identity identifier
        """
        success = False
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            delete = op.delete_identity(uuid=ident_id, individual=False)
            delete.delete_identity().uuid()
            result = sh_db.execute(op)
            removed_uuid = result['data']['deleteIdentity']['uuid']
            logger.debug("[sortinghat] Identity {} deleted".format(removed_uuid))
            success = True
        except Exception as e:
            logger.error("[sortinghat] Error remove identity {}: {}".format(ident_id, e))

        return success

    @classmethod
    def remove_unique_identity(cls, sh_db, uuid):
        """Delete a unique identity from SortingHat.

        :param sh_db: SortingHat database
        :param uuid: Unique identity identifier
        """
        success = False
        try:
            op = Operation(SortingHatSchema.SortingHatMutation)
            delete = op.delete_identity(uuid=uuid, individual=True)
            delete.delete_identity().uuid()
            result = sh_db.execute(op)
            removed_uuid = result['data']['deleteIdentity']['uuid']
            logger.debug("[sortinghat] Unique identity {} deleted".format(removed_uuid))
            success = True
        except Exception as e:
            logger.debug("[sortinghat] Error remove unique identity {}: {}".format(uuid, e))

        return success

    @classmethod
    def unique_identities(cls, sh_db):
        """List the unique identities available in SortingHat.

        :param sh_db: SortingHat database
        """
        def _fetch_mk(entities):
            mks = []
            for e in entities:
                mk = e['mk']
                mks.append(mk)

            return mks
        args = {
            'page': 1,
            'page_size': 10
        }
        try:
            op = Operation(SortingHatSchema.Query)
            op.individuals(**args)
            individual = op.individuals().entities()
            individual.mk()
            page_info = op.individuals().page_info()
            page_info.has_next()
            result = sh_db.execute(op)
            entities = result['data']['individuals']['entities']
            mks = _fetch_mk(entities)
            has_next = result['data']['individuals']['pageInfo']['hasNext']
            while has_next:
                page = args['page']
                args['page'] = page + 1
                op = Operation(SortingHatSchema.Query)
                op.individuals(**args)
                individual = op.individuals().entities()
                individual.mk()
                individual.profile().name()
                page_info = op.individuals().page_info()
                page_info.has_next()
                result = sh_db.execute(op)
                entities = result['data']['individuals']['entities']
                mks.extend(_fetch_mk(entities))
                has_next = result['data']['individuals']['pageInfo']['hasNext']
            return mks
        except SortingHatClientError as e:
            logger.debug("[sortinghat] Error list unique identities: {}".format(e))
