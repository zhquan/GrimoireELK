# -*- coding: utf-8 -*-
#
# Copyright (C) 2021 Bitergia
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
#   Quan Zhou <quan@bitergia.com>
#


class Analyzer:
    """Class for Elasticsearch analyzer.

    This class will be subclassed by backends,
    which will provide specific settings.
    """

    @staticmethod
    def get_elastic_analyzers(es_major):
        """Get Elasticsearch analyzer.

        Different versions of Elasticsearch may need specific versions
        of the setting, thus using es_major to potentially provide
        different settings.

        When the setting depends on the version of Kibiter/Kibana,
        we will use the Elasticsearch version anyway, since they should
        be paired (same major version for 5 and larger, ES major version 2
        for Kibiter/Kibana major version 4).

        :param es_major: major version of Elasticsearch, as string
        :returns:        dictionary with a key, 'items', with the setting
        """

        analyzer = '{}'

        return {"items": analyzer}
