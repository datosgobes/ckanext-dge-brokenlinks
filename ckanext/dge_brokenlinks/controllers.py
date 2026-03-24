# Copyright (C) 2026 Entidad Pública Empresarial Red.es
#
# This file is part of "dge-brokenlinks (datos.gob.es)".
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import logging

import ckan.lib.base as base
import ckan.plugins.toolkit as toolkit
from ckan.common import c, _
from ckan.lib.base import BaseController
from ckan.lib.base import render
from pylons import request

from .logic import dge_brokenlinks_group_to_check, dge_brokenlinks_get_checkeable_groups

log = logging.getLogger(__name__)


class DGEBrokenlinksController(BaseController):

    def broken_links(self):
        user = c.user
        log.debug('broken_links action')
        if c.userobj:
            log.debug('User is admin' if c.userobj.sysadmin else 'User is org')
            return render('report/broken_links.html')
        else:
            base.abort(401, _('Not authorized to see this page'))

    def checkeable_groups(self):
        if not c.userobj or not c.userobj.sysadmin:
            base.abort(401, _('Not authorized to see this page'))

        if request.method == 'POST':
            log.debug('POST request')
            log.debug(list(request.params.values()))
            dge_brokenlinks_group_to_check(list(request.params.values()))
        elif request.method == 'GET':
            import json

            if not c.userobj or not c.userobj.sysadmin:
                base.abort(401, _('Not authorized to see this page'))

            return json.dumps({'selected_orgs': dge_brokenlinks_get_checkeable_groups()})
        else:
            toolkit.abort(404)

    def load_checkeable_groups(self):
        pass
