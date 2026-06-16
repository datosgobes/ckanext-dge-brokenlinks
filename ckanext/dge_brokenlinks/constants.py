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

#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Stores common constants for broken links process
'''
from ckan.plugins.toolkit import config

USER_AGENT = config.get('ckanext-dge-brokenlinks.user_agent', None)
ALLOWED_SCHEMES = set(('http', 'https', 'ftp'))
COULD_NOT_MAKE_HEAD_REQUEST = 'Could not make HEAD request'
UNCHECKED_LINK_BANNED_DOMAIN_STATUS_CODE = -2
HTTP_TIMEOUT_STATUS_CODE = 408
UNCHECKED_LINK_BANNED_DOMAIN_REASON = 'Not checked due to a blocked domain'