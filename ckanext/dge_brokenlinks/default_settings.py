# Copyright (C) 2025 Entidad Pública Empresarial Red.es
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

from ckan.plugins.toolkit import config

# directory to save downloaded files to
ARCHIVE_DIR = config.get('ckanext-archiver.archive_dir', '')

# Max content-length of archived files, larger files will be ignored
MAX_CONTENT_LENGTH = int(config.get('ckanext-archiver.max_content_length',
                                    50000000))

USER_AGENT_STRING = config.get('ckanext-archiver.user_agent_string', None)
if not USER_AGENT_STRING:
    USER_AGENT_STRING = '%s %s ckanext-archiver' % (
        config.get('ckan.site_title', ''), config.get('ckan.site_url'))
