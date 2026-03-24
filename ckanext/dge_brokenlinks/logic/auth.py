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

import ckan.plugins as p


@p.toolkit.auth_allow_anonymous_access
def dge_brokenlinks_resource_show(context, data_dict):
    # anyone
    return {'success': True}


@p.toolkit.auth_allow_anonymous_access
def dge_brokenlinks_dataset_show(context, data_dict):
    # anyone
    return {'success': True}
