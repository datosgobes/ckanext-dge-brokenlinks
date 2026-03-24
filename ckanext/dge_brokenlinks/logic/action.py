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

import ckan.plugins as p
from ckan import model
from ckanext.dge_brokenlinks.model import BrokenlinksDB, aggregate_archivals_for_a_dataset

ObjectNotFound = p.toolkit.ObjectNotFound
_get_or_bust = p.toolkit.get_or_bust

log = logging.getLogger(__name__)


@p.toolkit.side_effect_free
def dge_brokenlinks_resource_show(context, data_dict=None):
    '''Return a details of the archival of a resource

    :param id: the id of the resource
    :type id: string

    :rtype: dictionary
    '''
    id_ = _get_or_bust(data_dict, 'id')
    archival = Archival.get_for_resource(id_)
    if archival is None:
        raise ObjectNotFound
    archival_dict = archival.as_dict()
    p.toolkit.check_access('dge_brokenlinks_resource_show', context, data_dict)
    return archival_dict


@p.toolkit.side_effect_free
def dge_brokenlinks_dataset_show(context, data_dict=None):
    '''Return a details of the archival of a dataset, aggregated across its
    resources.

    :param id: the name or id of the dataset
    :type id: string

    :rtype: dictionary
    '''
    id_ = _get_or_bust(data_dict, 'id')
    dataset = model.Package.get(id_)
    if not dataset:
        raise ObjectNotFound
    archivals = BrokenlinksDB.get_for_package(dataset.id)
    archival_dict = aggregate_archivals_for_a_dataset(archivals)
    p.toolkit.check_access('dge_brokenlinks_dataset_show', context, data_dict)
    return archival_dict
