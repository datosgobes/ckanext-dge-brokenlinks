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

from __future__ import print_function
import os
import ckan.plugins as p


def load_config(config_filepath):
    import paste.deploy
    config_abs_path = os.path.abspath(config_filepath)
    conf = paste.deploy.appconfig('config:' + config_abs_path)
    import ckan
    ckan.config.environment.load_environment(conf.global_conf,
                                             conf.local_conf)


def register_translator():
    from paste.registry import Registry
    from pylons import translator
    from ckan.lib.cli import MockTranslator
    global registry
    registry = Registry()
    registry.prepare()
    global translator_obj
    translator_obj = MockTranslator()
    registry.register(translator, translator_obj)


def get_resources(state='active', publisher_ref=None, resource_id=None,
                  dataset_name=None):
    ''' Returns all active resources, or filtered by the given criteria. '''
    from ckan import model
    resources = model.Session.query(model.Resource) \
        .filter_by(state=state)
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        resources = resources.join(model.ResourceGroup)
    resources = resources \
        .join(model.Package) \
        .filter_by(state='active')
    criteria = [state]
    if publisher_ref:
        publisher = model.Group.get(publisher_ref)
        assert publisher
        resources = resources.filter(model.Package.owner_org == publisher.id)
        criteria.append('Publisher:%s' % publisher.name)
    if dataset_name:
        resources = resources.filter(model.Package.name == dataset_name)
        criteria.append('Dataset:%s' % dataset_name)
    if resource_id:
        resources = resources.filter(model.Resource.id == resource_id)
        criteria.append('Resource:%s' % resource_id)
    resources = resources.all()
    print('%i resources (%s)' % (len(resources), ' '.join(criteria)))
    return resources
