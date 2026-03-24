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
import json
import ckan.plugins as p
from builtins import str
from ckan import model
from ckan.plugins.toolkit import config
from ckanext.dge_brokenlinks import tasks
from ckan.plugins.toolkit import enqueue_job

log = logging.getLogger(__name__)


def compat_enqueue(name, fn, queue, args=None):
    u'''
    Enqueue a background job using Celery or RQ.
    '''
    try:
        # Try to use RQ
        from ckan.plugins.toolkit import enqueue_job
        enqueue_job(fn, args=args, queue=queue)
    except ImportError:
        # Fallback to Celery
        import uuid
        from ckan.lib.celery_app import celery
        celery.send_task(name, args=args + [queue], task_id=str(uuid.uuid4()))


def create_archiver_resource_task(resource, queue):
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        # earlier CKANs had ResourceGroup
        package = resource.resource_group.package
    else:
        package = resource.package
    tasks.update_resource_task(resource.id, queue)
    log.debug('Archival of resource put into celery queue %s: %s/%s url=%r',
              queue, package.name, resource.id, resource.url)

def create_link_checker_task(data, queue, is_resource):
    '''
    Create the tasks of check the link regardless of whether it is a resource os a package
    '''

    if type(is_resource) == bool:
        if is_resource:
            id = data.id
            url = data.url
            data = json.dumps(model.Resource.as_dict(data))
            log.debug('Link checker of resource put into celery queue %s: %s url=%r', queue, id, url)
        else:
            name = data.name
            data = json.dumps(model.Package.as_dict(data))
            log.debug('Link checker of package put into celery queue %s: %s', queue, name)
        enqueue_job(tasks.link_checker_task, queue=queue, kwargs={"data": data, 'is_resource': is_resource}, rq_kwargs={"timeout": config.get('ckan.jobs.timeout', 2000)})
    else:
        log.error('The data with id %s insn\'t a resource or a package. The task to check the if the link is broken won\'t be created', data.id)



def create_package_task(package, queue):

    log.debug('Archival of package put into celery queue %s: %s',
              queue, package.name)


def get_extra_from_pkg_dict(pkg_dict, key, default=None):
    for extra in pkg_dict.get('extras', []):
        if extra['key'] == key:
            return extra['value']
    return default
