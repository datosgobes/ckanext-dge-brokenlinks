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

import copy
try:
    from collections import OrderedDict  # from python 2.7
except ImportError:
    from sqlalchemy.util import OrderedDict

from ckan.common import _
import ckan.model as model
import ckan.plugins as p

from ckanext.report import lib


def broken_links(organization, include_sub_organizations=False):
    if organization is None:
        return broken_links_index(include_sub_organizations=include_sub_organizations)
    else:
        return broken_links_for_organization(organization=organization, include_sub_organizations=include_sub_organizations)


def broken_links_index(include_sub_organizations=False):
    '''Returns the count of broken links for all organizations.'''

    from ckanext.dge_brokenlinks.model import BrokenlinksDB

    counts = {}
    orgs = model.Session.query(model.Group)\
        .filter(model.Group.type == 'organization')\
        .filter(model.Group.state == 'active').all()
    for org in add_progress_bar(
            orgs, 'Part 1/2' if include_sub_organizations else None):
        brokenlinks = (model.Session.query(BrokenlinksDB)
            .filter(BrokenlinksDB.is_broken == True)
            .join(model.Package, BrokenlinksDB.package_id == model.Package.id)
            .filter(model.Package.owner_org == org.id)
            .filter(model.Package.state == 'active')
            .join(model.Resource, BrokenlinksDB.resource_id == model.Resource.id)
            .filter(model.Resource.state == 'active'))
        broken_resources = brokenlinks.count()
        broken_datasets = brokenlinks.distinct(model.Package.id).count()
        num_datasets = model.Session.query(model.Package)\
            .filter_by(owner_org=org.id)\
            .filter_by(state='active')\
            .filter_by(type='dataset')\
            .count()
        num_resources = model.Session.query(model.Package)\
            .filter_by(owner_org=org.id)\
            .filter_by(state='active')
        if p.toolkit.check_ckan_version(max_version='2.2.99'):
            num_resources = num_resources.join(model.ResourceGroup)
        num_resources = num_resources \
            .join(model.Resource)\
            .filter_by(state='active')\
            .count()
        counts[org.name] = {
            'organization_title': org.title,
            'broken_packages': broken_datasets,
            'broken_resources': broken_resources,
            'packages': num_datasets,
            'resources': num_resources
        }

    counts_with_sub_orgs = copy.deepcopy(counts)
    if include_sub_organizations:
        for org_name in add_progress_bar(counts_with_sub_orgs, 'Part 2/2'):
            org = model.Group.by_name(org_name)

            for sub_org_id, sub_org_name, sub_org_title, sub_org_parent_id \
                    in org.get_children_group_hierarchy(type='organization'):
                if sub_org_name not in counts:
                    continue
                counts_with_sub_orgs[org_name]['broken_packages'] += \
                    counts[sub_org_name]['broken_packages']
                counts_with_sub_orgs[org_name]['broken_resources'] += \
                    counts[sub_org_name]['broken_resources']
                counts_with_sub_orgs[org_name]['packages'] += \
                    counts[sub_org_name]['packages']
                counts_with_sub_orgs[org_name]['resources'] += \
                    counts[sub_org_name]['resources']
        results = counts_with_sub_orgs
    else:
        results = counts

    data = []
    num_broken_packages = 0
    num_broken_resources = 0
    num_packages = 0
    num_resources = 0
    for org_name, org_counts in results.items():
        data.append(OrderedDict((
            ('organization_title', results[org_name]['organization_title']),
            ('organization_name', org_name),
            ('package_count', org_counts['packages']),
            ('resource_count', org_counts['resources']),
            ('broken_package_count', org_counts['broken_packages']),
            ('broken_package_percent', lib.percent(org_counts['broken_packages'], org_counts['packages'])),
            ('broken_resource_count', org_counts['broken_resources']),
            ('broken_resource_percent', lib.percent(org_counts['broken_resources'], org_counts['resources'])),
            )))

        org_counts_ = counts[org_name]
        num_broken_packages += org_counts_['broken_packages']
        num_broken_resources += org_counts_['broken_resources']
        num_packages += org_counts_['packages']
        num_resources += org_counts_['resources']

    data.sort(key=lambda x: (-x['broken_package_count'],
                             -x['broken_resource_count']))

    return {'table': data,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
            'num_packages': num_packages,
            'num_resources': num_resources,
            'broken_package_percent': lib.percent(num_broken_packages, num_packages),
            'broken_resource_percent': lib.percent(num_broken_resources, num_resources),
            }


def broken_links_for_organization(organization, include_sub_organizations=False):
    '''
    Returns a dictionary detailing broken resource links for the organization
    or if organization it returns the index page for all organizations.

    params:
      organization - name of an organization

    Returns:
    {'organization_name': 'cabinet-office',
     'organization_title:': 'Cabinet Office',
     'table': [
       {'package_name', 'package_title', 'resource_url', 'status', 'reason', 'last_success',
       'first_failure', 'failure_count', 'last_updated'}
      ...]

    '''
    from ckanext.dge_brokenlinks.model import BrokenlinksDB

    org = model.Group.get(organization)
    if not org:
        raise p.toolkit.ObjectNotFound()

    name = org.name
    title = org.title

    brokenlinks = (model.Session.query(BrokenlinksDB, model.Package, model.Group).
        filter(BrokenlinksDB.is_broken == True).
        join(model.Package, BrokenlinksDB.package_id == model.Package.id).
        filter(model.Package.state == 'active').
        join(model.Resource, BrokenlinksDB.resource_id == model.Resource.id).
        filter(model.Resource.state == 'active'))

    if not include_sub_organizations:
        org_ids = [org.id]
        brokenlinks = brokenlinks.filter(model.Package.owner_org == org.id)
    else:
        # We want any organization_id that is part of this organization's tree
        org_ids = ['%s' % child_org.id for child_org in lib.go_down_tree(org)]
        brokenlinks = brokenlinks.filter(model.Package.owner_org.in_(org_ids))

    brokenlinks = brokenlinks.join(model.Group, model.Package.owner_org == model.Group.id)

    results = []

    for brokenlink, pkg, org in brokenlinks.all():
        pkg = model.Package.get(brokenlink.package_id)
        resource = model.Resource.get(brokenlink.resource_id)
        via = ''
        er = pkg.extras.get('external_reference', '')
        if er == 'ONSHUB':
            via = "Stats Hub"
        elif er.startswith("DATA4NR"):
            via = "Data4nr"

        archived_resource = model.Session.query(model.Resource)\
                                 .filter_by(id=resource.id)\
                                 .first() or resource
        row_data = OrderedDict((
            ('dataset_title', pkg.title),
            ('dataset_name', pkg.name),
            ('dataset_notes', lib.dataset_notes(pkg)),
            ('organization_title', org.title),
            ('organization_name', org.name),
            ('resource_position', resource.position),
            ('resource_id', resource.id),
            ('resource_url', archived_resource.url),
            ('url_up_to_date', resource.url == archived_resource.url),
            ('via', via),
            ('first_failure', brokenlink.first_failure.isoformat() if brokenlink.first_failure else None),
            ('last_updated', brokenlink.updated.isoformat() if brokenlink.updated else None),
            ('last_success', brokenlink.last_success.isoformat() if brokenlink.last_success else None),
            ('url_redirected_to', brokenlink.url_redirected_to),
            ('reason', brokenlink.reason),
            ('status_id', brokenlink.status_id),
            ('failure_count', brokenlink.failure_count),
            ))

        results.append(row_data)

    num_broken_packages = brokenlinks.distinct(model.Package.name).count()
    num_broken_resources = len(results)

    num_packages = model.Session.query(model.Package)\
                        .filter(model.Package.owner_org.in_(org_ids))\
                        .filter_by(state='active')\
                        .filter_by(type='dataset')\
                        .count()
    num_resources = model.Session.query(model.Resource)\
                         .filter_by(state='active')
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        num_resources = num_resources.join(model.ResourceGroup)
    num_resources = num_resources \
        .join(model.Package)\
        .filter(model.Package.owner_org.in_(org_ids))\
        .filter_by(state='active').count()

    return {'organization_name': name,
            'organization_title': title,
            'num_broken_packages': num_broken_packages,
            'num_broken_resources': num_broken_resources,
            'num_packages': num_packages,
            'num_resources': num_resources,
            'broken_package_percent': lib.percent(num_broken_packages, num_packages),
            'broken_resource_percent': lib.percent(num_broken_resources, num_resources),
            'table': results}


def broken_links_option_combinations():
    for organization in lib.all_organizations(include_none=True):
        for include_sub_organizations in (False, True):
            yield {'organization': organization,
                   'include_sub_organizations': include_sub_organizations}


broken_links_report_info = {
    'name': 'broken-links',
    'title': _('Broken links'),
    'description': _('Dataset URLs that have broken links.'),
    'option_defaults': OrderedDict((('organization', None),
                                    ('include_sub_organizations', False),
                                    )),
    'option_combinations': broken_links_option_combinations,
    'generate': broken_links,
    'template': 'report/broken_links.html',
    }


def add_progress_bar(iterable, caption=None):
    try:
        import progressbar
        bar = progressbar.ProgressBar(widgets=[
            (caption + ' ') if caption else '',
            progressbar.Percentage(), ' ',
            progressbar.Bar(), ' ', progressbar.ETA()])
        return bar(iterable)
    except ImportError:
        return iterable
