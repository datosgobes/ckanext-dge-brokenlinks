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

from ckan.plugins import toolkit as tk
import ckan.lib.helpers as h
import ckanext.dge.helpers as dh
from ckanext.dge_brokenlinks import utils, model
from ckanext.dge_brokenlinks.tasks import http_status_success
import logging
from ckan.common import c, request


log = logging.getLogger(__name__)


def dge_brokenlinks_resource_show(resource_id):
    data_dict = {'id': resource_id}
    return tk.get_action('dge_brokenlinks_resource_show')(data_dict)


def archiver_is_resource_broken_html(resource):
    archival = resource.get('archiver')
    if not archival:
        return tk.literal('<!-- No archival info for this resource -->')
    extra_vars = {'resource': resource}
    extra_vars.update(archival)
    return tk.literal(
        tk.render('archiver/is_resource_broken.html',
                  extra_vars=extra_vars))


def archiver_is_resource_cached_html(resource):
    archival = resource.get('archiver')
    if not archival:
        return tk.literal('<!-- No archival info for this resource -->')
    extra_vars = {'resource': resource}
    extra_vars.update(archival)
    return tk.literal(
        tk.render('archiver/is_resource_cached.html',
                  extra_vars=extra_vars))


# Replacement for the core ckan helper 'format_resource_items'
# but with our own blacklist
def archiver_format_resource_items(items):
    blacklist = ['archiver', 'qa']
    items_ = [item for item in items
              if item[0] not in blacklist]
    import ckan.lib.helpers as ckan_helpers
    return ckan_helpers.format_resource_items(items_)


def dge_url_for_user_organization():
    orgs = dh.dge_url_for_user_organization()
    return orgs


# Get organization from URL
def dge_getOrganization(params):
    c.options['organization'] = params.get('organization') if params else None
    # if empty organization then force None
    if (not c.options['organization']):
        c.options['organization'] = None

def dge_organization_data():
    organization = []
    for row in c.data['table']:
        if row['organization_name'] in c.options['organization']:
            organization = row
            break
    data_table = []

    # Get pagination data
    try:
        limit = int(request.args.get('limit', 50))
    except ValueError:
        limit = 50
    if limit < 5:
        limit = 5
    elif limit > 1000:
        limit = 1000

    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
    if page < 1:
        page = 1

    raw_types = request.args.get('types')
    types = []
    if raw_types:
        types = raw_types.split(',')
        types = [int(t) for t in types if t.isdigit() and (100 <= int(t) <= 599 or int(t) == -1)]

    data_dict = {
        'limit': limit,
        'offset': (page - 1) * limit,
        'types': types
    }

    data_BL, num_total_links = model.getBrokenlinksByOrganizationName(organization['organization_name'], data_dict)
    for data_t in data_BL:
        row = {
            'dataset_name' : data_t[13],
            'resource_id' : data_t[2],
            'dataset_title' : data_t[14],
            'resource_url' : data_t[6],
            'reason' : data_t[5],
            'first_failure' : data_t[7],
            'last_success' : data_t[8],
            'last_updated' : data_t[11],
            'status_id': data_t[3]}
        data_table.append(row)
    for row in c.data['table']:
        if row['organization_name'] in c.options['organization']:
            row['num_broken_packages'] = row['broken_package_count']
            row['num_packages'] = row['package_count']
            row['num_broken_resources'] = row['broken_resource_count']
            row['num_resources'] = row['resource_count']
            del row['package_count']
            del row['resource_count']
            del row['broken_resource_count']
            del row['broken_package_count']
            c.data = row
            c.data['table'] = data_table
    c.data['num_total_links'] = num_total_links

    # Cretate pager
    base_url = h.url_for("/report/broken-links", organization=organization['organization_name'])
    def pager_url(q=None, page=None):
        url = base_url
        if page:
            url += '&page={0}'.format(page)
        if limit:
            url += '&limit={0}'.format(limit)
        if types and len(types) > 0:
            url += '&types={0}'.format(','.join(map(str, types)))
        return url

    pager = h.Page(
        collection=c.data['table'],
        page=page,
        url=pager_url,
        item_count=num_total_links,
        items_per_page=limit
    )
    pager.items = c.data['table']
    c.data['pager'] = pager.pager()

    return organization['organization_title'], data_table


# Get date of last report execution
def dge_getBrokenLinksReportDate():
    try:
        data, report_date = tk.get_action('report_data_get')({}, {'id': 'broken-links', 'options': {}})
    except tk.ObjectNotFound:
        tk.abort(404)
    except tk.NotAuthorized:
        tk.abort(401)

    return report_date


def dge_processDataToSelect(data):
    organizations = []
    orgs_with_datasets = utils.organizations_with_resources()
    checked_organizations = utils.getOrganizationsChecked()

    for row in data['table']:
        if row['organization_name'] in orgs_with_datasets:
            is_checked = any(checked_group[1] == row['organization_name'] and checked_group[2] == row['organization_title'] for checked_group in checked_organizations)
            organizations.append({
                'org_name': row['organization_name'],
                'org_title': row['organization_title'],
                'is_checked': is_checked
            })

    organizations.sort(key=lambda x: (x['org_name'], x['org_title']))

    return organizations


def dge_brokenlinks_get_report_date(org):
    return model.BrokenlinksDB.get_last_updated_date_by_organization(org)


def dge_check_brokenlinks(resource_id):
    return utils.resource_is_broken(resource_id)


def dge_brokenlinks_add_timeout_registers(organization_name, data):
    timeout_regs = utils.get_timeout_data_by_organization_name(organization_name)
    timeout_row = {}
    for row in timeout_regs:
        timeout_row['dataset_name'] = row['name']
        timeout_row['resource_id'] = row['resource_id']
        timeout_row['dataset_title'] = row['title']
        timeout_row['resource_url'] = row['url_redirected_to']
        timeout_row['reason'] = row['reason']
        timeout_row['first_failure'] = row['first_failure']
        timeout_row['last_success'] = row['last_success']
        timeout_row['last_updated'] = row['updated']
        data.append(timeout_row)
    return data


def dge_brokenlinks_get_banned_data():
    return utils.get_banned_data_table()


def dge_brokenlinks_get_brokenlinks_status_count(org_name):
    return utils.get_brokenlinks_status_count(org_name)


def dge_brokenlinks_get_url(data, status_id):
    return [item['resource_url'] for item in data if item['status_id'] == status_id]

