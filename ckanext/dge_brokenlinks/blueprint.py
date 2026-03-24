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

from flask import Blueprint, jsonify, make_response, Flask, render_template, request, send_file
from ckanext.report.blueprint import ensure_data_is_dicts, anonymise_user_names, make_csv_from_dicts
import ckan.plugins.toolkit as t
import ckan.lib.helpers as h
from ckanext.report.helpers import relative_url_for
from ckanext.report import blueprint as report_bp
from ckanext.report.report_registry import Report
from jinja2.exceptions import TemplateNotFound
from ckanext.dge_brokenlinks.model import CheckGroupArchiver
from ckanext.dge_brokenlinks import utils
from ckan.plugins.toolkit import config
from werkzeug.datastructures import ImmutableMultiDict
from ckan.common import _

import json, csv
import logging
c = t.c

app = Flask(__name__)
log = logging.getLogger(__name__)
dge_brokenlinks_bp = Blueprint('dge_brokenlinks_bp', __name__)
app.register_blueprint(dge_brokenlinks_bp, url_prefix='/')
backend = config.get('ckanext.dge_brokenlinks.download.path')



@dge_brokenlinks_bp.route('/report/broken-links-report', methods=['POST'])
def saveOrgsanizationsSelected():
    if request.method == 'POST':
        orgs_selected = request.form
        orgs_selected = orgs_selected.get('selected_organizations')
        if orgs_selected:
            orgs_selected = orgs_selected.replace('\'', '\'\'') 
            orgs_selected = orgs_selected.split(',')
        else:
            orgs_selected = []  
        CheckGroupArchiver.updateCheckedGroups(orgs_selected)
        return h.redirect_to('/report/broken-links')


@dge_brokenlinks_bp.route('/report/broken-links/download', methods=['GET'])
def downloadReport():
    '''
    Download a json file with the active organizations in json format
    '''
    global backend
    
    if not c.userobj:
        t.abort(401)

    # Get parameters
    all_orgs = None
    org_name = request.args.get('org_name', None)
    format = request.args.get('format')

    if org_name:
        _check_param_organization_exists(request.args, 'org_name')

        _check_user_organization_permission(request.args, 'org_name')
    else:
        if not c.userobj.sysadmin:
            t.abort(403)

    request.args = ImmutableMultiDict([('format', 'json')])

    if org_name:
        filename = "report_brokenlinks-%s.%s" % (org_name, format)
        json_data = utils.organization_report_data(org_name)
        data = []
        for row in json_data:
            data_row = {}
            for key, value in row.items():
                data_row[key] = value
            data.append(data_row)
        json_data = {'table': data}
        column_names = [_('Dataset'), _('URL'), _('Reason of the error'),
                        _('Last success'), _('Latest fail')]
    else:
        all_orgs = utils.getAllOrganizations()
        all_orgs = [org[1] for org in all_orgs]
        filename = "report_brokenlinks.%s" % format
        json_data = report_bp.view('broken-links').data.decode('utf8')
        column_names = [_('organization_title'), _('broken_package_count'), _('broken_resource_count'),
                        _('package_count'), _('resource_count')]
        json_data = json.loads(json_data)
    path = backend + filename
    file = open(path, 'w', encoding='utf8')
    json_result = []
    for row in json_data['table']:
        if org_name:
            row[column_names[0]] = row['title']
            row[column_names[1]] = row['url']
            row[column_names[2]] = row['reason']
            row[column_names[3]] = h.render_datetime(row['last_success'], "%d/%m/%Y %H:%M")
            row[column_names[4]] = h.render_datetime(row['updated'], "%d/%m/%Y %H:%M") if row['is_broken'] else ''

            res = {key: row[key] for key in column_names}
            json_result.append(res)

        elif all_orgs and row['organization_name'] in all_orgs:
            row[column_names[0]] = row['organization_title']
            row[column_names[1]] = row['broken_package_count']
            row[column_names[2]] = row['broken_resource_count']
            row[column_names[3]] = row['package_count']
            row[column_names[4]] = row['resource_count']

            res = {key: row[key] for key in column_names}
            json_result.append(res)

    result = json.dumps(json_result, ensure_ascii=False, indent=2)
    try:
        if 'csv' in format:
            writer = csv.writer(file)
            writer.writerow(column_names)
            result = json.loads(result)
            for idx, row in enumerate(result):
                row_result = [
                    row[column_names[0]],
                    row[column_names[1]],
                    row[column_names[2]],
                    row[column_names[3]],
                    row[column_names[4]]
                ]
                writer.writerow(row_result)

        if 'json' in format:
            file.write(result)

    except Exception as e:
        log.error('Exception in brokenlinks file download: %s', e)
    finally:
        if file: file.close()

    return send_file(path, as_attachment=True, attachment_filename=filename)


@dge_brokenlinks_bp.route('/report/broken-links', methods=['GET', 'POST'])
def view(organization=None):
    if not c.userobj:
        log.error('Not authorized to access this page')
        t.abort(403)
    report_name = 'broken-links'
    try:
        report = t.get_action('report_show')({}, {'id': report_name})
    except t.NotAuthorized:
        t.abort(403)
    except t.ObjectNotFound:
        t.abort(404)

    # check if provided url organization exists
    _check_param_organization_exists(t.request.params, 'organization')

    # check if user is member of provided organization
    _check_user_organization_permission(t.request.params, 'organization')

    rule = request.url_rule
    # ensure correct url is being used
    if 'organization' in rule.rule and 'organization' not in report['option_defaults']:
        t.redirect_to(relative_url_for(organization=None))
    elif 'organization' not in rule.rule and 'organization' in report['option_defaults'] and \
            report['option_defaults']['organization']:
        org = report['option_defaults']['organization']
        t.redirect_to(relative_url_for(organization=org))
    if 'organization' in t.request.params:
        # organization should only be in the url - let the param overwrite
        # the url.
        t.redirect_to(relative_url_for())

    # options
    options = Report.add_defaults_to_options(t.request.params, report['option_defaults'])
    option_display_params = {}
    if 'format' in options:
        format = options.pop('format')
    else:
        format = None
    if 'organization' in report['option_defaults']:
        options['organization'] = organization
    options_html = {}
    if 'to[]' in options:
        del options['to[]']
    c.options = options  # for legacy genshi snippets
    for option in options:
        if option not in report['option_defaults']:
            log.warn('Not displaying report option HTML for param %s as option not recognized')
            continue
        option_display_params = {'value': options[option],
                                 'default': report['option_defaults'][option]}
        try:
            options_html[option] = \
                t.render_snippet('report/option_%s.html' % option,
                                 data=option_display_params)
        except TemplateNotFound:
            log.warn('Not displaying report option HTML for param %s as no template found')
            continue
            
    # Alternative way to refresh the cache - not in the UI, but is
    # handy for testing
    try:
        refresh = t.asbool(t.request.params.get('refresh'))
        if 'refresh' in options:
            options.pop('refresh')
    except ValueError:
        refresh = False

    # Get pagination options
    if 'limit' in options:
        options.pop('limit')
    if 'page' in options:
        options.pop('page')
    if 'types' in options:
        options.pop('types')
    
    # Check for any options not allowed by the report
    for key in options:
        if key not in report['option_defaults']:
            t.abort(400, 'Option not allowed by report: %s' % key)

    try:
        data, report_date = t.get_action('report_data_get')({}, {'id': report_name, 'options': options})
    except t.ObjectNotFound:
        t.abort(404)
    except t.NotAuthorized:
        t.abort(401)
    if format and format != 'html':
        ensure_data_is_dicts(data)
        anonymise_user_names(data, organization=options.get('organization'))
        if format == 'csv':
            try:
                key = t.get_action('report_key_get')({}, {'id': report_name, 'options': options})
            except t.NotAuthorized:
                t.abort(401)
            filename = 'report_%s.csv' % key
            response = make_response(make_csv_from_dicts(data['table']))
            response.headers['Content-Type'] = 'application/csv'
            response.headers['Content-Disposition'] = str('attachment; filename=%s' % (filename))
            return response
        elif format == 'json':
            data['generated_at'] = report_date
            response = make_response(json.dumps(data))
            response.headers['Content-Type'] = 'application/json'
            return response
        else:
            t.abort(400, 'Format not known - try html, json or csv')
    are_some_results = bool(data['table'] if 'table' in data
                            else data)
    c.data = data
    c.options = options
    return t.render('report/view.html', extra_vars={
        'report': report, 'report_name': report_name, 'data': data,
        'report_date': report_date, 'options': options,
        'options_html': options_html,
        'report_template': report['template'],
        'are_some_results': are_some_results})

def _check_user_organization_permission(params, param_name):
    if param_name in params and not c.userobj.sysadmin:
        user_orgs = [org['name'] for org in t.get_action('organization_list_for_user')({}, {'id': c.userobj.id})]
        if params.get(param_name) not in user_orgs:
            log.error('User not belonging to the organization %s', params.get(param_name))
            t.abort(403)

def _check_param_organization_exists(params, param_name):
    if param_name in params and params.get(param_name):
        org_name = params.get(param_name)
        try:
            t.get_action('organization_show')({}, {'id': org_name})
        except t.ObjectNotFound:
            log.error('Organization not found: %s', org_name)
            t.abort(404)
        except t.NotAuthorized:
            log.error('Not authorized to access organization: %s', org_name)
            t.abort(403)
    else:
        if not c.userobj.sysadmin:
            log.error('Not authorized to access this page')
            t.abort(403)