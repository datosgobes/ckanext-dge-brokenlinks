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

from ckan import model
from ckan import plugins as p
from ckan.plugins.toolkit import config
from ckanext.dge_brokenlinks import blueprint
from ckanext.report.interfaces import IReport
from ckanext.dge_brokenlinks.interfaces import IPipe
from ckanext.dge_brokenlinks.logic import action, auth
from ckanext.dge_brokenlinks import helpers, lib
from ckanext.dge_brokenlinks import cli
from ckanext.dge_brokenlinks.dge_logic import dge_brokenlinks_report_email_finished, dge_brokenlinks_auth
from ckanext.dge_brokenlinks.model import BrokenlinksDB, aggregate_archivals_for_a_dataset
from routes.mapper import SubMapper
from ckan.lib.plugins import DefaultTranslation
import logging

log = logging.getLogger(__name__)


def is_frontend():
    is_frontend = False
    config_is_frontend = config.get('ckanext.dge.is_frontend', None)
    if config_is_frontend and config_is_frontend.lower() == 'true':
        is_frontend = True
    return is_frontend


class MixinDGEPlugin(p.SingletonPlugin):
        p.implements(p.IBlueprint)

        # IBlueprint

        def get_blueprint(self):
            return [blueprint.dge_brokenlinks_bp]

class DgeBrokenlinksPlugin(MixinDGEPlugin, p.SingletonPlugin, DefaultTranslation):
    p.implements(p.IDomainObjectModification, inherit=True)
    p.implements(IReport)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IPackageController, inherit=True)
    if is_frontend():
        p.implements(p.IConfigurer, inherit=True)
        p.implements(p.ITranslation, inherit=True)
        p.implements(p.IRoutes, inherit=True)

    if p.toolkit.check_ckan_version(min_version='2.9.0'):
        p.implements(p.IClick)

    # IDomainObjectModification

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Package):
            return

        log.debug('Notified of package event: %s %s', entity.name, operation)

        run_brokenlinks = \
            self._is_it_sufficient_change_to_run(entity, operation)
        if not run_brokenlinks:
            return

        log.debug('Creating dge_brokenlinks task: %s', entity.name)

        lib.create_package_task(entity, 'priority')

    def _is_it_sufficient_change_to_run(self, package, operation):
        ''' Returns True if in this revision any of these happened:
        * it is a new dataset
        * dataset licence changed (affects qa)
        * there are resources that have been added or deleted
        * resources have changed their URL or format (affects qa)
        '''
        if operation == 'new':
            log.debug('New package - will archive')
            return True
        elif operation == 'deleted':
            log.debug('Deleted package - won\'t archive')
            return False

        # 2.9 does not have revisions so archive anyway
        if p.toolkit.check_ckan_version(min_version='2.9.0'):
            return True

        rev_list = package.all_related_revisions
        if not rev_list:
            log.debug('No sign of previous revisions - will archive')
            return True

        if rev_list[0][0].id == model.Session.revision.id:
            rev_list = rev_list[1:]
        if not rev_list:
            log.warn('No sign of previous revisions - will archive')
            return True
        previous_revision = rev_list[0][0]
        log.debug('Comparing with revision: %s %s',
                  previous_revision.timestamp, previous_revision.id)

        # get the package as it was at that previous revision
        context = {'model': model, 'session': model.Session,
                   'ignore_auth': True,
                   'revision_id': previous_revision.id}
        data_dict = {'id': package.id}
        try:
            old_pkg_dict = p.toolkit.get_action('package_show')(
                context, data_dict)
        except p.toolkit.NotFound:
            log.warn('No sign of previous package - will archive anyway')
            return True

        old_licence = (old_pkg_dict['license_id'],
                       lib.get_extra_from_pkg_dict(old_pkg_dict, 'licence')
                       or None)
        new_licence = (package.license_id,
                       package.extras.get('licence') or None)
        if old_licence != new_licence:
            log.debug('Licence has changed - will archive: %r->%r',
                      old_licence, new_licence)
            return True

        old_resources = dict((res['id'], res)
                             for res in old_pkg_dict['resources'])
        old_res_ids = set(old_resources.keys())
        new_res_ids = set((res.id for res in package.resources))
        deleted_res_ids = old_res_ids - new_res_ids
        if deleted_res_ids:
            log.debug('Deleted resources - will archive. res_ids=%r',
                      deleted_res_ids)
            return True
        added_res_ids = new_res_ids - old_res_ids
        if added_res_ids:
            log.debug('Added resources - will archive. res_ids=%r',
                      added_res_ids)
            return True

        for res in package.resources:
            for key in ('url', 'format'):
                old_res_value = old_resources[res.id][key]
                new_res_value = getattr(res, key)
                if old_res_value != new_res_value:
                    log.debug('Resource %s changed - will archive. '
                              'id=%s pos=%s url="%s"->"%s"',
                              key, res.id[:4], res.position,
                              old_res_value, new_res_value)
                    return True

            was_in_progress = old_resources[res.id].get('upload_in_progress', None)
            is_in_progress = res.extras.get('upload_in_progress', None)
            if was_in_progress != is_in_progress:
                log.debug('Resource %s upload finished - will archive. ', 'upload_finished')
                return True

            log.debug('Resource unchanged. pos=%s id=%s',
                      res.position, res.id[:4])

        log.debug('No new, deleted or changed resources - won\'t archive')
        return False

    # IReport

    def register_reports(self):
        """Register details of an extension's reports"""
        from ckanext.dge_brokenlinks import reports
        return [reports.broken_links_report_info,
                ]

    # IConfigurer

    def update_config(self, config_):
        p.toolkit.add_template_directory(config_, 'templates')
        p.toolkit.add_public_directory(config_, 'public')
        p.toolkit.add_resource('fanstatic',
            'dge_brokenlinks'),
        p.toolkit.add_resource('assets',
            'dge_brokenlinks')

    # IActions

    def get_actions(self):
        return {
            'dge_brokenlinks_resource_show': action.dge_brokenlinks_resource_show,
            'dge_brokenlinks_dataset_show': action.dge_brokenlinks_dataset_show,
            'dge_brokenlinks_report_email_finished': dge_brokenlinks_report_email_finished,
            }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'dge_brokenlinks_resource_show': auth.dge_brokenlinks_resource_show,
            'dge_brokenlinks_dataset_show': auth.dge_brokenlinks_dataset_show,
            'dge_brokenlinks_email_finished': dge_brokenlinks_auth,
            }

    # ITemplateHelpers

    def get_helpers(self):
        return dict((name, function) for name, function
                    in list(helpers.__dict__.items())
                    if callable(function) and name[0] != '_')



    # IPackageController

    def after_show(self, context, pkg_dict):
        # Insert the archival info into the package_dict so that it is
        # available on the API.
        # When you edit the dataset, these values will not show in the form,
        # it they will be saved in the resources (not the dataset). I can't see
        # and easy way to stop this, but I think it is harmless. It will get
        # overwritten here when output again.
        archivals = BrokenlinksDB.get_for_package(pkg_dict['id'])
        if not archivals:
            return
        # dataset
        dataset_archival = aggregate_archivals_for_a_dataset(archivals)
        pkg_dict['archiver'] = dataset_archival
        # resources
        archivals_by_res_id = dict((a.resource_id, a) for a in archivals)
        for res in pkg_dict['resources']:
            archival = archivals_by_res_id.get(res['id'])
            if archival:
                archival_dict = archival.as_dict()
                del archival_dict['id']
                del archival_dict['package_id']
                del archival_dict['resource_id']
                res['archiver'] = archival_dict

    # IClick

    def get_commands(self):
        return cli.get_commands()

        def before_map(self, _map):
            if not is_frontend():
                return _map

            try:
                log.debug("before_map")

                with SubMapper(_map, controller='ckanext.dge_brokenlinks.controllers:DGEBrokenlinksController') as m:
                    m.connect('broken_links', '/enlaces-rotos', action='broken_links')

            except Exception as e:
                log.warn("MAP Before_map exception %r: %r:", type(e), str(e))
            return _map

        def after_map(self, _map):
            return _map

        def get_helpers(self):
            return {
                'organization_name': dah.organization_name,
            }