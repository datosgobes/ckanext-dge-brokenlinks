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
import logging
import sys
import os
import time

from ckan.cli.cli import CtxObject
from ckanext.dge_brokenlinks.parameters import Parameters as par

import click
import logging
import ckan.model as model
from subprocess import call
from datetime import datetime
from ckan.plugins.toolkit import config
from ckanext.dge_brokenlinks import model as dge_model
from ckanext.dge_brokenlinks import command_celery as celery
from ckanext.dge_brokenlinks import utils, dge_logic
from ckanext.dge_brokenlinks import cli

log = logging.getLogger(__name__)

REQUESTS_HEADER = {'content-type': 'application/json'}
LOG_INFO = 'info'
LOG_DEBUG = 'debug'
LOG_WARN = 'warn'
LOG_ERROR = 'error'


def get_commands():
    return [dg_brokenlinks, celery.dge_brokenlinks_celery, cli.dge_brokenlinks]


@click.group(name=u'dg_brokenlinks')
def dg_brokenlinks():
    pass

class DgeBrokenlinksCommand(CtxObject):
    """
    Control reports, their generation and caching.

    Reports can be cached if they implement IReportCache. Suitable for ones
    that take a while to run.

    The available commands are:

        initdb   - Initialize the database tables for this extension

        list     - Lists the reports

        generate - Generate and cache reports - all of them unless you specify
                   a comma separated list of them.

        generate-for-options - Generate and cache a report for one combination
                   of option values. You can leave it with the defaults or
                   specify options as more parameters: key1=value key2=value

    e.g.

      List all reports:
      $ paster report list

      Generate two reports:
      $ paster report generate openness-scores,broken-links

      Generate report for one specified option value(s):
      $ paster report generate-for-options publisher-activity organization=cabinet-office

      Generate all reports:
      $ paster report generate

    """

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = None
    min_args = 0
    datetime_format = '%d/%m/%Y %H:%M:%S.%f'

    def __init__(self, name):
        super(DgeBrokenlinksCommand, self).__init__(name)



    @dg_brokenlinks.command()
    @click.option('-q', '--queue')
    @click.argument('identifiers', nargs=-1)
    def update(identifiers, queue):
        utils.update(identifiers, queue) \


    @dg_brokenlinks.command(u'update_resource')
    @click.option('-q', '--queue', required=False, default = 'bulk')
    @click.option('-l', '--list', help='List of the identifiers Eg: --list ["id1_value","id2_value"]', required=False)
    @click.argument('identifier', required=False)
    def update_resource(queue, list = None, identifier = None):
        init = datetime.now()
        par.log(log, LOG_INFO, 'Init update_resource command')
        if list.endswith(","):
            log.warn("List is not correct, try without spaces between the identifiers.")
        elif(not list and not identifier):
            log.warn('No identifiers to update founded.')
        else:
            list = list[1: len(list) - 1].split(",")
            utils.update(list, queue)

        end = datetime.now()
        par.log(log, LOG_INFO, ('[%s] - End update_resource command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)





    @dg_brokenlinks.command(u'packageview')
    @click.argument('package_ref', required=False)
    def view(package_ref):
        init = datetime.now()
        par.log(log, LOG_INFO, ('Init packageview command with args: %s', package_ref))
        try:
            if package_ref:
                utils.view(package_ref)
            else:
                utils.view()
        except Exception as e:
            log.error(('Exception %s' % e))
            sys.exit(1)

        finally:
            end = datetime.now()
            par.log(log, LOG_INFO, ('[%s] - End packageview command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)


    @dg_brokenlinks.command()
    def clean_status():
        utils.clean_status()


    @dg_brokenlinks.command()
    def clean_cached_resources():
        utils.clean_cached_resources()


    @dg_brokenlinks.command()
    def migrate():
        utils.migrate()


    @dg_brokenlinks.command()
    def migrate_archive_dirs():
        utils.migrate_archive_dirs()


    @dg_brokenlinks.command()
    def size_report():
        utils.size_report()


    @dg_brokenlinks.command()
    def delete_files_larger_than_max_content_length():
        utils.delete_files_larger_than_max_content_length()

from ckanext.dge_brokenlinks.model import CheckGroupArchiver

log = logging.getLogger(__name__)


def configureSession():
    model.Session.remove()
    model.Session.configure(bind=model.meta.engine)