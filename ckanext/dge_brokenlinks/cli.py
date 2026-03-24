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

import sys
import click
import re
import logging
import datetime
from ckanext.dge_brokenlinks import utils
from ckanext.dge_brokenlinks.tasks import check_to_unban_by_organism
from ckan.cli.cli import CtxObject
import ckan.model as model
from subprocess import call
from datetime import datetime
from ckan.plugins.toolkit import config
from ckanext.dge_brokenlinks.parameters import Parameters as par
from ckanext.dge_brokenlinks import command_celery as celery
from ckanext.dge_brokenlinks import utils, dge_logic
from ckanext.dge_brokenlinks import model as dge_model
from ckanext.dge_brokenlinks.model import Status, BrokenlinksDB, BrokendomainUrl

log = logging.getLogger(__name__)
PATTERN = '([a-z0-9-_]+)'

def get_commands():
    return [dge_brokenlinks, celery.dge_brokenlinks_celery]

@click.group(name=u'dge_brokenlinks')
def dge_brokenlinks():
    pass



class DgeBrokenlinksCommand(CtxObject):
    """
        Control reports, their generation and caching.

        Reports can be cached if they implement IReportCache. Suitable for ones
        that take a while to run.

        The available commands are:

            initdb   - Initialize the database tables for this extension

            link_checker - Check the brokenlinks of the resources

            generate - Generate and cache reports - all of them unless you specify
                       a comma separated list of them.

            generate-for-options - Generate and cache a report for one combination
                       of option values. You can leave it with the defaults or
                       specify options as more parameters: key1=value key2=value

        e.g.

            ckan -c /etc/ckan/ckan.ini dge_brokenlinks initdb

            ckan -c /etc/ckan/ckan.ini dge_brokenlinks drop_brokenlinks_tables

            ckan -c /etc/ckan/ckan.ini dge_brokenlinks empty_brokenlinks_tables

            ckan -c /etc/ckan/ckan.ini dge_brokenlinks report

            ckan -c /etc/ckan/ckan.ini dge_brokenlinks link_checker -o selected


        """

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = None
    min_args = 0
    datetime_format = '%d/%m/%Y %H:%M:%S.%f'

    def __init__(self, name):
        super(DgeBrokenlinksCommand, self).__init__(name)


    @dge_brokenlinks.command(u'clean_banned_dommains')
    @click.option('-e', '--empty', required=False, is_flag=True, help='Empty the table of the banned domains')
    @click.option('-d', '--domain', required=False, help='Delete the rows of the table of the banned domains, the domain that have been specified')
    @click.option('-o', '--organism', required=False, help='Delete the rows of the table of the banned domains, the organism that have been specified')
    def clean_banned_dommains(empty, domain, organism):
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init clean_banned_dommains')
        if empty:
            BrokendomainUrl.empty_table()
        elif domain:
            BrokendomainUrl.unban_by_domain(domain)
        elif organism:
            BrokendomainUrl.unban_by_organism(organism)

        end = datetime.now()
        par.log(log, par.LOG_INFO, ('[%s] - End link_checker command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))


    @dge_brokenlinks.command(u'link_checker')
    @click.option('-q', '--queue', required=False, default = 'bulk')
    @click.option('-o', '--organism', required=False, help='Organisms to check resource links. Acepted "all", "selected" or organism id options.')
    @click.option('-l', '--list', help='List of the identifiers Eg: --list [id1_value,id2_value]', required=False)
    @click.argument('identifier', required=False)
    def link_checker(queue, organism= None, list = None, identifier = None):
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init link_checker command')
        if list and identifier or organism and identifier:
            log.error("Parameters aren't correct. Detected at the same time unsupported arguments")
            sys.exit(-1)
        if list:
            if list.endswith(","):
                log.warn("List is not correct, try without spaces between the identifiers.")
            elif(not list and not identifier):
                log.warn('No identifiers to update founded.')
            else:
                list = list[1: len(list) - 1].split(",")
                utils.link_checker(list, queue)
        elif identifier:
            utils.link_checker(identifier, queue)

        elif organism:
            if organism in {'selected', 'all'}:
                check_to_unban_by_organism(None, organism)
                dge_logic.dge_organism_check_broken_links(organism)
            elif re.search(PATTERN, organism):
                check_to_unban_by_organism(organism, None)
                dge_logic.dge_organism_check_broken_links(organism)
            else:
                log.error('Organism not matches with the options: Acepted "all", "selected" or organism id options.')
        end = datetime.now()
        par.log(log, par.LOG_INFO, ('[%s] - End link_checker command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)


    @dge_brokenlinks.command(u'initdb')
    def initdb():
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init initdb command')
        try:
            configureSession()
            dge_model.BrokenlinksDB.init_tables_brokenlinks(dge_model.BrokenlinksDB)
            click_command = config.get('ckanext-dge-brokenlinks.click_report_initdb_command')
            click_command = click_command.format(config.get('ckanext-dge-brokenlinks.config_file'))
            log.info(click_command)
            call(click_command, shell=True)
            log.info("Set up custom statistics tables in main database")
        except Exception as e:
            log.error(('Exception %s' % e))
            sys.exit(1)
        finally:
            end = datetime.now()
            par.log(log, par.LOG_INFO,('[%s] - End initdb command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)


    @dge_brokenlinks.command(u'drop_brokenlinks_tables')
    def dropBrokenlinksTables():
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init drop_brokenlinks_tables command')
        try:
            configureSession()
            dge_model.BrokenlinksDB.drop_tables_brokenlinks(dge_model.BrokenlinksDB)
            log.info("Drop custom statistics tables in main database")
        except Exception as e:
            log.error(('Exception %s' % e))
            sys.exit(1)
        finally:
            end = datetime.now()
            par.log(log, par.LOG_INFO,('[%s] - End drop_brokenlinks_tables command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)


    @dge_brokenlinks.command(u'empty_brokenlinks_tables')
    def emptyBrokenlinksTables():
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init empty_brokenlinks_tables command')
        try:
            configureSession()
            dge_model.BrokenlinksDB.empty_tables_brokenlinks(dge_model.BrokenlinksDB)
            log.info("Emptied all the tables of brokenlinks in main database")
        except Exception as e:
            log.error(('Exception %s' % e))
            sys.exit(1)
        finally:
            end = datetime.now()
            par.log(log, par.LOG_INFO,('[%s] - End empty_brokenlinks_tables command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))
        sys.exit(0)


    @dge_brokenlinks.command(u'report')
    @click.option('-n', '--notify', is_flag=True, help='Notify to the owners of the datasets that the report has been created', required=False)
    def generate_report(notify):
        init = datetime.now()
        par.log(log, par.LOG_INFO, 'Init report command')
        click_command = config.get('ckanext-dge-brokenlinks.click_report_command')
        click_command = click_command.format(config.get('ckanext-dge-brokenlinks.config_file'))
        log.info(click_command)
        call(click_command, shell=True)
        if(notify):
            par.log(log, par.LOG_INFO, 'SE LLAMA A LA FUNCION DEL MAIL')
            dge_logic.dge_brokenlinks_report_email_finished()
            print('Generando el reporte ' + (str(notify)))
            
        par.log(log, par.LOG_INFO, 'SE HA TERMINADO DE EJECUTAR EL REPORTE')

        end = datetime.now()
        par.log(log, par.LOG_INFO, ('[%s] - End report command. Executed command in %s milliseconds.' % (
            end.strftime(DgeBrokenlinksCommand.datetime_format), (end - init).total_seconds() * 1000)))



def configureSession():
    model.Session.remove()
    model.Session.configure(bind=model.meta.engine)