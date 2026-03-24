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

from __future__ import print_function
import logging
import sys
import os

from future import standard_library
import click
import subprocess


from ckan.cli.cli import CtxObject
from ckanext.dge_brokenlinks.parameters import Parameters as par
from ckan.plugins.toolkit import config


log = logging.getLogger(__name__)
standard_library.install_aliases()  # noqa
CONFIG_PATH_FILE = config.get('ckanext.config_path', None)

LOG_INFO = 'info'
LOG_DEBUG = 'debug'
LOG_WARN = 'warn'
LOG_ERROR = 'error'


@click.group(name=u'dge_brokenlinks_celery')
def dge_brokenlinks_celery():
    pass


class CeleryCmd(CtxObject):
    '''
    Manages the Celery daemons. This is an improved version of CKAN core's
    'celeryd' command.

    Usage:

        paster celeryd2 run [all|bulk|priority]
           - Runs a celery daemon to run tasks on the bulk or priority queue

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0
    max_args = 2

    def __init__(self, name):
        super(CeleryCmd, self).__init__(name)
        self.parser.add_option('--loglevel',
                               action='store',
                               dest='loglevel',
                               default='INFO',
                               help='Celery logging - choose between DEBUG, INFO, WARNING, ERROR, CRITICAL or FATAL')
        self.parser.add_option('--concurrency',
                               action='store',
                               dest='concurrency',
                               default='1',
                               help='Number of concurrent processes to run')
        self.parser.add_option('-n', '--hostname',
                               action='store',
                               dest='hostname',
                               help="Set custom hostname")

    def _load_config(self):
        CtxObject(CONFIG_PATH_FILE)


    @dge_brokenlinks_celery.command(u'run')
    @click.option('-q', '--queue', help='name of the queue')
    @click.option('-l', '--logging', help='Celery logging - choose between DEBUG, INFO, WARNING, ERROR, CRITICAL or FATAL', required=False)
    @click.option('-c', '--concurrency', help='Number of concurrent processes to run', default=1, required=False)
    @click.option('-n', '--hostname', help='Set custom hostname', required=False)
    def command(queue, logging, concurrency, hostname):
        """
        Click command line arguments and call appropriate method.
        """
        log_celery=logging
        par.log(log, LOG_INFO, 'Init celery run command')
        CeleryCmd._load_config(CeleryCmd)

        if not log_celery:
            log_celery = 'debug'
        elif log_celery.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL']:
            log_celery = log_celery.upper()
        else:
            log.error("Log type \'%s\' not in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL']", log_celery)
            sys.exit(1)
        if(queue in ['all', 'priority', 'bulk']):
            if queue == 'all':
                queue = 'priority,bulk'
            CeleryCmd.run_(CeleryCmd,
                           loglevel=log_celery,
                           queue=queue,
                           concurrency=int(concurrency),
                           hostname=hostname)
        else:
            logging.error()

    def run_(self, loglevel='INFO', queue=None, concurrency=None,
             hostname=None):
        os.environ['CKAN_CONFIG'] = CONFIG_PATH_FILE

        celery_args = []
        celery_args.append('celery')
        celery_args.append('-A')
        celery_args.append('tasks')
        celery_args.append('worker')
        if concurrency:
            celery_args.append('--concurrency=%d' % concurrency)
        if queue:
            celery_args.append('--queues=%s' % queue)
        if hostname:
            celery_args.append('--hostname=%s' % hostname)
        celery_args.append('--loglevel=%s' % loglevel)

        print(celery_args)

        subprocess.run(celery_args)
