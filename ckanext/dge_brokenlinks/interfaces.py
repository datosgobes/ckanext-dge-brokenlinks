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

import ckan.plugins as plugins
from ckan.plugins.interfaces import Interface

log = logging.getLogger(__name__)


class IPipe(Interface):
    """
    Process data in a Data Pipeline.

    Inherit this to subscribe to events in the Data Pipeline and be able to
    broadcast the results for others to process next. In this way, a number of
    IPipes can be linked up in sequence to build up a data processing pipeline.

    When a resource is archived, it broadcasts its resource_id, perhaps
    triggering a process which transforms the data to another format, or loads
    it into a datastore, or checks it against a schema. These processes can in
    turn put the resulting data into the pipeline
    """

    def receive_data(self, operation, queue, **params):
        pass

    @classmethod
    def send_data(cls, operation, queue, **params):
        for observer in plugins.PluginImplementations(cls):
            try:
                observer.receive_data(operation, queue, **params)
            except Exception as ex:
                log.exception(ex)
                # We reraise all exceptions so they are obvious there
                # is something wrong
                raise
