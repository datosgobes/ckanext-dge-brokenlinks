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

class Parameters():
    TRYING_DROP     = 'Trying to drop \'%s\', but table doesn´t exists'
    TRYING_EMPTY    = 'Trying to empty \'%s\', but table doesn´t exists'
    DROPPED         = 'Dropped table: \'%s\' '
    CREATED         = 'Created table: \'%s\' '
    ALREADY_EXISTS  = 'Table already exists: \'%s\''
    START_METHOD    = '##### START of the \'%s\' method #####'
    END_METHOD      = '##### END of the \'%s\' method #####'
    EMPTY           = 'Emptied table: \'%s\' '

    LOG_INFO = 'info'
    LOG_DEBUG = 'debug'
    LOG_WARN = 'warn'
    LOG_ERROR = 'error'

    def log(log, category, message):

        if category in ['info', 'debug', 'error', 'warn']:
            print('#' * 100)
        if category == 'info':
            log.info(message)
        elif category == 'debug':
            log.debug(message)
        elif category == 'error':
            log.error(message)
        elif category == 'warn':
            log.warn(message)
        if category in ['info', 'debug', 'error', 'warn']:
            print('#' * 100)
