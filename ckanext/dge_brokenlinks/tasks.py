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
import os
import json
import re
import requests
import http.client
import time
from http import HTTPStatus
from rq.timeouts import JobTimeoutException
from datetime import datetime, timedelta
from ckanext.dge_brokenlinks.parameters import Parameters as par
from requests.packages import urllib3
from future.moves.urllib.parse import urlparse, urljoin, quote, urlunparse, unquote
from ckan import model
from ckan.common import _
from ckanext.dge_brokenlinks import default_settings as settings
from ckan.plugins.toolkit import config
from ckan.plugins import toolkit
from ckanext.dge_brokenlinks import dge_logic
from ckanext.dge_brokenlinks import interfaces as brokenlink_interfaces
from ckanext.dge_brokenlinks import model as dge_model

log = logging.getLogger(__name__)

USER_AGENT = config.get('ckanext-dge-brokenlinks.user_agent', None)
ALLOWED_SCHEMES = set(('http', 'https', 'ftp'))
COULD_NOT_MAKE_HEAD_REQUEST = 'Could not make HEAD request'

http_status_success = [
    HTTPStatus.OK.phrase,
    HTTPStatus.ACCEPTED.phrase,
    HTTPStatus.MULTIPLE_CHOICES.phrase,
    HTTPStatus.MOVED_PERMANENTLY.phrase,
    HTTPStatus.FOUND.phrase,
    HTTPStatus.SEE_OTHER.phrase,
    HTTPStatus.NOT_MODIFIED.phrase,
    HTTPStatus.USE_PROXY.phrase,
    HTTPStatus.TEMPORARY_REDIRECT.phrase,
    HTTPStatus.PERMANENT_REDIRECT.phrase,
    HTTPStatus.METHOD_NOT_ALLOWED.phrase,
    HTTPStatus.REQUEST_TIMEOUT.phrase,
    COULD_NOT_MAKE_HEAD_REQUEST
]

from ckanext.dge_brokenlinks.model import BrokenlinksDB, BrokendomainUrl


banned_until = int(config.get('ckanext-dge-brokenlinks.time_banned', 600000) \
        if config.get('ckanext-dge-brokenlinks.time_banned', 600000) \
        else os.environ.get('BROKENLINKS_TIME_BANNED', 600000))

retry_attempts = int(config.get('ckanext-dge-brokenlinks.max_failures', 15) \
        if config.get('ckanext-dge-brokenlinks.max_failures', 15) \
        else os.environ.get('BROKENLINKS_RETRY_ATTEMPTS', 15))

log = logging.getLogger(__name__)

banned_domains_model = None


def getBannedDomains():
    global banned_domains_model
    banned_domains_model = BrokendomainUrl.get_banned_domains()

    banned_domains = []
    for dom in banned_domains_model:
        count = str(dom.failure_count)
        if dom.failure_count >= retry_attempts: banned_domains.append(dom.domain)
    return banned_domains


banned_domains = getBannedDomains()


class ArchiverError(Exception):
    pass


class LinkCheckerError(ArchiverError):
    pass


class LinkInvalidError(LinkCheckerError):
    pass


class LinkHeadRequestError(LinkCheckerError):
    pass


class LinkHeadMethodNotSupported(LinkCheckerError):
    pass


def link_checker_task(*args, **kwargs):
    '''
    Task to check the url of the resources

    data -> data of the package or resource
    queue -> queue to use. bulk by default
    is_resource -> Boolean data to specify if the data is a package or resource
    '''
    timeout = config.get('ckanext.deg_brokenlinks.check_timeout', 30)
    data_checker = {'url': None, 'url_timeout': timeout, 'package_id': None, 'resource_id': None}
    is_resource = kwargs.get('is_resource')
    data = json.loads(kwargs.get('data'))  # Obtain the object data of the args and transform to dict
    global banned_domains

    banned_domains = getBannedDomains()

    if is_resource:
        data_checker['url'] = data['url']
        data_checker['resource_id'] = data['id']
        data_checker['package_id'] = data['package_id']
        updateCkeckResourceInDB(data_checker)

    else:  # Is a package
        resources = dge_model.get_resources_id_by_package_id(data['id'])
        for resource in resources:
            data_checker['url'] = resource.url
            data_checker['resource_id'] = resource.id
            data_checker['package_id'] = resource.package_id
            updateCkeckResourceInDB(data_checker)


    dge_model.closeSession()


def update_resource_task(resource_id, queue='bulk'):
    '''
    Archive a resource.
    '''
    log.debug(par.START_METHOD, 'update_resource_task')
    log.info('Starting update resource \'%s\' in queue \'%s\'', resource_id, queue)
    time.sleep(2)

    # Do all work in a sub-routine since it can then be tested without celery.
    # Also put try/except around it is easier to monitor ckan's log rather than
    # celery's task status.
    result = None
    try:
        result = _update_resource(resource_id, queue, log)

    except Exception as e:
        if os.environ.get('DEBUG'):
            raise
        log.error('Error occurred during archiving resource: %s\nResource: %r',
                  e, resource_id)
        raise
    log.debug(par.END_METHOD, 'update_resource_task')
    return result


def updateCkeckResourceInDB(data_checker):
    '''
    Update the resource at follows:
        - If the resource were already in the table, obtain it, check if the
          last check is above of the minimun check retry time. If is above, checks the
          link again. If it is below, it does nothing

        - If the resource weren't in the table, checks the link and save it in the table
    '''

    brokenlinksDB = BrokenlinksDB.get_for_resource(data_checker['resource_id'])
    domain = transformUrlToDomain(data_checker['url'])
    if domain in banned_domains:
        banned_yet = check_to_unban_by_domain(domain) if brokenlinksDB else True
        if banned_yet:
            log.debug("Resource with id %s banned temporally.", data_checker['resource_id'])
            return

    elif brokenlinksDB and not _compare_retry_attempt(
            brokenlinksDB.updated):
        log.debug("Check of resource with id %s skipped because it was recently checked.", data_checker['resource_id'])
        return
    result, status_code, reason = link_checker(data_checker)
    data_checker['result'] = result
    data_checker['status_code'] = status_code
    data_checker['reason'] = reason

    is_broken = dge_model.Status.is_status_broken_bl(data_checker['status_code'])
    last_success = None
    url_redirected_to = data_checker['url']
    updated = datetime.now()
    reason = data_checker['reason']
    failure_count = 0
    first_failure = None

    if brokenlinksDB:
        if is_broken:
            if not brokenlinksDB.first_failure: brokenlinksDB.first_failure = datetime.now()
            brokenlinksDB.failure_count += 1
        elif not is_broken and data_checker['status_code'] != HTTPStatus.REQUEST_TIMEOUT:
            brokenlinksDB.last_success = datetime.now()

        brokenlinksDB.status_id = data_checker['status_code']
        brokenlinksDB.is_broken = is_broken
        brokenlinksDB.url_redirected_to = url_redirected_to
        brokenlinksDB.reason = reason
        brokenlinksDB.updated = updated

        BrokenlinksDB.update(brokenlinksDB)

    else:
        if is_broken:
            first_failure = datetime.now()
            failure_count = 1
        else:
            last_success = datetime.now()

        brokenlinksDB = BrokenlinksDB(package_id=data_checker['package_id'], resource_id=data_checker['resource_id'],
                                      status_id=data_checker['status_code'], is_broken=is_broken,
                                      reason=reason, url_redirected_to=url_redirected_to,
                                      first_failure=first_failure, last_success=last_success,
                                      failure_count=failure_count, created=datetime.now(), updated=updated)

        BrokenlinksDB.create(brokenlinksDB)
    ban_domain(brokenlinksDB, domain) if brokenlinksDB.status_id == HTTPStatus.REQUEST_TIMEOUT else None


def _compare_retry_attempt(last_updated):
    '''
    Compare if the retry time has been completed to retry link check
    Return True if can be recheked
    Return False if not
    '''
    retry_time = config.get('ckanext.deg_brokenlinks.check_retry_time_segs') \
        if config.get('ckanext.deg_brokenlinks.check_retry_time_segs', None) \
        else os.environ.get('BROKENLINKS_RETRY_TIME_SEGS', None)
    retry_time = int(retry_time) if retry_time is not None else 45
    real_diference_time = datetime.now() - last_updated
    real_diference_seconds = real_diference_time.total_seconds()
    return True if (real_diference_seconds - retry_time) > 0 else False


def transformUrlToDomain(url):
    steps = url.split('/')
    domain = ''
    for i in range(3):
        domain += steps[i] + '/'

    return domain


def link_checker(data):
    """
    Check that the resource's url is valid, and accepts a HEAD request.

    Redirects are not followed - they simple return 'location' in the headers.

    data is a JSON dict describing the link:
        { 'url': url,
          'url_timeout': url_timeout }

    Raises LinkInvalidError if the URL is invalid
    Raises LinkHeadRequestError if HEAD request fails
    Raises LinkHeadMethodNotSupported if server says HEAD is not supported

    Returns a json dict of the headers of the request
    """
    if (type(data) == dict):
        data = json.dumps(data)

    data = json.loads(data)
    url_timeout = data.get('url_timeout', 30)

    error_message = ''
    headers = {'User-Agent': USER_AGENT} if USER_AGENT else None
    status_code = -1
    url = tidy_url(data['url'].strip())

    reason = ''
    res = None
    attempts = 0
    while attempts < 3:
        try:
            attempts += 1
            cert_path = config.get('requests.verify.ca_cert.path', '/etc/ssl/certs/ca-certificates.crt')
            if headers:
                res = requests.head(url, headers=headers, timeout=int(url_timeout), verify=cert_path)
            else:
                res = requests.head(url, timeout=int(url_timeout), verify=cert_path)
            status_code = res.status_code
            log.debug(status_code)
            for http_status in HTTPStatus:
                if res.status_code == http_status.value:
                    reason = http_status.phrase
            if (res.status_code >= 400 or not res.ok) and res.status_code != HTTPStatus.METHOD_NOT_ALLOWED and res.status_code != HTTPStatus.REQUEST_TIMEOUT:
                log.debug("Failed link check with %r, Headers: %s, Server returned HTTP error status: %s %s. Package is: %r. Resource is: %r", url,
                          res.headers, res.status_code, reason if reason else res.reason,
                          data.get('package_id'), data.get('resource_id'))
                error_message = _('Server returned HTTP error status: %s %s') % \
                                (res.status_code, reason if reason else res.reason)
                status_code = res.status_code
                reason = error_message

            break

        except http.client.InvalidURL as ve:
            log.debug("Could not make a head request to %r, error is: %s."
                      " Package is: %r. This sometimes happens when using an old version of requests on a URL"
                      " which issues a 301 redirect. Version=%s", url, ve, data.get('package'), requests.__version__)
            status_code = HTTPStatus.BAD_REQUEST
            reason = "Invalid URL or Redirect Link"
        except ValueError as ve:
            log.debug("Could not make a head request to %r, error is: %s. Package is: %r. Resource is: %r", url, ve,
                      data.get('package_id'), data.get('resource_id'))
            status_code = -1
            reason = COULD_NOT_MAKE_HEAD_REQUEST
        except requests.exceptions.ConnectionError as e:
            log.debug("Connection error to %r, error is: %s. Package is: %r. Resource is: %r", url, e,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.BAD_GATEWAY
            reason = 'Connection error: Failed to establish a new connection'
        except requests.exceptions.HTTPError as e:
            log.debug("Invalid HTTP response to %r, error is: %s. Package is: %r. Resource is: %r", url, e,
                      data.get('package_id'), data.get('resource_id'))
            reason = 'Invalid HTTP response: ' + e
        except requests.exceptions.Timeout:
            log.debug("Connection timed out after %ss to %r. Package is: %r. Resource is: %r", url_timeout, url,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.REQUEST_TIMEOUT
            reason = 'Connection timed out after ' + str(url_timeout) + 'secs'
        except requests.exceptions.TooManyRedirects:
            log.debug("Too many redirects to %r. Package is: %r. Resource is: %r", url,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.PERMANENT_REDIRECT
            reason = 'Too many redirects'
        except requests.exceptions.RequestException as e:
            log.debug("Request to %r, error is: %s. Package is: %r. Resource is: %r", url, e,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Error during request: ' + e
            if 'Task exceeded maximum timeout value' in reason or 'Connection timed out after' in reason:
                status_code = HTTPStatus.REQUEST_TIMEOUT
                reason = 'Connection timed out after ' + str(url_timeout) + 'secs'
        except JobTimeoutException as e:
            log.debug("Request to %r, error is: %s. Package is: %r. Resource is: %r", url, e,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.REQUEST_TIMEOUT
            reason = 'Connection timed out after ' + str(url_timeout) + 'secs'

        except Exception as e:
            log.debug("Request to %r, error is: %s. Package is: %r. Resource is: %r", url, e,
                      data.get('package_id'), data.get('resource_id'))
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            reason = 'Error with the request: ' + e

        finally:
            if status_code != HTTPStatus.REQUEST_TIMEOUT:
                break
    return json.dumps(dict(res.headers) if res else {}), status_code, reason if reason else res.reason


def ban_domain(brokenlinksDB, domain):
    '''
    Check if the domain have been banned and if it isn't banned yet but is broken,
    create a new register in the db or increment the count of the failures to the future ban
    '''

    brokendomainUrl = BrokendomainUrl.getByDomain(domain)

    session = dge_model.initSession()
    package = session.query(model.Package) \
        .filter_by(state='active', id=brokenlinksDB.package_id) \
        .order_by('name').first()

    if brokendomainUrl:
        brokendomainUrl.last_failure = datetime.now()
        brokendomainUrl.failure_count += 1
        banned_now, brokendomainUrl = check_to_unban(brokendomainUrl)
        if banned_now:
            BrokendomainUrl.update(brokendomainUrl)
            log.debug('Domain %s is updated to %s with %d recent failures.', domain, BrokendomainUrl.__tablename__, brokendomainUrl.failure_count)

    else:
        brokendomainUrl = BrokendomainUrl(domain=domain, organism=package.owner_org, first_failure=datetime.now(),
                                          last_failure=datetime.now(),
                                          failure_count=1, banned_until=None)
        BrokendomainUrl.create(brokendomainUrl)
        log.debug('Domain %s is added to %s as possible furure ban.', domain, BrokendomainUrl.__tablename__)

    global banned_domains
    banned_domains = getBannedDomains()


def tidy_url(url):
    '''
    Given a URL it does various checks before returning a tidied version
    suitable for calling.

    It may raise LinkInvalidError if the URL has a problem.
    '''

    # Find out if it has unicode characters, and if it does, quote them
    # so we are left with an ascii string
    try:
        url = url.decode('ascii')
    except Exception:
        parts = list(urlparse(url))
        parts[2] = unquote(parts[2])
        parts[2] = quote(parts[2].encode('utf-8'))
        url = urlunparse(parts)
    url = str(url)
    url = url.strip()

    # Use urllib3 to parse the url ahead of time, since that is what
    # requests uses, but when it does it during a GET, errors are not
    # caught well
    try:
        parsed_url = urllib3.util.parse_url(url)
    except urllib3.exceptions.LocationParseError as e:
        raise LinkInvalidError(_('URL parsing failure: %s') % e)

    if not parsed_url.scheme or not parsed_url.scheme.lower() in ALLOWED_SCHEMES:
        raise LinkInvalidError(_('Invalid url scheme. Please use one of: %s') %
                               ' '.join(ALLOWED_SCHEMES))

    if not parsed_url.host:
        raise LinkInvalidError(_('URL parsing failure - did not find a host name'))
    return url


def check_to_unban_by_domain(domain):
    '''
     Check if the domain will be unbanned or will still remain banned
    '''
    brokendomainUrl = BrokendomainUrl.getByDomain(domain)
    banned_yet, brokendomainUrl = check_to_unban(brokendomainUrl)
    return banned_yet


def check_to_unban_by_organism(organism=None, option=None):
    '''
     Check if the domains of the organism will be unbanned or will still remain banned
    '''
    if option:
        if option in 'selected':
            orgs_list = dge_model.CheckGroupArchiver.all_checkeable()
            for org in orgs_list:
                BrokendomainUrl.deleteByOrganismIfOverBannedDate(org.group_id)
        elif option in 'all':
            BrokendomainUrl.deleteAllIfOverBannedDate()

    elif organism:
        BrokendomainUrl.deleteByOrganismIfOverBannedDate(organism)


def check_to_unban(brokendomainUrl):
    '''
    Check if the domain is banned and unban it if the ban is over
    '''

    banned_yet = False
    # if have a register in the database and the failure count is over the limit allowed
    if brokendomainUrl.banned_until:
        if datetime.now() > brokendomainUrl.banned_until:
            BrokendomainUrl.delete(brokendomainUrl)
        else:
            banned_yet = True
    else:
        if brokendomainUrl.failure_count >= retry_attempts:
            if (brokendomainUrl.first_failure + timedelta(seconds=banned_until)) > datetime.now():
                # If the sum of the first failure plus the banned time is earlier than today, the domain is yet banned
                brokendomainUrl.banned_until = brokendomainUrl.first_failure + timedelta(seconds=banned_until)
                dge_logic.dge_brokenlinks_send_ban_mail(brokendomainUrl)
                banned_yet = True
            else:
                # If the banned time is over, delete of the table
                BrokendomainUrl.delete(brokendomainUrl)
        else:
            banned_yet = True

    return banned_yet, brokendomainUrl


def _update_resource(resource_id, queue, log):
    """
    Link check
    If successful, updates the brokenlinks table with the cache_url & hash etc.
    Finally, a notification of the brokenlinks is broadcast.

    Params:
      resource - resource dict
      queue - name of the celery queue

    Should only raise on a fundamental error:
      ArchiverError
      CkanError

    Returns a JSON dict, ready to be returned from the celery task giving a
    success status:
        {
            'resource': the updated resource dict,
            'file_path': path to archived file (if archive successful), or None
        }
    If not successful, returns None.
    """

    get_action = toolkit.get_action

    assert is_id(resource_id), resource_id
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session}
    resource = get_action('resource_show')(context_, {'id': resource_id})

    if not os.path.exists(settings.ARCHIVE_DIR):
        log.info("Creating archive directory: %s" % settings.ARCHIVE_DIR)
        os.mkdir(settings.ARCHIVE_DIR)

    def _save(status_id, exception, resource, url_redirected_to=None,
              download_result=None, archive_result=None):
        reason = u'%s' % exception
        save_archival(resource, status_id,
                      reason, url_redirected_to,
                      download_result, archive_result,
                      log)

    log.info('Attempting to archive resource')
    try:
        archive_result = archive_resource(context, resource, log, download_result)
    except ArchiveError as e:
        log.error('System error during archival: %r, %r', e, e.args)
        return

    return json.dumps(dict(download_result, **archive_result))


def is_id(id_string):
    '''Tells the client if the string looks like a revision id or not'''
    reg_ex = '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(reg_ex, id_string))


def notify_resource(resource, queue, cache_filepath):
    '''
    Broadcasts an IPipe notification that an resource brokenlinks has taken place
    (or at least the brokenlinks object is changed somehow).
    '''
    brokenlink_interfaces.IPipe.send_data('checked',
                                          resource_id=resource['id'],
                                          queue=queue,
                                          cache_filepath=cache_filepath)


def update_package(package_id, queue='bulk'):
    '''
    Archive a package.
    '''

    log.info('Starting update_package task: package_id=%r queue=%s',
             package_id, queue)

    # Do all work in a sub-routine since it can then be tested without celery.
    # Also put try/except around it is easier to monitor ckan's log rather than
    # celery's task status.
    try:
        _update_package(package_id, queue, log)
    except Exception as e:
        if os.environ.get('DEBUG'):
            raise
        # Any problem at all is logged and reraised so that celery can log it
        # too
        log.error('Error occurred during archiving package: %s\nPackage: %s',
                  e, package_id)
        raise


def _update_package(package_id, queue, log):
    get_action = toolkit.get_action

    num_archived = 0
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session}
    package = get_action('package_show')(context_, {'id': package_id})

    for resource in package['resources']:
        resource_id = resource['id']
        res = _update_resource(resource_id, queue, log)
        if res:
            num_archived += 1

    if num_archived > 0:
        log.info("Notifying package as %d items were archived", num_archived)
        notify_package(package, queue)
    else:
        log.info("Not notifying package as 0 items were archived")

    # Refresh the index for this dataset, so that it contains the latest
    # archive info. However skip it if there are downstream plugins that will
    # do this anyway, since it is an expensive step to duplicate.
    if 'qa' not in get_plugins_waiting_on_ipipe():
        _update_search_index(package_id, log)
    else:
        log.info('Search index skipped %s', package['name'])
