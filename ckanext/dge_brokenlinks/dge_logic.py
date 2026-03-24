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
import time
import smtplib
import json
from datetime import datetime
from ckan.logic import NotFound
from email.mime.text import MIMEText
from ckan.lib.mailer import MailerException
import ckan.model as model
from ckan.model.group import Group
from ckan.plugins import toolkit
from ckanext.dge_brokenlinks import utils
from ckanext.dge_brokenlinks.model import CheckGroupArchiver
from ckanext.dge_brokenlinks.parameters import Parameters as par
from ckanext.report.model import DataCache
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
from ckan.plugins.toolkit import config

log = logging.getLogger(__name__)

def dge_organism_check_broken_links(option):
    import operator
    log.debug(par.START_METHOD, 'organism_check_broken_links')
    if option in 'selected':
        # Find in CheckGroupArchiver table all the organims
        organizations = CheckGroupArchiver.all_checkeable()
    elif option in 'all':
        # Find all the diferents organism in the database and simulate that all are checkeables
        organizations = CheckGroupArchiver.all()
    else:
        # Simulate a CheckGroupArchiver object that is checkeable and create a list with only this object
        organizations = [CheckGroupArchiver(group_id=option, checkeable=True)]

    group_ids = [o.group_id for o in organizations if o.checkeable]
    count = {}
    for id in group_ids:
        result = CheckGroupArchiver.getNumberofResourcesByOrgId(id)
        count[id] = result

    sorted_d = sorted(count.items(), key=operator.itemgetter(1))
    ckan_path = config.get('ckanext.config_path', None)
    if (ckan_path):
        for sort in sorted_d:
            cmd = config.get('ckanext-dge-brokenlinks.click_link_checker_command') + ' ' + sort[0]
            os.system(cmd)
            time.sleep(1)  # to try to avoid machine getting overloaded
    else:
        log.warn("ckan config file not found. The task won't be created")

    log.debug(par.END_METHOD, 'organism_check_broken_links')


def dge_brokenlinks_group_to_check(groups_to_check):
    log.debug('DENTRO DE dge_brokenlinks_group_to_check')
    groups_to_check_ids = []
    for name in groups_to_check:
        group = Group.search_by_name_or_title(name, is_org=True).first()
        if group:
            groups_to_check_ids.append(group.id)
    log.debug('group_list:')
    log.debug(groups_to_check_ids)

    groups_archiver = CheckGroupArchiver.all()

    groups_archiver_ids = []
    for g in groups_archiver:
        groups_archiver_ids.append(g.group_id)
        g.checkeable = False
        log.debug("Checkeable a FALSE")
    log.debug('all_group_id:')
    log.debug(groups_archiver_ids)

    for org in groups_to_check_ids:
        if org in groups_archiver_ids:
            log.debug("Organization in DB")
        else:
            CheckGroupArchiver.add_org(org)

    for o in groups_archiver:
        if o.group_id in groups_to_check_ids:
            o.checkeable = True
            log.debug("Checkeable a TRUE")

    CheckGroupArchiver.commit()

def dge_brokenlinks_get_checkeable_groups():
    check_groups = CheckGroupArchiver.all()
    result = []

    for check_group in check_groups:
        if check_group.checkeable:
            group = Group.get(check_group.group_id)
            if group is not None:
                result.append(group.name)

    return result


def dge_brokenlinks_check_broken_links(context):
    context.log.debug('DENTRO DE dge_brokenlinks_check_broken_links')
    method_log_prefix = '[%s][[dge_brokenlinks_check_broken_links]' % __name__
    context.log.debug('%s Init method.' % (method_log_prefix))

    organizations = CheckGroupArchiver.all()
    group_ids = [o.group_id for o in organizations if o.checkeable]

    for id in group_ids:
        cmd = '/var/lib/ckan/default/bin/paster --plugin=ckanext-archiver archiver update ' + id + ' -c /etc/ckan/default/production.ini'
        os.system(cmd)
        time.sleep(1)  # to try to avoid machine getting overloaded


def dge_brokenlinks_send_ban_mail(brokenlinksUrl):
    log.debug(par.START_METHOD, 'dge_brokenlinks_send_ban_mail')
    organization = utils.getOrganizationById(brokenlinksUrl.organism)
    message, mail_to, mail_ccs, mail_bccs = dge_brokenlinks_buildmail(organization, model, 'ban', brokenlinksUrl)

    log.debug('mail_to: %s mail_ccs: %s mail_bccs: %s' % (mail_to, mail_ccs, mail_bccs))
    try:
        log.info('########################################################')
        log.info(message['From'])
        log.info(mail_to + mail_ccs), message
        log.info(message)
        _dge_brokenlinks_send_email(message['From'], (mail_to + mail_ccs), message)
    except MailerException as e:
        msg = '%r' % e
        log.exception('Exception sending email.')
    finally:
        log.debug(par.END_METHOD, 'dge_brokenlinks_send_ban_mail')


def dge_brokenlinks_report_email_finished():
    log.debug(par.START_METHOD, 'dge_brokenlinks_report_email_finished')

    organizations = CheckGroupArchiver.all()
    result = []

    for check_group in organizations:
        if check_group.checkeable:
            group = Group.get(check_group.group_id)
            if group is not None:
                result.append(group)

    organizations = result

    for organization in organizations:
        object_id = organization.name
        key = 'broken-links?organization=%s&include_sub_organizations=0' % (object_id)

        value, created = DataCache.get(object_id, key, convert_json=True)
        if value is None or created is None:
            raise NotFound
        if created.strftime('%d-%m-%Y') != datetime.today().strftime('%d-%m-%Y'):
            continue
        if value['num_broken_packages'] == 0 and value['num_broken_resources'] == 0:
            continue
        message, mail_to, mail_ccs, mail_bccs = dge_brokenlinks_buildmail(organization, model, 'report')
        log.debug('mail_to: %s mail_ccs: %s mail_bccs: %s' % (mail_to, mail_ccs, mail_bccs))
        try:
            log.info('########################################################')
            log.info(message['From'])
            log.info(mail_to + mail_ccs), message
            log.info(message)
            _dge_brokenlinks_send_email(message['From'], (mail_to + mail_ccs), message)
        except MailerException as e:
            msg = '%r' % e
            log.exception('Exception sending email.')
        finally:
            log.debug(par.END_METHOD, 'dge_brokenlinks_report_email_finished')
            time.sleep(2)


def dge_brokenlinks_buildmail(organization, model, use, brokendomainUrl = None):

    log.debug(organization.id)

    members = toolkit.get_action('member_list')(
        data_dict={'id': organization.id, 'table_name': 'user', 'capacity': 'editor', 'state': 'active'})
    if members is None:
        raise NotFound

    # To
    mail_to = []
    for member in members:
        user = model.User.get(member[0])
        if user and user.state == 'active' and user.email and len(user.email) > 0:
            mail_to.append(user.email)

    # From
    mail_from = config.get('smtp.mail_from')
    # CC
    mail_ccs = config.get('smtp.mail_cc', '').split(' ')
    # BCC
    mail_bccs = config.get('smtp.mail_bcc', '').split(' ')
    # Reply-To
    mail_reply_to = config.get('smtp.mail_reply_to', None)
    
    # Template
    path = config.get('ckanext-dge-brokenlinks.template.path_emails')
    url = config.get('ckanext.comments.url.images.drupal')
    url_logos = config.get('ckanext.comments.url.image.logos')
    url_image_subscribe = config.get('ckanext.comments.url.image.subscribe')
    url_subscribe = config.get('ckanext.comments.url.subscribe')
    site_title = config.get('ckan.site_title', 'datos.gob.es')
    site_url = config.get('ckan.site_url')
    env = Environment(loader=FileSystemLoader(path))
        
    if use in 'ban':
        # Subject
        subject = f'Dominio {brokendomainUrl.domain} bloqueado temporalmente en {site_title} durante la revisión de enlaces rotos'
        
        # Body
        url_report = config.get('ckan.site_url') + "/report/broken-links?organization=" + organization.name
        url_report = url_report.replace("http://", "https://")
        banned_until = brokendomainUrl.banned_until.strftime(u'%d-%m-%Y %H:%M:%S')
        blocked_domain_template = env.get_template('blocked_domain.html')
        body = blocked_domain_template.render(
            url=url,
            url_logos=url_logos,
            url_image_subscribe=url_image_subscribe,
            url_subscribe=url_subscribe,
            site_title=site_title,
            site_url=site_url,
            brokendomainUrl=brokendomainUrl,
            organization=organization,
            url_report=url_report,
            banned_until=banned_until
        )
        
    elif use in 'report':
        # Subject
        subject = f'Enlaces rotos entre sus conjuntos de datos ({site_title})'
        # Body
        url_report = site_url + "/report/broken-links?organization=" + organization.name
        url_report = url_report.replace("http://", "https://")
        datetime_today = datetime.today().strftime('%d/%m/%Y')
        broken_link_template = env.get_template('broken_link.html')
        body = broken_link_template.render(
            url=url,
            url_logos=url_logos,
            url_image_subscribe=url_image_subscribe,
            url_subscribe=url_subscribe,
            site_title=site_title,
            url_report=url_report,
            datetime_today=datetime_today
        )

    msg = MIMEText(body, 'html')
    if mail_from:
        msg['From'] = mail_from
    if mail_reply_to:
        msg['Reply-To'] = mail_reply_to
    if mail_to and len(mail_to) > 0:
        msg['To'] = ", ".join(mail_to)
    if mail_ccs and len(mail_ccs) > 0:
        msg['Cc'] = ", ".join(mail_ccs)
    if mail_bccs and len(mail_bccs) > 0:
        msg['Bcc'] = ", ".join(mail_bccs)
    msg['Subject'] = subject

    return msg, mail_to, mail_ccs, mail_bccs

def _dge_brokenlinks_send_email(from_addr, to_addrs, msg):
    from socket import error as socket_error

    log.info("Sending email from {0} to {1}".format(from_addr, to_addrs))
    if from_addr and to_addrs:
        smtp_connection = smtplib.SMTP()
        if 'smtp.test_server' in config:
            smtp_server = config['smtp.test_server']
            smtp_starttls = False
            smtp_user = None
            smtp_password = None
        else:
            smtp_server = config.get('smtp.server', 'localhost')
            smtp_starttls = False
            smtp_user = config.get('smtp.user')
            smtp_password = config.get('smtp.password')
        try:
            smtp_connection.connect(smtp_server)
            smtp_connection.ehlo()

            if smtp_starttls:
                if smtp_connection.has_extn('STARTTLS'):
                    smtp_connection.starttls()
                    smtp_connection.ehlo()
                else:
                    raise MailerException("SMTP server does not support STARTTLS")

            if smtp_user:
                assert smtp_password, ("If smtp.user is configured then "
                                       "smtp.password must be configured as well.")
                smtp_connection.login(smtp_user, smtp_password)

            smtp_connection.sendmail(from_addr, to_addrs, msg.as_string())
            smtp_connection.quit()
            log.info("Sent email from {0} to {1}".format(from_addr, to_addrs))

        except smtplib.SMTPException as e:
            msg = '%r' % e
            log.exception(msg)
            raise MailerException(msg)
        except AttributeError as e:
            msg = '%r' % e
            log.exception(msg)
            raise MailerException(msg)
        except socket_error as e:
            log.exception(e)
            raise MailerException(e)
    else:
        log.info("Skip sending email. From_addr ({0}) or to_addrs ({1}) aren't correct".format(from_addr, to_addrs))


@toolkit.auth_allow_anonymous_access
def dge_brokenlinks_auth(context, data_dict):
    '''
    All users can access DCAT endpoints by default
    '''
    return {'success': True}

