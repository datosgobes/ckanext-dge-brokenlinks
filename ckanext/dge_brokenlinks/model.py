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
import itertools
import uuid
import os
import gevent
from builtins import str
from builtins import object
from datetime import datetime
from http import HTTPStatus
from ckan.plugins.toolkit import config

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import types, Table, Column, Index, MetaData
from sqlalchemy.exc import SQLAlchemyError, InvalidRequestError
from sqlalchemy import create_engine, insert, delete
from sqlalchemy.orm import mapper, sessionmaker
from ckanext.dge_brokenlinks.parameters import Parameters as param

import ckan.model as model

from ckan.lib import dictization

log = logging.getLogger(__name__)

Base = declarative_base()


def make_uuid():
    return str(uuid.uuid4())


DGE_BROKENLINKS_TABLE = 'check_brokenlinks'
DGE_BROKENLINKS_DOMAIN = 'check_brokenlinks_banned_domains'
__all__ = ['CheckGroupArchiver', 'BrokendomainUrl', 'BrokenlinksDB']

metadata = MetaData()

DGE_GROUP_BROKENLINKS_TABLE_TABLE = 'check_group_archiver'
dge_group_brokenliks_table = Table(
    DGE_GROUP_BROKENLINKS_TABLE_TABLE, metadata,
    Column('id', types.UnicodeText, primary_key=True,
           default=model.types.make_uuid),
    Column('group_id', types.UnicodeText),
    Column('checkeable', types.Boolean, default=False),
)


class BrokendomainUrl(Base):
    """
    Details of the BrokendomainUrl of resources.Basic error history provided for unsuccessful ones.
    """
    __tablename__ = DGE_BROKENLINKS_DOMAIN
    session = None
    _instance = None

    id = Column(types.UnicodeText, primary_key=True, default=make_uuid)
    domain = Column(types.UnicodeText, nullable=False, index=True)
    organism = Column(types.UnicodeText, nullable=False, index=True)

    # History
    first_failure = Column(types.DateTime, default=datetime.now)
    last_failure = Column(types.DateTime, default=datetime.now)
    failure_count = Column(types.Integer, default=1)

    banned_until = Column(types.DateTime)

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_banned_domains(cls):
        result = []
        cls.session = initSession() if not cls.session else cls.session
        if model.meta.engine.dialect.has_table(model.meta.engine, cls.__tablename__):
            result = cls.session.query(cls).all()
        cls.session.close()
        return result

    @classmethod
    def getByDomain(cls, domain):
        log.info(domain)
        cls.session = initSession() if not cls.session else cls.session
        result = cls.session.query(cls).filter_by(domain=domain).first()
        cls.session.close()
        return result

    @classmethod
    def getAllByOrganism(cls, organism):
        log.info('Find registers in %s by organism: %s',
                 cls.__tablename__, organism)
        cls.session = initSession() if not cls.session else cls.session
        result = cls.session.query(cls).filter_by(organism=organism).all()
        cls.session.close()
        return result

    @classmethod
    def deleteByOrganismIfOverBannedDate(cls, organism):
        log.info('Deleting registers in %s by organism: %s',
                 BrokendomainUrl.__tablename__, organism)
        result = model.Session.query(BrokendomainUrl).filter(
            BrokendomainUrl.organism == organism, BrokendomainUrl.banned_until < datetime.now()).delete()
        model.Session.commit()
        model.Session.close()
        log.info('Deleted %d registers in %s by organism: %s',
                 result, BrokendomainUrl.__tablename__, organism)

    @classmethod
    def deleteAllIfOverBannedDate(cls):
        date = datetime.now()
        log.info('Deleting registers in %s with a banned_until date is over than now: %s',
                 BrokendomainUrl.__tablename__, date)
        result = model.Session.query(BrokendomainUrl).filter(
            BrokendomainUrl.banned_until < date).delete()
        model.Session.commit()
        model.Session.close()
        log.info('Deleted %d registers in %s with a banned_until date is over than now: %s',
                 result, BrokendomainUrl.__tablename__, date)

    @classmethod
    def _save(cls, brokendomainUrl):
        try:
            cls.session.add(brokendomainUrl)
            cls.session.commit()
        except gevent.Timeout:
            cls.session.invalidate()
            raise
        except:
            cls.session.rollback()
            raise

        return brokendomainUrl

    @classmethod
    def _delete(cls, brokenlinksDB):
        try:
            cls.session.delete(brokenlinksDB)
            cls.session.commit()
        except gevent.Timeout:
            cls.session.invalidate()
            raise
        except:
            cls.session.rollback()
            raise

        return brokenlinksDB

    @classmethod
    def create(cls, brokendomainUrl):
        log.debug(param.START_METHOD, 'create BrokendomainUrl')
        try:
            brokendomainUrl = cls._save(brokendomainUrl)
        except InvalidRequestError:
            cls.session.rollback()
            brokendomainUrl = cls._save(brokendomainUrl)
        finally:
            log.debug('{0} row created. The Domain with id {1} have the url domain {2}'
                      .format(cls.__tablename__, brokendomainUrl.id, brokendomainUrl.domain))
            log.debug(param.END_METHOD, 'create BrokendomainUrl')
        return brokendomainUrl

    @classmethod
    def delete(cls, brokendomainUrl):
        log.debug(param.START_METHOD, 'delete BrokendomainUrl')
        cls._delete(brokendomainUrl)
        log.debug('{0} row delete. The Domain with id {1} and url domain {2} has been deleted'
                  .format(cls.__tablename__, brokendomainUrl.id, brokendomainUrl.domain))
        log.debug(param.END_METHOD, 'delete BrokendomainUrl')
        return brokendomainUrl

    @classmethod
    def update(cls, brokendomainUrl):
        log.debug(param.START_METHOD, 'update BrokendomainUrl')
        try:
            brokendomainUrl = cls._save(brokendomainUrl)
        except InvalidRequestError:
            model.Session.rollback()
            brokendomainUrl = cls._save(brokendomainUrl)
        finally:
            log.debug('{0} row updated. The resource with id {1} have the url domain {2}'
                      .format(cls.__tablename__, brokendomainUrl.id, brokendomainUrl.domain))
            log.debug(param.END_METHOD, 'update BrokendomainUrl')
        return brokendomainUrl

    @classmethod
    def empty_table(cls):
        if model.meta.engine.dialect.has_table(model.meta.engine, cls.__tablename__):
            model.meta.engine.execute(
                Base.metadata.tables[cls.__tablename__].delete())
            log.debug(param.EMPTY, cls.__tablename__)

    @classmethod
    def unban_by_organism(cls, organism):
        '''
         Delete the rows of the table no matter if the organism is banned or not
        '''
        try:
            log.debug('Unbanning the %s organism.', organism)
            result = model.Session.query(cls).filter(
                cls.organism == organism).delete()
            model.Session.commit()
            model.Session.close()
            if result == 0:
                log.debug('Organism %s does not exists.', organism)
            else:
                log.debug('Unbanned the %s organism.', organism)
        except:
            log.error('Error to delete the registers with organism %s.', organism)
            raise

    @classmethod
    def unban_by_domain(cls, domain):
        '''
         Delete the rows of the table no matter if the domain is banned or not
        '''
        try:
            log.debug('Unbanning the %s domain.', domain)
            result = model.Session.query(cls).filter(
                cls.domain == domain).delete()
            model.Session.commit()
            model.Session.close()
            if result == 0:
                log.debug('Domain %s does not exists.', domain)
            else:
                log.debug('Unbanned the %s domain.', domain)
        except:
            log.error('Error to delete the registers with domain %s.', domain)
            raise


# enum of all the archival statuses (singleton)
# NB Be very careful changing these status strings. They are also used in
# ckanext-qa tasks.py.
class Status(object):
    _instance = None

    def __init__(self):
        not_broken = {
            0: 'Archived successfully',
            1: 'Content has not changed',
        }
        broken = {
            10: 'URL invalid',
            11: 'URL request failed',
            12: 'Download error',
        }
        not_sure = {
            21: 'Chose not to download',
            22: 'Download failure',
            23: 'System error during archival',
        }
        self._by_id = dict(itertools.chain(not_broken.items(), broken.items()))
        self._by_id.update(not_sure)
        self._by_text = dict((value, key)
                             for key, value in self._by_id.items())

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def by_text(cls, status_txt):
        return cls.instance()._by_text[status_txt]

    @classmethod
    def by_id(cls, status_id):
        return cls.instance()._by_id[status_id]

    @classmethod
    def is_status_broken(cls, status_id):
        if 200 <= status_id < 400:
            return False
        elif status_id != 408:
            return True
        else:
            return None  # not sure

    @classmethod
    def is_status_broken_bl(cls, status_id):
        return True if (status_id >= 400 and status_id != HTTPStatus.METHOD_NOT_ALLOWED) \
                and status_id != HTTPStatus.REQUEST_TIMEOUT else False


    @classmethod
    def is_ok(cls, status_id):
        return status_id in [0, 1]


broken_enum = {True: 'Broken',
               None: 'Not sure if broken',
               False: 'Downloaded OK'}


class BrokenlinksDB(Base):
    """
    Details of the brokenlinks of resources. Has the filepath for successfully
    archived resources. Basic error history provided for unsuccessful ones.
    """
    __tablename__ = DGE_BROKENLINKS_TABLE
    session = None

    id = Column(types.UnicodeText, primary_key=True, default=make_uuid)
    package_id = Column(types.UnicodeText, nullable=False, index=True)
    resource_id = Column(types.UnicodeText, nullable=False, index=True)

    
    status_id = Column(types.Integer)
    is_broken = Column(types.Boolean)
    
    reason = Column(types.UnicodeText)
    url_redirected_to = Column(types.UnicodeText)

    # History
    first_failure = Column(types.DateTime)
    last_success = Column(types.DateTime)
    failure_count = Column(types.Integer, default=0)

    created = Column(types.DateTime, default=datetime.now)
    updated = Column(types.DateTime)

    def __repr__(self):
        broken_details = '' if not self.is_broken else \
                         ('%d failures' % self.failure_count)
        package = model.Package.get(self.package_id)
        package_name = package.name if package else '?%s?' % self.package_id
        return '<Brokenlinks %s /dataset/%s/resource/%s %s>' % \
            (broken_enum[self.is_broken], package_name, self.resource_id,
             broken_details)

    @classmethod
    def get_for_resource(cls, resource_id):
        '''Returns the Brokenlinks for the given resource, or if it doens't exist,
        returns None.'''
        cls.session = initSession() if not cls.session else cls.session
        return cls.session.query(cls).filter(cls.resource_id == resource_id).first()

    @classmethod
    def get_for_package(cls, package_id):
        '''Returns the Brokenlinks for the given package. May not be any if the
        package has no resources or has not been archived. It checks the
        resources are not deleted.'''
        return model.Session.query(cls) \
                    .filter(cls.package_id == package_id) \
                    .join(model.Resource, cls.resource_id == model.Resource.id) \
                    .filter(model.Resource.state == 'active') \
                    .all()

    @classmethod
    def get_last_updated_date_by_organization(cls, org):
        sql = '''select updated from check_brokenlinks cb where package_id in
        (select id from package p where owner_org in
        (select id from public.group where name = '{org}'))
        order by updated desc limit 1;'''.format(
            org=org)
        datetime = model.Session.execute(sql).fetchone()
        return datetime[0] if datetime and len(datetime) > 0 else ''

    @classmethod
    def create(cls, brokenlinksDB):
        log.debug(param.START_METHOD, 'create BrokenlinksDB')
        try:
            brokenlinksDB = brokenlinksDB.save(brokenlinksDB)
        except InvalidRequestError:
            cls.session.rollback()
            brokenlinksDB = brokenlinksDB.save(brokenlinksDB)
        finally:
            log.debug('{0} row created. The resource with id {1} have status code {2}'
                      .format(cls.__tablename__,  brokenlinksDB.resource_id, brokenlinksDB.status_id))
            log.debug(param.END_METHOD, 'create BrokenlinksDB')
        return brokenlinksDB

    @classmethod
    def update(cls, brokenlinksDB):
        log.debug(param.START_METHOD, 'update BrokenlinksDB')
        try:
            brokenlinksDB = brokenlinksDB.save(brokenlinksDB)
        except InvalidRequestError:
            model.Session.rollback()
            brokenlinksDB = brokenlinksDB.save(brokenlinksDB)
        finally:
            log.debug('{0} row updated. The resource with id {1} have status code {2}'
                      .format(cls.__tablename__,  brokenlinksDB.resource_id, brokenlinksDB.status_id))
            log.debug(param.END_METHOD, 'update BrokenlinksDB')
        return brokenlinksDB

    def save(self, brokenlinksDB):
        try:
            self.session.add(brokenlinksDB)
            self.session.commit()
        except gevent.Timeout:
            self.session.invalidate()
            raise
        except:
            self.session.rollback()
            raise

        return brokenlinksDB

    @property
    def status(self):
        if self.status_id is None:
            return None
        return self.status_id

    def as_dict(self):
        context = {'model': model}
        archival_dict = dictization.table_dictize(self, context)
        archival_dict['status_id'] = self.status_id
        archival_dict['is_broken_printable'] = broken_enum[self.is_broken]
        return archival_dict

    def init_tables_brokenlinks(self):
        log.debug(param.START_METHOD, 'init_tables_brokenlinks')
        log.info(model.meta.engine)
        if not model.meta.engine.dialect.has_table(model.meta.engine, self.__tablename__):
            Base.metadata.tables[self.__tablename__].create(
                bind=model.meta.engine)
            log.debug(param.CREATED, self.__tablename__)

        else:
            log.debug(param.ALREADY_EXISTS, self.__tablename__)

        if not model.meta.engine.dialect.has_table(model.meta.engine, BrokendomainUrl.__tablename__):
            Base.metadata.tables[BrokendomainUrl.__tablename__].create(
                bind=model.meta.engine)
            log.debug(param.CREATED, BrokendomainUrl.__tablename__)

        else:
            log.debug(param.ALREADY_EXISTS, BrokendomainUrl.__tablename__)

        if not dge_group_brokenliks_table.exists(model.meta.engine):
            dge_group_brokenliks_table.create(model.meta.engine)
            log.debug(param.CREATED, CheckGroupArchiver.__tablename__)
        else:
            log.debug(param.ALREADY_EXISTS, CheckGroupArchiver.__tablename__)
        log.debug(param.END_METHOD, 'init_tables_brokenlinks')

    def drop_tables_brokenlinks(self):
        log.debug(param.START_METHOD, 'drop_tables_brokenlinks')
        if model.meta.engine.dialect.has_table(model.meta.engine, self.__tablename__):
            Base.metadata.tables[self.__tablename__].drop(
                bind=model.meta.engine)
            log.debug(param.DROPPED, self.__tablename__)

        else:
            log.debug(param.TRYING_DROP, self.__tablename__)

        if model.meta.engine.dialect.has_table(model.meta.engine, BrokendomainUrl.__tablename__):
            Base.metadata.tables[BrokendomainUrl.__tablename__].drop(
                bind=model.meta.engine)
            log.debug(param.DROPPED, BrokendomainUrl.__tablename__)

        else:
            log.debug(param.TRYING_DROP, BrokendomainUrl.__tablename__)

        if dge_group_brokenliks_table.exists(model.meta.engine):
            dge_group_brokenliks_table.drop(model.meta.engine)
            log.debug(param.DROPPED, CheckGroupArchiver.__tablename__)

        else:
            log.debug(param.TRYING_DROP, CheckGroupArchiver.__tablename__)

        log.debug(param.END_METHOD, 'drop_tables_brokenlinks')

    def empty_tables_brokenlinks(self):
        log.debug(param.START_METHOD, 'empty_tables_brokenlinks')
        if model.meta.engine.dialect.has_table(model.meta.engine, self.__tablename__):
            model.meta.engine.execute(
                Base.metadata.tables[self.__tablename__].delete())
            log.debug(param.EMPTY, self.__tablename__)
        else:
            log.debug(param.TRYING_EMPTY, self.__tablename__)

        if model.meta.engine.dialect.has_table(model.meta.engine, BrokendomainUrl.__tablename__):
            model.meta.engine.execute(
                Base.metadata.tables[BrokendomainUrl.__tablename__].delete())
            log.debug(param.EMPTY, BrokendomainUrl.__tablename__)
        else:
            log.debug(param.TRYING_EMPTY, BrokendomainUrl.__tablename__)

        if dge_group_brokenliks_table.exists(model.meta.engine):
            delete(dge_group_brokenliks_table)
            log.debug(param.EMPTY, CheckGroupArchiver.__tablename__)
        else:
            log.debug(param.TRYING_EMPTY, CheckGroupArchiver.__tablename__)

        log.debug(param.END_METHOD, 'empty_tables_brokenlinks')


def aggregate_archivals_for_a_dataset(archivals):
    '''Returns aggregated archival info for a dataset, given the archivals for
    its resources (returned by get_for_package).

    :param archivals: A list of the archivals for a dataset's resources
    :type archivals: A list of Archival objects
    :returns: Archival dict about the dataset, with keys:
                status_id
                status
                reason
                is_broken
    '''
    archival_dict = {'status_id': None, 'status': None,
                     'reason': None, 'is_broken': None}
    for archival in archivals:
        if archival_dict['status_id'] is None or \
                archival.status_id > archival_dict['status_id']:
            archival_dict['status'] = archival.status_id
            archival_dict['is_broken'] = archival.is_broken
            archival_dict['reason'] = archival.reason

    return archival_dict


class CheckGroupArchiver(object):
    '''
	CheckGroupArchiver saves a registry of wich organizations are going to have their resources checked with the archiver extension of CKAN
	'''

    __tablename__ = DGE_GROUP_BROKENLINKS_TABLE_TABLE

    def __init__(self, **kwargs):
        for k, v in list(kwargs.items()):
            setattr(self, k, v)

    @classmethod
    def get(cls, group_id=None):
        '''
		Retrieves all the rows if group_id is not specified, retrieves one if it is specified
		'''
        item = None
        if group_id:
            item = model.Session.query(cls).filter(
                cls.group_id == group_id).first()
        else:
            item = model.Session.query(cls)

        return item

    @classmethod
    def all(cls):
        """
		Returns all groups.
		"""
        return model.Session.query(cls).all()

    @classmethod
    def all_checkeable(cls):
        """
		Returns all groups with ckeckable True.
		"""
        sql = '''select * from {p0} cga where cga.checkeable = true
        and group_id in (select id from public.group g where g.id
        in (select distinct (p.owner_org) from package p where state = 'active'
        and exists(select * from resource r where r.package_id = p.id)) and g.state = 'active');'''.format(p0 = cls.__tablename__)
        return model.Session.execute(sql).fetchall()

    def toggle_check(cls, group_id):
        '''
		Set to True or False if archiver can check all of the resources of the group specified, depending of the actual value in DB
		'''
        item = model.Session.query(cls).filter(
            cls.group_id == group_id).first()

        if item:
            item.checkeable = False if item.checkeable else True

    @classmethod
    def add_org(cls, org):
        org_to_add = CheckGroupArchiver(
            **{'group_id': org, 'checkeable': True})
        model.Session.add(org_to_add)
        model.Session.commit()

    @classmethod
    def getNumberofResourcesByOrgId(cls, id):

        sql = '''
        select count(*) from resource r
        where r.package_id in
        (select id from package p where p.owner_org = '{p0}' and p.state='active');
        '''.format(p0=id)
        return model.Session.execute(sql).fetchall()[0][0]

    @classmethod
    def updateCheckedGroups(cls, organizations):
        items_list = ''
        if (len(organizations) > 0):
            sql = 'select id from public.group where state = \'active\' and is_organization is true and name in ('
            for idx, org in enumerate(organizations):
                if idx == (len(organizations) - 1):
                    sql += '\'' + org + '\''
                else:
                    sql += '\'' + org + '\', '
            sql += ');'
            list = model.Session.execute(sql).fetchall()

            groups_archiver = CheckGroupArchiver.all()
            groups_archiver_ids = []
            for g in groups_archiver:
                groups_archiver_ids.append(g.group_id)

            for idx, org in enumerate(list):
                if org.id in groups_archiver_ids:
                    log.debug("Organization in DB=%s" % org.id)
                else:
                    log.debug(
                        "Organization NOT in DB=%s | Adding it to check_group_archiver..." % org.id)
                    CheckGroupArchiver.add_org(org.id)

                if idx == (len(list) - 1):
                    items_list += '\'' + org.id + '\''
                else:
                    items_list += '\'' + org.id + '\', '
        sql_check = None
        if items_list: 
            sql_check   = 'update public.' + cls.__tablename__ + ' set checkeable = true where group_id in (' + items_list + ');'
            sql_uncheck = 'update public.' + cls.__tablename__ + ' set checkeable = false where group_id not in (' + items_list + ');'
        else: 
            sql_uncheck = 'update public.' + cls.__tablename__ + ' set checkeable = false;'

        if sql_check:
            model.Session.execute(sql_check)
        model.Session.execute(sql_uncheck)
        model.Session.commit()

    @classmethod
    def commit(cls):
        try:
            model.Session.commit()
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error


mapper(CheckGroupArchiver, dge_group_brokenliks_table)


def get_resources_id_by_package_id(package_id):
    return session.query(model.Resource).filter_by(package_id=package_id, state='active')


def initSession():
    engine_path = os.environ.get('CKAN_SQLALCHEMY_URL', None)
    if not engine_path:
        engine_path = config.get('sqlalchemy.url')
    engine = create_engine(engine_path, pool_pre_ping=True)
    Session = sessionmaker(bind=engine)
    return Session()


def closeSession():
    session.close()


def getGroupsById(organizations):
    orgs_str = 'select id, name, title from public.group where state = \'active\' and is_organization is true and id in ('
    for idx, org in enumerate(organizations):
        if idx == (len(organizations) - 1):
            orgs_str += '\'' + org + '\''
        else:
            orgs_str += '\'' + org + '\', '
    orgs_str += ');'
    return model.Session.execute(orgs_str).fetchall()


def getBrokenlinksByOrganizationName(org_name, data_dict):

    limit = data_dict.get('limit', 100)
    if not isinstance(limit, int):
        limit = 100
   
    offset = data_dict.get('offset', 0)
    if not isinstance(offset, int):
        offset = 0

    types = data_dict.get('types', [])
    if not isinstance(types, list):
        types = []

    types_str = ''
    if len(types) > 0:
        types_str = 'cb.status_id in ({}) and '.format(','.join(map(str, types)))

    broken_links_sql = '''select cb.*, p.id, p.name, p.title, p.owner_org from check_brokenlinks cb
        join package p on p.id = cb.package_id
        join resource r on r.id = cb.resource_id
        where cb.status_id >= '400' and cb.status_id not in ('405', '408') and {types}
        p.state = 'active' and r.state = 'active' and p.owner_org in 
        (select id from "group" g where name like '{org}' and state like 'active')
        limit {limit} offset {offset};'''.format(types=types_str, org=org_name, limit=limit, offset=offset)

    total_broken_links_sql = '''select count(cb.id) from check_brokenlinks cb
        join package p on p.id = cb.package_id
        join resource r on r.id = cb.resource_id
        where cb.status_id >= '400' and cb.status_id not in ('405', '408') and {types}
        p.state = 'active' and r.state = 'active' and p.owner_org in 
        (select id from "group" g where name like '{org}' and state like 'active');
        '''.format(types=types_str, org=org_name)

    broken_links = model.Session.execute(broken_links_sql).fetchall()
    total_broken_links = model.Session.execute(total_broken_links_sql).fetchall()[0][0]

    return broken_links, total_broken_links


session = initSession()
