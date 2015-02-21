# -*- coding: utf-8 -*-

from sqlalchemy import *
from sqlalchemy.orm import create_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.exc import *
from sqlalchemy import desc
from config_mgr import get_config;

__author__ = 'duhyeong1.kim'

print "start alchemy init"
try :
    Base = declarative_base()
    print "mysql://" + get_config("mysql_accnt") + ":" + get_config("mysql_pwd") + "@" + get_config("mysql_ip") + ":" + get_config("mysql_port") +"/" +get_config("mysql_db_name") +"?charset=utf8";
    engine= create_engine("mysql://" + get_config("mysql_accnt") + ":" + get_config("mysql_pwd") + "@" + get_config("mysql_ip") + ":" + get_config("mysql_port") +"/" +get_config("mysql_db_name") +"?charset=utf8",encoding='utf-8',echo=False)
    metadata = MetaData(bind=engine)
    session = create_session(bind=engine)

except Exception as e:
    print e
    print "-----------------cannot bind DB using Alchemy---------------------"
print "meta data : -----------------" + str(metadata)

class User(Base):
    __table__ = Table('users', metadata, autoload=True)

class Book(Base):
    __table__ = Table('books', metadata, autoload=True)

class Bookimage(Base):
    __table__ = Table('bookimages', metadata, autoload=True)

print "end of alchemy init"
