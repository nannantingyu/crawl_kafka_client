# -*- coding: utf-8 -*-
from dotenv import load_dotenv
from os import environ

load_dotenv('.env')
DATABASE = {
    'drivername': 'mysql+pymysql',
    'host':     environ.get("db_host"),
    'port':     environ.get("db_port"),
    'username': environ.get("db_username"),
    'password': environ.get("db_password"),
    'database': environ.get("db_database"),
    'query': {'charset': 'utf8'}
}

kafka = {
    'host': environ.get("cafca_host"),
    'port': environ.get("cafca_port")
}