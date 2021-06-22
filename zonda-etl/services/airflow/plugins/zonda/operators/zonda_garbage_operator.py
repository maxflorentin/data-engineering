#!/usr/bin/python3

import re
from itertools import chain
from os import environ, path
from platform import system, release
from socket import gethostname
from sys import exit
from datetime import datetime, timedelta
import os,getpass
import time
from pywebhdfs.webhdfs import PyWebHdfsClient, errors
import kerberos
import json
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED
import ssl
import urllib3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



import subprocess



class HDFS:
    @staticmethod
    def hdfs_getHdfsCli(hdfs_conn):
        auth = HTTPKerberosAuth(hostname_override=hdfs_conn['hdfs_host'])
        base_uri = "https://" + hdfs_conn['hdfs_host'] + ":" + hdfs_conn['hdfs_port'] + "/webhdfs/v1/"
        print(os.getegid())
        print(getpass.getuser())
        print(base_uri)
        hdfs_client = PyWebHdfsClient(request_extra_opts={'auth': auth, 'verify': False}, base_uri_pattern=base_uri)
        return hdfs_client

    @staticmethod
    def hdfs_detectit(hdfs_conn, landing_date,  path):
        ## SET HDFS CLIENT
        hdfs_clie = HDFS.hdfs_getHdfsCli(hdfs_conn)

        try:
            print(path)
            print(hdfs_conn)
            data = hdfs_clie.list_dir(path)
            print(data)
        except:
            print('ZONDA:Hadoop Directory Not Found')

class ZondaGDOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        self.log.info('--------------------------GET DAG VARIABLES-------------------------------')
        super(ZondaGDOperator, self).__init__(*args, **kwargs)
        self.hdfs_host = kwargs.get('hdfs_host')
        self.hdfs_port = kwargs.get('hdfs_port')
        self.hdfs_user_name = kwargs.get('hdfs_user_name')
        self.path = kwargs.get('path')
        self.threshold = kwargs.get('threshold')

    def execute(self, context):
        """
        Execute operator
        :param context: dag context
        :return: None
        """
        self.hdfs_conn = {}
        self.hdfs_conn['hdfs_host'] = self.hdfs_host
        self.hdfs_conn['hdfs_user'] = self.hdfs_user_name
        self.hdfs_conn['hdfs_port'] = self.hdfs_port
        self.landing_date = datetime.now()
        self.log.info('--------------------------------------------------------------------------')
        self.log.info('landing date ' + self.landing_date.strftime('%Y-%m-%d'))
        self.log.info('--------------------------EXECUTE----------------------------------------')
        HDFS.hdfs_detectit(self.hdfs_conn, self.landing_date, self.path)
        self.log.info("OK")


    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
