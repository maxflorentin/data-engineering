#!/usr/bin/python3

import re
from itertools import chain
from os import environ, path
from platform import system, release
from socket import gethostname
from sys import exit
from datetime import datetime, timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import time
from pywebhdfs.webhdfs import PyWebHdfsClient, errors
import kerberos
import json
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED
import ssl
import urllib3


import subprocess


def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
        assert (False)
    return (output, errors)


class HDFS:
    @staticmethod
    def hdfs_getHdfsCli(hdfs_conn):
        auth = HTTPKerberosAuth(hostname_override=hdfs_conn['hdfs_host'])
        base_uri = "https://" + hdfs_conn['hdfs_host'] + ":" + hdfs_conn['hdfs_port'] + "/webhdfs/v1/"
        hdfs_client = PyWebHdfsClient(request_extra_opts={'auth': auth, 'verify': False}, base_uri_pattern=base_uri)
        return hdfs_client

    @staticmethod
    def hdfs_moveit(hdfs_conn, landing_date, model, table, path_from, files, delta):
        ## DECLARE HDFS PATHS
        hdfs_path = os.path.join(os.sep + 'santander/bi-corp/landing', model, table, str(landing_date.year),
                                 landing_date.strftime('%m'), landing_date.strftime('%d'))
        hdfs_path_history = os.path.join(hdfs_path, 'history')

        ## SET HDFS CLIENT
        hdfs_clie = HDFS.hdfs_getHdfsCli(hdfs_conn)

        try:
            hdfs_clie.list_dir(hdfs_path)
        except:
            print('ZONDA:Hadoop Directory Not Found')
            hdfs_clie.make_dir(hdfs_path)
            print('ZONDA:Hadoop Directory Sucessfully Created')

        if not (delta):  ## False significa que guarda historia
            if (len(hdfs_clie.list_dir(hdfs_path)["FileStatuses"]["FileStatus"]) > 0):
                try:
                    hdfs_clie.list_dir(hdfs_path_history)
                except:
                    print('ZONDA:Hadoop Historic Directory Not Found')
                    hdfs_clie.make_dir(hdfs_path_history)
                    print('ZONDA:Hadoop Historic Directory Sucessfully Created')
                for each_file in hdfs_clie.list_dir(hdfs_path)["FileStatuses"]["FileStatus"]:
                    # MOVE HISTORY FILES TO A LANDING HISTORY FILES AREA OF THE LANDING DATE
                    if each_file['type'] == 'FILE':
                        hdfs_clie.rename_file_dir(os.path.join(hdfs_path, each_file['pathSuffix']),
                                                  os.path.join(hdfs_path_history,
                                                               str(each_file['modificationTime']) + '_' + each_file[
                                                                   'pathSuffix'] + '.old'))
                        print('ZONDA: ' + each_file['pathSuffix'] + ' file was moved')

        ## MOVE IT
        os.environ['PYTHONIOENCODING'] = "UTF-8"
        for filename in files:
            print(os.path.join(path_from, filename))
            try:
                (out, errors) = run_cmd(['hadoop', 'fs', '-put', '-f', os.path.join(path_from, filename),
                                         os.path.join(hdfs_path, filename)])
                os.remove(os.path.join(path_from, filename))
            except:
                print('ZONDA: Error loading file in HDFS')
                assert (False)
            print('--------------------------UPLOADED ' + filename + '------------------------------------')
        return hdfs_path

    @staticmethod
    def hdfs_prepareit(model, table, landing_date, path, regexp, hdfs_conn, delta, pdate):
        print('--------------------------HDFS-Prepare-It------------------------------------')

        yyyyddmm = str(landing_date.year) + landing_date.strftime('%m') + landing_date.strftime('%d')
        pattern = re.compile('.*' + yyyyddmm + '.*')
        files = []
        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                if regexp and pattern.match(file):
                    files.append(file)
                    print('--------------------------' + file + ' accepted------------------------------------')
                elif not (regexp):
                    files.append(file)
                    print('--------------------------' + file + ' accepted------------------------------------')
                else:
                    print(not (regexp))
                    print('--------------------------' + file + ' not accepted------------------------------------')
            else:
                print('--------------------------' + path + ' dont have files------------------------------------')
                assert (False)

        if len(files) > 0:
            hdfs_path = HDFS.hdfs_moveit(hdfs_conn, landing_date, model, table, path, files, delta)
            if pdate:
                return (hdfs_path + ',' + yyyyddmm)
            else:
                return (hdfs_path)
        else:
            assert (False)


class ZondaMoveItOperator(BaseOperator):
    @apply_defaults
    def __init__(self, hdfs_host, hdfs_port, hdfs_user_name, path, table, model, pdate=False, delta=False, shift=0, regexp_date_files=True, filewatch_ttl_min=30, *args, **kwargs):

        self.log.info('--------------------------GET DAG VARIABLES-------------------------------')
        super(ZondaMoveItOperator, self).__init__(*args, **kwargs)

        self.params = {'path': path, 'table': table, 'shift': shift, 'regexp_date_files': regexp_date_files,
                       'filewatch_ttl_min': filewatch_ttl_min, 'model': model,
                       'hdfs_host': hdfs_host, 'hdfs_port': str(hdfs_port),
                       'hdfs_user_name': hdfs_user_name, 'delta': delta, 'pdate': pdate}

    def execute(self, context):
        """
        Execute operator
        :param context: dag context
        :return: None
        """
        self.path = self.params.get('path')
        self.table = self.params.get('table')
        self.ttl = int(self.params.get('filewatch_ttl_min')) * 60
        self.model = self.params.get('model')
        self.delta = bool(self.params.get('delta'))
        self.pdate = bool(self.params.get('pdate'))

        self.hdfs_conn = {}
        self.hdfs_conn['hdfs_host'] = self.params.get('hdfs_host')
        self.hdfs_conn['hdfs_user'] = self.params.get('hdfs_user_name')
        self.hdfs_conn['hdfs_port'] = self.params.get('hdfs_port')

        self.regexp_date_files = self.params.get('regexp_date_files')
        self.landing_date = datetime.now() - timedelta(days=int(self.params.get('shift')))

        self.log.info('--------------------------------------------------------------------------')
        self.log.info('landing date ' + self.landing_date.strftime('%Y-%m-%d'))
        self.log.info('--------------------------EXECUTE----------------------------------------')
        ##check if path receiver exists if not, create it .
        timer = 0
        self.path = os.path.join(self.path, self.model, self.table)
        while int(self.ttl) > int(timer):
            timer += 60
            self.log.info(self.path)
            if (os.path.exists(self.path)) and len(os.listdir(self.path)) > 0:
                self.log.info('-----------------------------Path & File Exists-----------------------------')
                hdfs_landing_path = HDFS.hdfs_prepareit(self.model, self.table, self.landing_date, self.path, self.regexp_date_files,
                                                        self.hdfs_conn, self.delta, self.pdate)
                return hdfs_landing_path
                break
            else:
                self.log.info('--------------------------Filewatch ' + self.path + ' will wait one more minute------------')
                time.sleep(60)
        if int(self.ttl) <= int(timer):
            self.log.info('--------------------------Filewatch finished with no files----------------------------------------')
            assert (False)

    def __repr__(self):
        """
        Object string representation
        :return: str representation of this object
        """
        return str(self)
