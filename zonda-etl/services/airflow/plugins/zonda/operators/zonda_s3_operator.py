# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import re
import os
import urllib3
from datetime import datetime
from pathlib import Path

REGEX_S3_FILE = '^s3:/*||^/*'
REGEX_DATE_FILE = '(^.*([12]\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])).*$)'
REGEX_IS_FILE = '(\/.*?\.[\w:]+)'

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class S3Operation:
    PUT = 'put'


class ZondaS3Operator(BaseOperator):
    template_fields = ['operation', 'local_path', 'remote_path', 's3_bucket', 'source_s3_conn_id', 'overwrite']

    @apply_defaults
    def __init__(self, operation='Put', local_path=None, remote_path=None, s3_bucket=None,
                 source_s3_conn_id='aws_s3_desagote', *args, **kwargs):
        """
        Constructor
        """
        super(ZondaS3Operator, self).__init__(*args, **kwargs)
        self.operation = operation
        self.local_path = local_path
        self.remote_path = remote_path
        self.s3_bucket = s3_bucket
        self.source_s3_conn_id = source_s3_conn_id
        self.s3 = S3Hook(aws_conn_id=self.source_s3_conn_id, verify=False)
        self.skip_errors = kwargs.get("skip_errors", False)
        self.overwrite = kwargs.get("overwrite", True)
        self.kwargs = kwargs

    def execute(self, context):
        """
        Execute ZondaS3Operator Task
        :param context:
        :return:
        """
        result = []

        if self.operation.lower() in [S3Operation.PUT]:
            if self.operation.lower() == S3Operation.PUT:
                self.log.info('Processing File: ' + self.local_path)
                if not self.local_path:
                    self.local_path = []

                if not isinstance(self.local_path, (str, list)):
                    raise Exception(
                        'Invalid data type for local_path argument. Only string and list data types are supported')
                elif isinstance(self.local_path, str):
                    self.local_path = self.local_path.strip("'] ['").split("', '")

                for local_path in self.local_path:

                    flag_error = False
                    remote_path = self.remote_path

                    if remote_path:
                        if not re.match(REGEX_S3_FILE, remote_path):
                            raise Exception("Malformed remote path '{}'".format(remote_path))
                    else:
                        # prefix preparation
                        default_prefix = str(Path(local_path).parent)
                        prefix = self.kwargs.get('prefix')
                        replace_prefix = self.kwargs.get('replace_prefix')

                        if not prefix and not replace_prefix:
                            prefix = default_prefix
                            replace_prefix = default_prefix
                        elif not prefix and replace_prefix:
                            prefix = default_prefix
                        elif prefix and not replace_prefix:
                            replace_prefix = prefix

                        local_path = local_path.replace(r'\udcf3', '?')
                        # generate remote path
                        remote_path = str(Path(local_path.replace(prefix, replace_prefix)).parent)

                    if not re.match(REGEX_IS_FILE, remote_path):
                        # check partition by date option
                        if self.kwargs.get('partitioned_by_date'):
                            check = re.match(REGEX_DATE_FILE, local_path)
                            last_modification_date = datetime.fromtimestamp(os.path.getmtime(local_path))
                            if check:
                                last_modification_date = datetime.strptime(check[2], '%Y%m%d')

                            suffix = 'partition_date={}-{}-{}'.format(last_modification_date.year,
                                                                      str(last_modification_date.month).zfill(2),
                                                                      str(last_modification_date.day).zfill(2))

                            # miscorp fix
                            if remote_path.startswith('/santander/bi-corp/landing/control_gestion'):
                                remote_path = str(Path(remote_path).parent)

                            remote_path = os.path.join(remote_path, suffix)

                        filename = local_path.split('/')[-1]
                        remote_path = os.path.join(remote_path, filename)

                    if remote_path.startswith('/'):
                        remote_path = remote_path[1:]

                    self.log.info('Input: ' + local_path)
                    self.log.info('Bucket:' + self.s3_bucket)
                    self.log.info('Output: ' + remote_path)

                    self.s3.load_file(local_path, remote_path, bucket_name=self.s3_bucket, replace=self.overwrite)
                    # "s3://"

                    self.log.info("Upload successful")

                    if not flag_error:
                        result.append(os.path.join(remote_path, os.path.basename(local_path)))
        else:
            if not self.skip_errors:
                raise Exception('Invalid S3 operation or not supported ({})'.format(self.operation))

        return result
