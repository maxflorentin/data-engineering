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

from __future__ import print_function, unicode_literals

import contextlib
import os
import re
import subprocess
import time
import io
from collections import OrderedDict
from tempfile import NamedTemporaryFile

import six
import unicodecsv as csv
from past.builtins import basestring
from past.builtins import unicode
from six.moves import zip

import airflow.security.utils as utils
from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.file import TemporaryDirectory
from airflow.utils.helpers import as_flattened_list
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']

ZONDA_DIR = os.getenv('ZONDA_DIR')


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.
    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}


class ZondaHiveCliHook(BaseHook):
    """Simple wrapper around the hive CLI.

    It also supports the ``beeline``
    a lighter CLI that runs JDBC and is replacing the heavier
    traditional CLI. To enable ``beeline``, set the use_beeline param in the
    extra field of your connection as in ``{ "use_beeline": true }``

    Note that you can also set default hive CLI parameters using the
    ``hive_cli_params`` to be used in your connection as in
    ``{"hive_cli_params": "-hiveconf mapred.job.tracker=some.jobtracker:444"}``
    Parameters passed here can be overridden by run_cli's hive_conf param

    The extra connection parameter ``auth`` gets passed as in the ``jdbc``
    connection string as is.

    :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
    :type  mapred_queue: str
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :type  mapred_queue_priority: str
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :type  mapred_job_name: str
    """

    def __init__(
            self,
            hive_cli_conn_id="hive_cli_default",
            run_as=None,
            mapred_queue=None,
            mapred_queue_priority=None,
            mapred_job_name=None):
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params = conn.extra_dejson.get('hive_cli_params', '')
        self.use_beeline = conn.extra_dejson.get('use_beeline', False)
        self.auth = conn.extra_dejson.get('auth', 'nosasl')
        self.conn = conn
        self.run_as = run_as

        if mapred_queue_priority:
            mapred_queue_priority = mapred_queue_priority.upper()
            if mapred_queue_priority not in HIVE_QUEUE_PRIORITIES:
                raise AirflowException(
                    "Invalid Mapred Queue Priority.  Valid values are: "
                    "{}".format(', '.join(HIVE_QUEUE_PRIORITIES)))

        self.mapred_queue = mapred_queue or configuration.get('hive',
                                                              'default_hive_mapred_queue')
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name

    def _prepare_cli_cmd(self):
        """
        This function creates the command list from available information
        """
        conn = self.conn
        hive_bin = 'hive'
        cmd_extra = []


        if self.use_beeline:
            hive_bin = 'beeline'
            jdbc_url = "jdbc:hive2://{conn.host}:{conn.port}/{conn.schema}"
            if configuration.conf.get('core', 'security') == 'kerberos':
                template = conn.extra_dejson.get(
                    'principal', "hive/_HOST@EXAMPLE.COM")
                if "_HOST" in template:
                    template = utils.replace_hostname_pattern(
                        utils.get_components(template))

                proxy_user = ""  # noqa
                if conn.extra_dejson.get('proxy_user') == "login" and conn.login:
                    proxy_user = "hive.server2.proxy.user={0}".format(conn.login)
                elif conn.extra_dejson.get('proxy_user') == "owner" and self.run_as:
                    proxy_user = "hive.server2.proxy.user={0}".format(self.run_as)

                jdbc_url += ";principal={template};{proxy_user}"
            elif self.auth:
                jdbc_url += ";auth=" + self.auth

            jdbc_url = jdbc_url.format(**locals())
            jdbc_url = '"{}"'.format(jdbc_url)

            cmd_extra += ['-u', jdbc_url]
            if conn.login:
                cmd_extra += ['-n', conn.login]
            if conn.password:
                cmd_extra += ['-p', conn.password]
        hive_params_list = self.hive_cli_params.split()

        return [hive_bin] + cmd_extra + hive_params_list

    @staticmethod
    def _prepare_hiveconf(d):
        """
        This function prepares a list of hiveconf params
        from a dictionary of key value pairs.

        :param d:
        :type d: dict

        >>> hh = ZondaHiveCliHook()
        >>> hive_conf = {"hive.exec.dynamic.partition": "true",
        ... "hive.exec.dynamic.partition.mode": "nonstrict"}
        >>> hh._prepare_hiveconf(hive_conf)
        ["-hiveconf", "hive.exec.dynamic.partition=true",\
 "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]
        """
        if not d:
            return []
        return as_flattened_list(
            zip(["-hiveconf"] * len(d),
                ["{}={}".format(k, v) for k, v in d.items()])
        )

    def run_cli(self, hql, output_format, show_header, delimiter, schema=None, output_file=None, empty_file=True, verbose=True, hive_conf=None, shell=False):
        """
        Run an hql statement using the hive cli. If hive_conf is specified
        it should be a dict and the entries will be set as key/value pairs
        in HiveConf


        :param hive_conf: if specified these key value pairs will be passed
            to hive as ``-hiveconf "key"="value"``. Note that they will be
            passed after the ``hive_cli_params`` and thus will override
            whatever values are specified in the database.
        :type hive_conf: dict

        >>> hh = HiveCliHook()
        >>> result = hh.run_cli("USE airflow;")
        >>> ("OK" in result)
        True
        """
        conn = self.conn
        schema = schema or conn.schema
        """
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())
        """

        with TemporaryDirectory(prefix='airflow_hiveop_', dir=ZONDA_DIR+'/airflow_ops') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                # hql = hql + '\n'
                f.write(hql.encode('UTF-8'))
                f.flush()
                hive_cmd = self._prepare_cli_cmd()
                env_context = get_context_from_env_var()
                # Only extend the hive_conf if it is defined.
                if hive_conf:
                    env_context.update(hive_conf)
                hive_conf_params = self._prepare_hiveconf(env_context)
                if self.mapred_queue:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapreduce.job.queuename={}'
                             .format(self.mapred_queue),
                         '-hiveconf',
                         'mapred.job.queue.name={}'
                             .format(self.mapred_queue),
                         '-hiveconf',
                         'tez.job.queue.name={}'
                             .format(self.mapred_queue)
                         ])

                if self.mapred_queue_priority:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapreduce.job.priority={}'
                             .format(self.mapred_queue_priority)])

                if self.mapred_job_name:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapred.job.name={}'
                             .format(self.mapred_job_name)])

                hive_cmd.extend(hive_conf_params)
                hive_cmd.extend(['-e', hql])

                replace = ''

                if delimiter == '':
                    output_format = 'csv2'
                    replace = "| sed 's/,//g'"

                if output_file is not None:
                    hive_cmd.extend(['--silent', '--hiveconf hive.root.logger=ERROR', '--showHeader='+show_header, '--outputformat='+output_format, '--delimiterForDSV='+"'"+delimiter+"'", replace, '>', output_file])
                    shell = True
                    hql_exec = " ".join(hive_cmd)
                else:
                    hql_exec = hive_cmd

                if verbose:
                    self.log.info("%s", " ".join(hive_cmd))

                sp = subprocess.Popen(
                    hql_exec,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    shell=shell,
                    close_fds=True)

                self.sp = sp
                stdout = ''

                while True:
                    line = sp.stdout.readline()
                    if not line:
                        break
                    stdout += line.decode('UTF-8')
                    if verbose:
                        self.log.info(line.decode('UTF-8').strip())
                sp.wait()

                return_code = sp.returncode

                if return_code:
                    raise AirflowException(stdout)

                if output_file is not None and empty_file is False:
                    if os.path.getsize(output_file) == 0:
                        empty_file_cmd = "rm {}".format(output_file).split()
                        self.log.info('Executing command ' + ' '.join(empty_file_cmd))

                        p = subprocess.Popen(empty_file_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                        for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                            self.log.info(line.strip())

                        p.wait()
                        if p.returncode != 0:
                            raise Exception()

                return stdout

    def test_hql(self, hql):
        """
        Test an hql statement using the hive cli and EXPLAIN

        """
        create, insert, other = [], [], []
        for query in hql.split(';'):  # naive
            query_original = query
            query = query.lower().strip()

            if query.startswith('create table'):
                create.append(query_original)
            elif query.startswith(('set ',
                                   'add jar ',
                                   'create temporary function')):
                other.append(query_original)
            elif query.startswith('insert'):
                insert.append(query_original)
        other = ';'.join(other)
        for query_set in [create, insert]:
            for query in query_set:

                query_preview = ' '.join(query.split())[:50]
                self.log.info("Testing HQL [%s (...)]", query_preview)
                if query_set == insert:
                    query = other + '; explain ' + query
                else:
                    query = 'explain ' + query
                try:
                    self.run_cli(query, verbose=False)
                except AirflowException as e:
                    message = e.args[0].split('\n')[-2]
                    self.log.info(message)
                    error_loc = re.search(r'(\d+):(\d+)', message)
                    if error_loc and error_loc.group(1).isdigit():
                        lst = int(error_loc.group(1))
                        begin = max(lst - 2, 0)
                        end = min(lst + 3, len(query.split('\n')))
                        context = '\n'.join(query.split('\n')[begin:end])
                        self.log.info("Context :\n %s", context)
                else:
                    self.log.info("SUCCESS")

    def load_df(
            self,
            df,
            table,
            field_dict=None,
            delimiter=',',
            encoding='utf8',
            pandas_kwargs=None, **kwargs):
        """
        Loads a pandas DataFrame into hive.

        Hive data types will be inferred if not passed but column names will
        not be sanitized.

        :param df: DataFrame to load into a Hive table
        :type df: pandas.DataFrame
        :param table: target Hive table, use dot notation to target a
            specific database
        :type table: str
        :param field_dict: mapping from column name to hive data type.
            Note that it must be OrderedDict so as to keep columns' order.
        :type field_dict: collections.OrderedDict
        :param delimiter: field delimiter in the file
        :type delimiter: str
        :param encoding: str encoding to use when writing DataFrame to file
        :type encoding: str
        :param pandas_kwargs: passed to DataFrame.to_csv
        :type pandas_kwargs: dict
        :param kwargs: passed to self.load_file
        """

        def _infer_field_types_from_df(df):
            DTYPE_KIND_HIVE_TYPE = {
                'b': 'BOOLEAN',    # boolean
                'i': 'BIGINT',     # signed integer
                'u': 'BIGINT',     # unsigned integer
                'f': 'DOUBLE',     # floating-point
                'c': 'STRING',     # complex floating-point
                'M': 'TIMESTAMP',  # datetime
                'O': 'STRING',     # object
                'S': 'STRING',     # (byte-)string
                'U': 'STRING',     # Unicode
                'V': 'STRING'      # void
            }

            d = OrderedDict()
            for col, dtype in df.dtypes.iteritems():
                d[col] = DTYPE_KIND_HIVE_TYPE[dtype.kind]
            return d

        if pandas_kwargs is None:
            pandas_kwargs = {}

        with TemporaryDirectory(prefix='airflow_hiveop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, mode="w") as f:

                if field_dict is None:
                    field_dict = _infer_field_types_from_df(df)

                df.to_csv(path_or_buf=f,
                          sep=(delimiter.encode(encoding)
                               if six.PY2 and isinstance(delimiter, unicode)
                               else delimiter),
                          header=False,
                          index=False,
                          encoding=encoding,
                          date_format="%Y-%m-%d %H:%M:%S",
                          **pandas_kwargs)
                f.flush()

                return self.load_file(filepath=f.name,
                                      table=table,
                                      delimiter=delimiter,
                                      field_dict=field_dict,
                                      **kwargs)

    def load_file(
            self,
            filepath,
            table,
            delimiter=",",
            field_dict=None,
            create=True,
            overwrite=True,
            partition=None,
            recreate=False,
            tblproperties=None):
        """
        Loads a local file into Hive

        Note that the table generated in Hive uses ``STORED AS textfile``
        which isn't the most efficient serialization format. If a
        large amount of data is loaded and/or if the tables gets
        queried considerably, you may want to use this operator only to
        stage the data into a temporary table before loading it into its
        final destination using a ``HiveOperator``.

        :param filepath: local filepath of the file to load
        :type filepath: str
        :param table: target Hive table, use dot notation to target a
            specific database
        :type table: str
        :param delimiter: field delimiter in the file
        :type delimiter: str
        :param field_dict: A dictionary of the fields name in the file
            as keys and their Hive types as values.
            Note that it must be OrderedDict so as to keep columns' order.
        :type field_dict: collections.OrderedDict
        :param create: whether to create the table if it doesn't exist
        :type create: bool
        :param overwrite: whether to overwrite the data in table or partition
        :type overwrite: bool
        :param partition: target partition as a dict of partition columns
            and values
        :type partition: dict
        :param recreate: whether to drop and recreate the table at every
            execution
        :type recreate: bool
        :param tblproperties: TBLPROPERTIES of the hive table being created
        :type tblproperties: dict
        """
        hql = ''
        if recreate:
            hql += "DROP TABLE IF EXISTS {table};\n"
        if create or recreate:
            if field_dict is None:
                raise ValueError("Must provide a field dict when creating a table")
            fields = ",\n    ".join(
                [k + ' ' + v for k, v in field_dict.items()])
            hql += "CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            if partition:
                pfields = ",\n    ".join(
                    [p + " STRING" for p in partition])
                hql += "PARTITIONED BY ({pfields})\n"
            hql += "ROW FORMAT DELIMITED\n"
            hql += "FIELDS TERMINATED BY '{delimiter}'\n"
            hql += "STORED AS textfile\n"
            if tblproperties is not None:
                tprops = ", ".join(
                    ["'{0}'='{1}'".format(k, v) for k, v in tblproperties.items()])
                hql += "TBLPROPERTIES({tprops})\n"
        hql += ";"
        hql = hql.format(**locals())
        self.log.info(hql)
        self.run_cli(hql)
        hql = "LOAD DATA LOCAL INPATH '{filepath}' "
        if overwrite:
            hql += "OVERWRITE "
        hql += "INTO TABLE {table} "
        if partition:
            pvals = ", ".join(
                ["{0}='{1}'".format(k, v) for k, v in partition.items()])
            hql += "PARTITION ({pvals})"

        # As a workaround for HIVE-10541, add a newline character
        # at the end of hql (AIRFLOW-2412).
        hql += ';\n'

        hql = hql.format(**locals())
        self.log.info(hql)
        self.run_cli(hql)

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                print("Killing the Hive job")
                self.sp.terminate()
                time.sleep(60)
                self.sp.kill()

