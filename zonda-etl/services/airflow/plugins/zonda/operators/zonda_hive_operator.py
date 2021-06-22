#!/usr/bin/python3

import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from zonda.hooks.zonda_hive_cli_hook import ZondaHiveCliHook


class ZondaHiveOperator(BaseOperator):
    template_fields = ['connection_id', 'hql', 'output_file']

    @apply_defaults
    def __init__(self, connection_id, hql, output_file=None, empty_file=True, show_header='true', delimiter='', output_format='csv2', **kwargs):
        """
        Constructor
        """
        super(ZondaHiveOperator, self).__init__(**kwargs)
        self.connection_id = connection_id
        self.schema = 'default'
        self.vars = kwargs.get('vars', {})
        self.hql = hql
        self.output_file = output_file
        self.show_header = show_header
        self.output_format = output_format
        self.delimiter = delimiter
        self.empty_file = empty_file

    def execute(self, context):

        """
        Execute HiveOperator Task
        :param context:
        :return:
        """
        if not self.vars:
            dag_name = self.dag.parent_dag.dag_id if self.dag.parent_dag else self.dag.dag_id
            self.vars = context['ti'].xcom_pull(task_ids='InputConfig', dag_id=dag_name, key='all')

        print('Variables: {}'.format(self.vars))
        executable = self.transform_executables(self.hql, self.vars)

        hive = ZondaHiveCliHook(hive_cli_conn_id=self.connection_id)
        hive.run_cli(hql=executable, schema=self.schema, output_file=self.output_file, empty_file=self.empty_file, show_header=self.show_header, output_format=self.output_format, delimiter=self.delimiter)

    def transform_executables(self, hql, vars):
        """
        Transform queries and replace variables
        :param vars: set of variables to be replaced from XCOM [Dictionary]
        :return: queries transformed [List<String>]
        """
        tmp_script = hql
        query_variables = self.query_replace_variables(tmp_script, vars)
        return query_variables

    @staticmethod
    def query_replace_variables(query1, vars):
        varss = vars
        for k, v in varss.items():
            var_upper = k.upper()
            posFormats = ['${{{}}}'.format(var_upper), '${{{}}}'.format(k)]
            if any(x in query1 for x in posFormats):
                for x in posFormats:
                    query1 = query1.replace(x, v)
        query1 = os.path.expandvars(query1)
        return query1