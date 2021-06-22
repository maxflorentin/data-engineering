#!/usr/bin/python3

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

os.environ['https_proxy'] = 'https://proxy.ar.bsch:8080'


class ZondaGCSOperator(BaseOperator):
    template_fields = ['connection_id', 'bucket', 'prefix', 'destination', 'sql']

    @apply_defaults
    def __init__(self, connection_id, sql, *args, **kwargs):
        super(ZondaGCSOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.bucket = kwargs.get('bucket')
        self.prefix = kwargs.get('prefix')
        self.destination = kwargs.get('destination')
        self.sql = sql

    def execute(self, context):
        dag_run = context.get('dag_run')

        try:
            conf = dag_run.conf or {}
        except AttributeError as _:
            conf = {}
            pass

        self.prefix = conf.get('prefix') or self.prefix
        self.bucket = conf.get('bucket') or self.bucket
        self.destination = conf.get('destination') or self.destination
        self.sql = conf.get('sql') or self.sql

        dag_name = self.dag.parent_dag.dag_id if self.dag.parent_dag else self.dag.dag_id
        dict = context['ti'].xcom_pull(task_ids='InputConfig', dag_id=dag_name, key='all')

        final_prefix = self.replace_variables(self.prefix, dict)

        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.connection_id)
        os.environ[
            'GOOGLE_APPLICATION_CREDENTIALS'] = hook.extras.get('extra__google_cloud_platform__key_path')

        #Create Temp Table in BigQuery
        self.create_table_bq(final_prefix, context)

        #Export Table To Google Cloud Storage
        self.bq_to_gcs(final_prefix, context)

        #Delete Temp Table
        self.delete_table_bq(final_prefix, context)

        if self.destination[-1:] == "/":
            pass
        else:
            self.destination = self.destination+"/"

        #List All Objects in Bucket With Selected Prefix
        fileList = hook.list(bucket=self.bucket, prefix=final_prefix)

        #Generate and Download File
        hook.compose(bucket=self.bucket, source_objects=fileList, destination_object=final_prefix+".gz")
        hook.download(bucket=self.bucket, object=final_prefix+".gz", filename=self.destination+final_prefix+".gz")

        #Delete Compose File
        hook.delete(bucket=self.bucket, object=final_prefix+".gz")

        #Delete All Files
        for file in fileList:
            hook.delete(bucket=self.bucket, object=file)

    @staticmethod
    def replace_variables(prefix, vars):
        prefix = prefix+"_${partition_date_filter}"
        final_prefix = ""
        varss = vars
        for k, v in varss.items():
            var_upper = k.upper()
            posFormats = ['${{{}}}'.format(var_upper), '${{{}}}'.format(k)]
            if any(x in prefix for x in posFormats):
                for x in posFormats:
                    final_prefix = prefix.replace(x, v)
        return final_prefix

    def create_table_bq(self, final_prefix, context):
        bq_recent_questions_query = BigQueryOperator(bigquery_conn_id=self.connection_id, sql=self.sql,
                                                     destination_dataset_table="137275638.{}".format(
                                                         final_prefix), use_legacy_sql=False,
                                                     allow_large_results=True, task_id='generate_table')
        bq_recent_questions_query.execute(context)

    def bq_to_gcs(self, final_prefix, context):
        bqToGCS = BigQueryToCloudStorageOperator(bigquery_conn_id=self.connection_id,
                                                 destination_cloud_storage_uris='gs://'+self.bucket+'/'+final_prefix+'-*',
                                                 compression='GZIP', export_format='CSV', field_delimiter="|", print_header=False,
                                                 source_project_dataset_table="137275638.{}".format(
                                                     final_prefix),
                                                 task_id='bqToGCS')
        bqToGCS.execute(context)

    def delete_table_bq(self, final_prefix, context):
        delete = BigQueryOperator(bigquery_conn_id=self.connection_id, sql="DROP TABLE `137275638.{}`".format(final_prefix),
                                                     use_legacy_sql=False, task_id='delete_table')
        delete.execute(context)