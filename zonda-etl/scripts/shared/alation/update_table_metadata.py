#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import os
import requests
import datetime
import urllib.parse
import urllib3
import argparse
from slack import WebClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ALATION_CLIENT_TOKEN = os.getenv('ALATION_CLIENT_TOKEN')
ALATION_HOST = os.getenv('ALATION_HOST', 'alation.ar.bsch')
ZONDA_DIR = os.getenv('ZONDA_DIR')
DEFAULT_DATASOURCE = "Data Lake - HIVE"

class TableMetadataWriter(object):
    def __init__(self, json_config):
        self.config_path = json_config
        self.config = self.read_config(self.config_path)
        # mandatory properties validation
        if not self.config.get('datasource', DEFAULT_DATASOURCE):
            raise Exception('Missing datasource property in configuration file {}'.format(self.config_path))

        # get datasource id
        self.ds_id = self._get_datasource_id(self.config.get('datasource', DEFAULT_DATASOURCE))
        if self.ds_id < 0:
            raise Exception('Datasource ID not found for datasource name {}'.format(self.config.get('datasource')))

    def _build_metadata(self):
        governance = self.config['objectMetadata']['governance']
        table = self.config['objectMetadata']['table']
        schedule = self.config['objectMetadata']['schedule']

        rows = []
        table_row = {
            'key': '{}.{}.{}'.format(str(self.ds_id), table.get('schema'), table.get('name')),
            'description': table.get('description'),
            'source application': table.get('source').upper(),
            'additional information': "<table><tbody><tr><td>TIPO DE TABLA</td><td>{}</td></tr>"
                                      "<tr><td>TIPO CARGA</td><td>{}</td></tr>"
                                      "<tr><td>ESQUEMA PROCESAMIENTO</td><td>{}</td></tr>"
                                      "</tbody></table>".format(table.get('type').upper(),
                                                                schedule['loading'].get('type').upper(),
                                                                schedule['loading'].get('delta').upper()),
            'periodicity': schedule.get('periodicity'),
            'cdo level governance': governance.get('level')
        }
        if table.get('title'):
            table_row['title'] = table.get('title')

        # stewards
        if governance.get('steward'):
            stewards = governance.get('steward')
            if isinstance(stewards, str):
                stewards = [stewards]

            table_row['it steward'] = [{'type': 'user', 'key': x} for x in stewards]

        # verified by
        if governance.get('validator'):
            stewards = governance.get('validator')
            if isinstance(stewards, str):
                stewards = [stewards]

            table_row['validator'] = [{'type': 'user', 'key': x} for x in stewards]

        if governance.get('businessSteward'):
            stewards = governance.get('businessSteward')
            if isinstance(stewards, str):
                stewards = [stewards]

            table_row['business steward'] = [{'type': 'user', 'key': x} for x in stewards]

        if governance.get('owner'):
            table_row['owner']=str(governance.get('owner'))
        else:
            table_row['owner']='NaN'

        if governance.get('dataQuality'):
            table_row['Data Quality'] = governance.get('dataQuality')

        if governance.get('documentation'):
            table_row['documentation'] = governance.get('documentation')
        if governance.get('businessReference'):
            table_row['business reference'] = governance.get('businessReference')
        if governance.get('verified'):
            table_row['cdo verified'] = governance.get('verified')

        rows.append(table_row)

        for col in table.get('columns'):
            column_row = {
                'key': '{}.{}.{}.{}'.format(str(self.ds_id), table.get('schema'), table.get('name'),
                                            col.get('name', 'undefined').strip()),
                'title': col.get('title'),
                'description': col.get('description'),
                'personal data': 'Yes' if col.get('personIdentifier', False) is True else 'No',
                'security taxonomy': "Público" if col.get('security', 'Interno') == 'Publico' else col.get('security',
                                                                                                           'Interno'),
                'additional information': "<table><tbody><tr><td>NULLABLE</td><td>{}</td></tr><tr><td>SEPARADOR DECIMAL</td><td>{}</td></tr></tbody></table>".format(
                    col.get('nullable'), col.get('decimalSeparator')),
                'length': "{}".format(col.get('length')),
            }

            if(col.get('governance')):
                governance_col = col.get('governance')
                column_row['Semantic classification']= governance_col.get('semanticClassification') if governance_col.get('semanticClassification')else 'NaN'
                column_row['Metric ID']= governance_col.get('metricID') if governance_col.get('metricID') else 'NaN'
                column_row['business reference'] = governance_col.get('businessReference') if governance_col.get('businessReference') else governance.get('businessReference')
                column_row['cdo verified'] = governance_col.get('verified') if governance_col.get('verified') else governance.get('verified')
                column_row['owner'] = str(governance_col.get('owner')) if governance_col.get('owner') else table_row['owner']
                if governance_col.get('level'):
                    column_row['CDO Level Governance'] = governance_col.get('level')
                if governance_col.get('ownerNegocio'):
                    column_row['Owner Negocio'] = governance_col.get('ownerNegocio')
                if governance_col.get('locallyAplicable'):
                    column_row['Locally Applicable'] = governance_col.get('locallyAplicable')
                if governance_col.get('businessSteward'):
                    column_row['business steward'] = [{'type': 'user', 'key': x} for x in governance_col.get('businessSteward')]
                if (governance_col.get('expert')):
                    column_row['expert'] = [{'type': 'user', 'key': x} for x in governance_col.get('expert')]
                if governance_col.get('steward'):
                    stewards = governance_col.get('steward')
                    if isinstance(stewards, str):
                        stewards = [stewards]
                    column_row['it steward'] = [{'type': 'user', 'key': x} for x in stewards]

            else:
                try:
                    if(governance.get('owner')):
                        column_row['owner'] = [{'type': 'user', 'key': x} for x in governance.get('owner')]
                    column_row['business steward'] = table_row['business steward']
                    column_row['business reference'] = table_row['business reference']
                    column_row['cdo verified'] = table_row['cdo verified']
                except:
                    pass
            rows.append(column_row)
        return rows

    def update_table_metadata(self, template='default', replace_values=True):
        table = self.config['objectMetadata']['table']

        print('Updating metadata for table {}.{}...'.format(table.get('schema'), table.get('name')))
        rows = self._build_metadata()
        params = {'replace_values': replace_values}

        # update table metadata
        endpoint = '/api/v1/bulk_metadata/custom_fields/{}/mixed'.format(template)
        data = '\n'.join([json.dumps(x) for x in rows])
        print(data)
        r = requests.post(self._get_url(endpoint), data=data, params=params, headers=self._get_client_headers(), verify=False)
        r.raise_for_status()
        json_text = json.loads(r.text)
        print("Received Objects -> {}".format(json_text.get('number_received') + 1))
        print("Updated Objects  -> {}".format(json_text.get('updated_objects') + 1))

        if json_text.get('error_objects'):
            objects = []
            print('ERROR -> ' + json_text.get('error'))
            for k in json_text.get('error_objects'):
                objects.append(k)
                print('- ' + k)
            self.send_slack_alert(objects, table.get('schema')+'.'+table.get('name'))

    def update_table_status(self):
        table = self.config['objectMetadata']['table']

        print('Updating status for table {}.{}...'.format(table.get('schema'), table.get('name')))

        if table.get('status'):
            flag_type = 'ENDORSEMENT' if table['status'].get('active') is True else 'DEPRECATION'
            flag_reason = table['status'].get('reason')
        else:
            flag_type = 'WARNING'
            flag_reason = 'Tabla sin validar'

        body = {
            "flag_type": flag_type,
            "flag_reason": flag_reason,
            "subject": {
                "id": self._get_table_id(table.get('name')),
                "otype": "table"
            }
        }

        if flag_type is 'ENDORSEMENT':
            del body['flag_reason']

        data = json.dumps(body, indent=4)

        endpoint = '/integration/flag/'
        r = requests.post(self._get_url(endpoint), data=data, headers=self._get_client_headers(), verify=False)
        flag = json.loads(r.text)
        print(flag)

    def update_table_lineage(self):
        lineage = self.config['objectMetadata']['table']

        print("Updating Lineage for Table {}.{}...".format(lineage.get('schema'), lineage.get('name')))

        rows = []

        content = "NA"
        if lineage.get('query'):
            content = open(lineage.get('query').replace('$ZONDA_DIR', ZONDA_DIR), 'r').read()

        dataflow = {
            "dataflow_objects": [
                {
                    "external_id": "api/{}".format(lineage.get('schema')+'.'+lineage.get('name')),
                    "content": content
                }
            ]
        }

        source_tables = []
        source_columns = []

        for column in lineage.get('columns'):

            for sourceTable in column.get('sourceColumns', []):
                table = "{}.{}.{}".format(str(self.ds_id), sourceTable.get('schema'), sourceTable.get('table'))
                source_tables.append(table)

            column_lineage = [[{"otype": "column", "key": "{}.{}.{}.{}".format(str(self.ds_id), x.get("schema"),
                                                                               x.get("table"), x.get("column").strip())}
                               for x in column.get('sourceColumns', [])],
                              [{"otype": "dataflow", "key": "api/{}".format(lineage.get('schema')+'.'+lineage.get('name'))}],
                              [{"otype": "column", "key": "{}.{}.{}.{}".format(self.ds_id, lineage.get('schema'),
                                                                        lineage.get('name'), column.get('name'))}]]

            source_columns.append(column_lineage)

        source_tables = list(set(source_tables))

        paths = [[[{"otype": "table", "key": table} for table in source_tables],
                 [{"otype": "dataflow", "key": "api/{}".format(lineage.get('schema')+'.'+lineage.get('name'))}],
                 [{"otype": "table", "key": "{}.{}.{}".format(self.ds_id, lineage.get('schema'), lineage.get('name'))}]]]

        for columnLineage in source_columns:
            paths.append(columnLineage)

        dataflow['paths'] = paths

        rows.append(dataflow)

        data = '\n'.join(json.dumps(body) for body in rows)

        endpoint_delete = '/integration/v2/dataflow/'

        param_delete = "keyField=external_id"

        ext_ids=["api/{}".format(lineage.get('schema')+'.'+lineage.get('name'))]

        r=requests.delete(self._get_url(endpoint_delete), json=ext_ids, params=param_delete,
                        headers=self._get_client_headers(), verify=False)
        job_id=r.json()['job_id']
        status = 'running'
        # Check the status of the job
        print('Job_id',job_id,' is running')
        while status == 'running':
            response = requests.get('https://' + ALATION_HOST + '/api/v1/bulk_metadata/job/?id=' + str(job_id),
                                                        headers=self._get_client_headers(), verify=False)
            job_details = json.loads(response.text)
            status = job_details['status']

        endpoint = '/integration/v2/lineage/'
        r = requests.post(self._get_url(endpoint), data=data, headers=self._get_client_headers(), verify=False)
        r.raise_for_status()
        response_text = json.loads(r.text)

        print(response_text)

        status = 'running'
        # Check the status of the job
        while status == 'running':
            response = requests.get('https://' + ALATION_HOST + '/api/v1/bulk_metadata/job/?id=' + str(response_text['job_id']),
                                                        headers=self._get_client_headers(), verify=False)
            job_details = json.loads(response.text)
            status = job_details['status']

        print("Updated table lineage")

    @staticmethod
    def _get_client_headers(**kwargs):
        headers = {'Token': ALATION_CLIENT_TOKEN, 'Content-Type': 'application/json'}
        headers.update(kwargs)
        return headers

    @staticmethod
    def _get_url(endpoint):
        base_url = 'https://' + ALATION_HOST
        return urllib.parse.urljoin(base_url, endpoint)

    def _get_datasource_id(self, ds_name):
        endpoint = '/catalog/datasource'
        params = {'title': ds_name}
        r = requests.get(self._get_url(endpoint), params=params, headers=self._get_client_headers(), verify=False)
        r.raise_for_status()
        json_text = json.loads(r.text)
        return -1 if not len(json_text) else int(json.loads(r.text)[0].get('id'))

    def _get_table_id(self, table):
        endpoint = '/catalog/table/'
        param = {'name': table}
        r = requests.get(self._get_url(endpoint), params=param, headers=self._get_client_headers(), verify=False)
        r.raise_for_status()
        json_text = json.loads(r.text)
        return -1 if not len(json_text) else int(json.loads(r.text)[0].get('id'))

    @staticmethod
    def read_config(json_config):
        with open(json_config, 'r') as f:
            data = json.load(f)
        return data

    def delete_lineage(self, lineage, source_tables):
        delete_endpoint = '/integration/v2/lineage/'

        for col in lineage.get('columns'):

            for sColumn in col.get('sourceColumns', []):

                param = "source_otype=column&source_key={}.{}.{}.{}&target_otype=dataflow&target_key=api/{}.{}".\
                    format(str(self.ds_id), sColumn.get('schema'), sColumn.get('table'), sColumn.get('column'),
                           lineage.get('schema'), lineage.get('name'))

                delete_source_column = requests.delete(self._get_url(delete_endpoint), params=param,
                                         headers=self._get_client_headers(), verify=False)
                job_id = json.loads(delete_source_column.content)['job_id']
                print("Job Id Delete Source Column: {}".format(job_id))

            param = "source_otype=dataflow&source_key=api/{}.{}&target_otype=column&target_key={}.{}.{}.{}". \
                format(lineage.get('schema'), lineage.get('name'), str(self.ds_id), lineage.get('schema'),
                       lineage.get('name'), col.get('name'))

            delete_target_column = requests.delete(self._get_url(delete_endpoint), params=param,
                                                   headers=self._get_client_headers(), verify=False)
            job_id = json.loads(delete_target_column.content)['job_id']
            print("Job Id Delete Target Column: {}".format(job_id))

        for table in source_tables:

            param = "source_otype=table&source_key={}&target_otype=dataflow&target_key=api/{}.{}".\
                    format(table, lineage.get('schema'), lineage.get('name'))
            delete_source_table = requests.delete(self._get_url(delete_endpoint), params=param,
                                                   headers=self._get_client_headers(), verify=False)
            job_id = json.loads(delete_source_table.content)['job_id']
            print("Job Id Delete Source Table: {}".format(job_id))

        param = "source_otype=dataflow&source_key=api/{}.{}&target_otype=table&target_key={}.{}.{}". \
            format(lineage.get('schema'), lineage.get('name'), str(self.ds_id), lineage.get('schema'),
                   lineage.get('name'))
        delete_target_table = requests.delete(self._get_url(delete_endpoint), params=param,
                                              headers=self._get_client_headers(), verify=False)
        job_id = json.loads(delete_target_table.content)['job_id']
        print("Job Id Delete Target Table: {}".format(job_id))

    def delete_dataflows(self):
        endpoint = '/integration/v2/dataflow/'
        param_get = 'keyField=id&limit=100&skip={}'
        param_delete = "keyField=external_id"

        i = 0

        resp_get = requests.get(self._get_url(endpoint), params=param_get.format(i),
                                   headers=self._get_client_headers(), verify=False)
        resp_json = resp_get.json()

        ext_ids = []

        while (resp_json['dataflow_objects']):
            print(i)
            aux = [dtf['external_id'] for dtf in resp_json['dataflow_objects']
                   if dtf['external_id'].startswith('sql/4')]
            ext_ids.extend(aux)
            i += 100
            resp_get = requests.get(self._get_url(endpoint), params=param_get.format(i),
                                   headers=self._get_client_headers(), verify=False)
            resp_json = resp_get.json()
        print("Deleting automatic Dataflows...")
        delete_target_dataflows = requests.delete(self._get_url(endpoint), json=ext_ids, params=param_delete,
                                                  headers=self._get_client_headers(), verify=False)

        if(ext_ids):
            job_id = json.loads(delete_target_dataflows.content)['job_id']
            print("Job Id {}: Delete {} Dataflows".format(job_id, len(ext_ids)))
        else:
            print("There are 0 dataflows to delete")



    @staticmethod
    def send_slack_alert(error, table):

        SLACK_API_TOKEN = os.getenv('SLACK_API_TOKEN')
        proxy = 'http://proxy.ar.bsch:8080'

        client = WebClient(token=SLACK_API_TOKEN, ssl=False, proxy=proxy)

        text = '<!here|here> :hand: Error Updating Alation Metadata!                             '
        color_notification = '#FFCC00'
        callback_id = 'warning_notification'
        fallback = text.replace('*', '').replace('<!here|here>', '').strip()

        fields = [{
                'title': table,
                'value': "```"+"\n".join(msg for msg in error)+"```",
                'short': False
            }]

        attachments = [
            {
                'text': text,
                'author_name': 'Zonda Alation Alerts',
                'fallback': fallback,
                'callback_id': callback_id,
                'color': color_notification,
                'attachment_type': 'default',
                'fields': fields,
                'footer': str(datetime.datetime.now()),
            }
        ]

        result = client.chat_postMessage(
            as_user='false',
            username='Zonda',
            channel='zonda-alation-alerts',
            attachments=attachments
        )

        if not result.get('ok'):
            print("Error notifying to channel zonda-alerts: {}".format(result.get('error')))

def update_table_metadata(schema, **kwargs):
    alation_schemas = schema.split(',')
    for alation_schema in alation_schemas:
        print("Processing File: {}".format(alation_schema))
        writer = TableMetadataWriter(alation_schema)
        columns = writer.config['objectMetadata']['table']['columns']
        lineage = False

        for column in columns:
            if column.get('sourceColumns', []):
                lineage = True

        if writer.config['objectMetadata'].get('active', False):
            writer.update_table_metadata()

        if lineage is True:
            writer.update_table_lineage()
    #writer.delete_dataflows()

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", help='Schema file with metadata to be uploaded')
    args = parser.parse_args()

    # validate arguments
    if not args.schema:
        raise Exception("Missing mandatory parameter: --schema")

    update_table_metadata(**vars(args))
