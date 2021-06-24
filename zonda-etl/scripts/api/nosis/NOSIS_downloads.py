import os

import requests
import json
import pandas as pd
import uuid
import pyarrow.parquet as pq
from pyzonda.hadoop import HDFS
import subprocess
import shutil
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

ZONDA_DIR = os.getenv('ZONDA_DIR')

#Variable generales
base_url = "https://nosis-api-nosis.apps.ocpprd.ar.bsch/variables"
inbound_path = ZONDA_DIR + '/inbound/nosis/fact/api_report/'
local_path = '/tmp/'

# Variables de Autenticacion
token = "afcaf2d8-67df-4d19-961e-d8a58b07e09d"
ibm_client_id = "radar-lake"
correlationid = str(uuid.uuid1())


# Input: document(integer),name(string),gender(string),costCentre(string),personType(string),filters(list)
# Output: JSON con el reporte de Nosis (ver documentacion)
def request_report(document, name, gender, costCentre, personType, filters):
    header = {"Authorization": token, "X-IBM-Client-Id": ibm_client_id, "x-san-correlationid": correlationid}
    body = {"document": document, "name": name, "gender": gender, "costCentre": costCentre, "personType": personType,
            "filters": filters}
    request = requests.post(base_url, headers=header, json=body, verify=False)
    response = json.loads(request.text)
    #print(response)

    return response


# Input: JSON de Nosis (ver documentacion)
# Output: Dataframe formateado
def format_report(reporte):
    # Creacion de data frame, seleccion de columnas, transponer y elminar sobrantes
    cabecera = {"document": reporte["document"], "enrollNumber": reporte["enrollNumber"], "name": reporte["name"],
                "gender": reporte["gender"],
                "updateDateTime": reporte["updateDateTime"], "observations": reporte["observations"]}

    cabecera = pd.DataFrame.from_dict(cabecera, 'index')
    cabecera = cabecera.transpose()
    cabecera['ID'] = 0

    variables = pd.DataFrame(reporte["variables"])
    variables = variables[["name", "value"]]
    variables = variables.transpose()
    variables.columns = variables.iloc[0]
    variables = variables.iloc[1:]
    variables['ID'] = 0

    listado_campos = ["VI_HistoriaLaboral_Empleadores", "AS_Beneficios_Detalle", "VR_Relaciones_Detalle",
                      "CI_24m_Detalle", "AP_12m_EmpleadoDomestico_Detalle",
                      "CI_Vig_Detalle", "CI_Vig_Detalle_PorEntidad", "VI_Tel_Detalle"]

    for campo in listado_campos:
        try:
            variables[[campo]] = variables[[campo]].replace('<Detalle>','', regex = True).replace('<D>','', regex = True).replace('</D>','|', regex = True).replace('</Detalle>','', regex = True).replace('<Detalle />','', regex = True)
        except:
            pass

    reporte = pd.merge(cabecera, variables, on='ID', how='inner')

    return reporte


def get_file_from_hdfs(dayofrun, local_path):
    partition_date = dayofrun
    hdfs_path = '/santander/bi-corp/sandbox/rda/staging/reporte_nosis/clientes/partition_date=' + partition_date + '/'

    path_parquet = HDFS.ls(hdfs_path)
    file_name = path_parquet[0]["path"]
    file_name = file_name.split("/")
    file_name = file_name[9]
    hdfs_path = hdfs_path + file_name
    local_path = local_path + file_name
    cmd = subprocess.call('hdfs dfs -get ' + hdfs_path + ' ' + local_path, shell=True)
    return local_path


def nosis_download(dayofrun):

    partition_date = dayofrun
    partition_date_file = partition_date[:4]+partition_date[5:7]+partition_date[8:10]

    file_path = get_file_from_hdfs(dayofrun, local_path)
    table = pq.read_table(file_path)
    df = table.to_pandas()

    df = df[['numero_documento', 'razon_social', 'descripcion_segmento', 'periodo']].dropna(
        subset=['descripcion_segmento'])

    for index, row in df.iterrows():
        document = str(row['numero_documento'])
        name = str(row['razon_social'])
        gender = 'male'
        costCentre = "000-999"
        filter = []
        if row['descripcion_segmento'] == 'INDIVIDUOS':
            personType = "individual"
            try:
                report = request_report(document,name,gender,costCentre,personType,filter)
                formatted_report = format_report(report)
                formatted_report.to_csv(local_path+'nosis_api_individuos_'+partition_date_file+'.csv', index=False, mode="a",  encoding='latin1', header=False)
            except:
                print('##### Error en la siguiente fila:')
                print(report)
                error = {"document": document,
                         "name": name,
                         "gender": gender,
                         "costCentre": costCentre,
                         "filter": filter,
                         "personType": personType,
                         "api_response": report
                         }

                error = pd.DataFrame.from_dict(error, 'index')
                error = error.transpose()
                error.to_csv(local_path + 'nosis_api_errores_' + partition_date_file + '.csv', index=False, mode="a",encoding='latin1', header=False)
                pass
        else:
            try:
                personType = "corporation"
                report = request_report(document,name,gender,costCentre,personType,filter)
                formatted_report = format_report(report)
                formatted_report.to_csv(local_path+'nosis_api_empresas_'+partition_date_file+'.csv', index=False, mode="a",  encoding='latin1', header=False)
            except:
                print('##### Error en la siguiente fila:')
                print(report)
                error = {"document": document,
                         "name": name,
                         "gender": gender,
                         "costCentre": costCentre,
                         "filter": filter,
                         "personType": personType,
                         "api_response": report
                         }

                error = pd.DataFrame.from_dict(error, 'index')
                error = error.transpose()
                error.to_csv(local_path + 'nosis_api_errores_' + partition_date_file + '.csv', index=False, mode="a",encoding='latin1', header=False)
                pass

if __name__ == "__main__":

    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('--dayofrun', metavar='dayofrun', required=True,help='dayofrun')
    args = parser.parse_args()

    nosis_download(dayofrun=args.dayofrun)

    partition_date = args.dayofrun
    partition_date_file = partition_date[:4]+partition_date[5:7]+partition_date[8:10]

    try:
        shutil.move(local_path + 'nosis_api_individuos_' + partition_date_file + '.csv',inbound_path +'nosis_api_individuos_' + partition_date_file + '.csv')
        print("Individuos OK")
    except:
        print("No se pudo mover archivo nosis_api_individuos_")
        pass

    try:
        shutil.move(local_path + 'nosis_api_empresas_' + partition_date_file + '.csv',inbound_path +'nosis_api_empresas_' + partition_date_file + '.csv')
        print("Empresas OK")
    except:
        print("No se pudo mover archivo nosis_api_empresas_")
        pass

    try:
        shutil.move(local_path + 'nosis_api_errores_' + partition_date_file + '.csv',inbound_path +'nosis_api_errores_' + partition_date_file + '.csv')
        print("Errores OK")
    except:
        print("No se pudo mover archivo nosis_api_errores_")
        pass