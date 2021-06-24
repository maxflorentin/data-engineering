# coding=utf-8


import requests
import zipfile
import json
import io, os
import sys
import pandas as pd
import subprocess
import datetime
import boto3
from botocore.client import Config

QUALTRICS_API_TOKEN = os.getenv('QUALTRICS_API_TOKEN')
ZONDA_DIR = os.getenv('ZONDA_DIR')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_S3 = os.getenv('AWS_BUCKET_S3')


def read_config(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data

def create_s3_connection():
    config = Config(connect_timeout=120, retries={'max_attempts': 0})
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        config=config
    )
    return s3


def download_nps_files(dayofrun):
    # Setting user Parameters

    try:
        apiToken = QUALTRICS_API_TOKEN
    except KeyError:
        print("set environment variable APIKEY")
        sys.exit(2)

    dataCenter = 'santanderargentina.ca1'

    # Setting static parameters
    requestCheckProgress = 0.0
    progressStatus = "inProgress"

    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken,
    }
    proxies = {
            "http": "http://proxy.ar.bsch:8080",
            "https": "http://proxy.ar.bsch:8080",
        }

    surveyids = read_config(ZONDA_DIR+"/repositories/zonda-etl/scripts/api/nps/nps_surveyIds.json")
    print("Config File Read")
    print(headers)

    for surveyName, surveyData in surveyids.items():

        if surveyName is 'CANALES_IND' or surveyName is 'CANALES_EMP':
            date_from = datetime.datetime.strptime(dayofrun, '%Y-%m-%d')
            date_from = date_from - datetime.timedelta(days=1)
            date_from = datetime.datetime.strftime(date_from, '%Y-%m-%d')
        else:
            date_from = dayofrun

        print(surveyName, date_from, dayofrun)

        # Step 1: Creating Data Export para
        data = {
            "format": "csv",
            "startDate": "{}T00:00:00-03:00".format(date_from),
            "endDate": "{}T23:59:59-03:00".format(date_from),
            "newlineReplacement": " ",
            "surveyMetadataIds": ["startDate", "endDate", "status", "ipAddress", "progress", "duration", "finished",
                                  "recordedDate", "_recordId", "recipientLastName", "recipientFirstName",
                                  "recipientEmail", "externalDataReference", "locationLatitude", "locationLongitude",
                                  "distributionChannel", "userLanguage"],
            "questionIds": surveyData.get('questions'),
            "embeddedDataIds": surveyData.get('columns')
        }

        if surveyData.get('filterId') is not None:
            data["filterId"] = surveyData.get('filterId')

        print(data)

        url = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter, surveyData.get('surveyId'))
        print("Start Download of {}".format(surveyName))

        downloadRequestResponse = requests.request("POST", url, json=data, headers=headers, proxies=proxies)
        print(downloadRequestResponse.json())

        try:
            progressId = downloadRequestResponse.json()["result"]["progressId"]
        except KeyError:
            print(downloadRequestResponse.json())
            sys.exit(2)

        isFile = None

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while progressStatus != "complete" and progressStatus != "failed" and isFile is None:
            if isFile is None:
               print("file not ready")
            else:
               print("progressStatus=", progressStatus)
            requestCheckUrl = url + progressId
            requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers, proxies=proxies)
            try:
               isFile = requestCheckResponse.json()["result"]["fileId"]
            except KeyError:
               1==1
            print(requestCheckResponse.json())
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            print("Download is " + str(requestCheckProgress) + " complete")
            progressStatus = requestCheckResponse.json()["result"]["status"]

        #step 2.1: Check for error
        if progressStatus is "failed":
            raise Exception("export failed")

        fileId = requestCheckResponse.json()["result"]["fileId"]
        print(fileId)

        # Step 3: Downloading file
        requestDownloadUrl = url + fileId + '/file'
        requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True, proxies=proxies)

        #Step 4: Get File Name in Zip File
        file_name = zipfile.ZipFile(io.BytesIO(requestDownload.content)).infolist()[0].filename.replace('.csv', '')
        file_name = file_name.replace('/', '_')
        print(file_name)

        # Step 5: Unzipping the file
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall(ZONDA_DIR+"/tmp/nps")
        print('{} Downloaded With Success'.format(file_name))

        gen_landing_file(file_name, surveyName, dayofrun, surveyData.get('npsGroups'))
        progressStatus = ""


def gen_landing_file(file, surveyName, dayofrun, npsGroups_list):
    #File Generated in DF
    df = pd.read_csv(ZONDA_DIR + '/tmp/nps/{}.csv'.format(file), skiprows=[2], header=0)

    #Get Question Columns and Generate DF
    df_questions = df.filter(regex=r'Q[0-9]$|Q[0-9][0-9]$|Q[0-9]_[0-9]$|Q[0-9][0-9]_[0-9]$|Q[0-9]_[0-9][0-9]$', axis=1)
    df_questions = df_questions[:1]
    df_questions = df_questions.transpose()
    df_questions.reset_index(inplace=True)
    df_questions.rename(columns={'index': 'question_id', '0': 'question_desc'}, inplace=True)

    #List of Question Columns and add ResponseId column to join
    l_questions = list(df.filter(regex=r'Q[0-9]$|Q[0-9][0-9]$|Q[0-9]_[0-9]$|Q[0-9][0-9]_[0-9]$|Q[0-9]_[0-9][0-9]$', axis=1).columns)
    l_questions.append('ResponseId')

    df1 = df[l_questions]
    df1 = df1[1:]

    #Delete importId
    df2 = df[1:]

    if not df2.empty:
        #Transpose DF
        # Toma la expresion regular y lo pasa de fila a columna df1 = {'ResponseId': xx, 'variable': 'Qn', 'Respuesta': valor_de_Qn}
        df1 = pd.melt(df1, id_vars=['ResponseId'], value_vars=list(df1.filter(regex=r'Q[0-9]$|Q[0-9][0-9]$|Q[0-9]_[0-9]$|Q[0-9][0-9]_[0-9]$|Q[0-9]_[0-9][0-9]$', axis=1).columns))

        #Merge DFs by Question Columns
        #Agrego a df1 {'question_id': Qn, 'question_desc': 'pregunta_de_Qn'} mergeando por variable y questian id que es Qn
        #df3 queda con todas las preguntas y respuestas en filas, ejemplo:
        #{'ResponseId': R_20OHeagiA1t, 'variable': 'Q1', 'Respuesta': 10, 'question_id': Q1, 'question_desc': '¿Que tan probable...?'}
        df3 = df1.merge(df_questions, how='outer', left_on='variable', right_on='question_id')

        #Generate Final DF with all columns
        df_final = df3.merge(df2, how='outer', left_on='ResponseId', right_on='ResponseId')

        #Create inbound path if not exists
        if os.path.isdir(ZONDA_DIR + '/inbound/nps/fact/{}'.format(surveyName.lower())) is False:
            tmp = 'mkdir ' + ZONDA_DIR + '/inbound/nps/fact/{}'.format(surveyName.lower())
            tmp = tmp.split()
            p = subprocess.Popen(tmp, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                print(line.strip())

            p.wait()
            if p.returncode != 0:
                raise Exception()

        #Se eliminan todas las columnas NPS_GROUP excepto las especificadas en el archivo de configuracion
        l_nps_group_columns = list(df_final.filter(regex=r'Q[0-9]_NPS_GROUP$|Q[0-9][0-9]_NPS_GROUP$|Q[0-9]_[0-9]_NPS_GROUP$|Q[0-9][0-9]_[0-9]_NPS_GROUP$|Q[0-9]_[0-9][0-9]_NPS_GROUP$', axis=1).columns)
        if npsGroups_list is not None:
            l_nps_group_columns = [x for x in l_nps_group_columns if (x not in npsGroups_list)]
        df_final.drop(columns=l_nps_group_columns, axis=1, inplace=True)

        l_questions.remove('ResponseId')
        df_final.drop(columns=l_questions, axis=1, inplace=True)
        df_final.rename(columns={0: 'question_desc'}, inplace=True)
        df_final.rename(columns={'value': 'respuesta'}, inplace=True)

        #Write Inbound File
        filename = ZONDA_DIR + '/inbound/nps/fact/{}/{}_{}.csv'.format(surveyName.lower(), surveyName.lower(),
                                                                        dayofrun.replace('-', ''))
        if not ZONDA_DIR.__contains__('airflow'):
            df_final.to_csv(filename, sep='^', index=False)
        else:
            df_final.to_csv(filename, sep='^', index=False)
            s3_connection = create_s3_connection()
            s3_connection.Bucket(AWS_BUCKET_S3).upload_file(Filename=filename, Key='data/in/{}_{}.csv'.format(surveyName.lower(),
                                                                       dayofrun.replace('-', '')))
            rm = 'rm {}'.format(filename)
            rm = rm.split()
            p = subprocess.Popen(rm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                print(line.strip())

    else:
        print("Empty DF")
        with open(ZONDA_DIR + '/inbound/nps/fact/{}/{}_{}.csv'.format(surveyName.lower(), surveyName.lower(),
                                                                       dayofrun.replace('-', '')), "w"):
            pass  #


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--dayofrun', metavar='dayofrun', required=True,
                        help='dayofrun')

    args = parser.parse_args()

    download_nps_files(dayofrun=args.dayofrun)