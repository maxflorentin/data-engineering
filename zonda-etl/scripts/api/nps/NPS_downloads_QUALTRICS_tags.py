# coding=utf-8


import requests
import zipfile
import json
import glob
import os
import sys
import shutil
import pandas as pd
import datetime
import io
import subprocess
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

    surveyids = read_config(ZONDA_DIR+"/repositories/zonda-etl/scripts/api/nps/nps_qualtrics_tags.json")
    print("Config File Read")

    # Delete temporary files before processing
    delete_tmp_files()

    for surveyName, surveyData in surveyids.items():

        date_from = datetime.datetime.strptime(dayofrun, '%Y-%m-%d')
        date_from = date_from - datetime.timedelta(days=6)
        date_from = datetime.datetime.strftime(date_from, '%Y-%m-%d')

        date_from = "2020-06-01" # reproceso
        print(date_from, dayofrun)

        # Step 1: Creating Data Export
        data = {
            "format": "csv",
            "startDate": "{}T00:00:00-03:00".format(date_from),
            "endDate": "{}T23:59:59-03:00".format(dayofrun),
            "newlineReplacement": " ",
            "surveyMetadataIds": [],
            "questionIds": [],
            "embeddedDataIds": surveyData.get('columns')
        }

        if surveyData.get('filterId') is not None:
            data["filterId"] = surveyData.get('filterId')

        print(data)

        url = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter,
                                                                                      surveyData.get('surveyId'))
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
                1 == 1
            print(requestCheckResponse.json())
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            print("Download is " + str(requestCheckProgress) + " complete")
            progressStatus = requestCheckResponse.json()["result"]["status"]

        # step 2.1: Check for error
        if progressStatus is "failed":
            raise Exception("export failed")

        fileId = requestCheckResponse.json()["result"]["fileId"]
        print(fileId)

        # Step 3: Downloading file
        requestDownloadUrl = url + fileId + '/file'
        requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True, proxies=proxies)

        # Step 4: Get File Name in Zip File
        file_name = zipfile.ZipFile(io.BytesIO(requestDownload.content)).infolist()[0].filename.replace('.csv', '')
        print(file_name)

        # Step 5: Unzipping the file
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall(ZONDA_DIR + "/tmp/nps")

        print('{} Downloaded With Success'.format(file_name))

        # Step 6: Copy files before concatenating
        gen_copy_file(file_name, surveyName, dayofrun)

        progressStatus = ""

    # Step 4: concatenates the generated files into one
    gen_final_landing_file(dayofrun)


def gen_copy_file(file, surveyName, dayofrun):
    # File Generated in DF
    df = pd.read_csv(ZONDA_DIR + '/tmp/nps/{}.csv'.format(file), skiprows=[1, 2], header=0)

    if not df.empty:
        df.to_csv(ZONDA_DIR + '/tmp/nps/tags/{}_{}.csv'.format(surveyName.lower(), dayofrun.replace('-', '')),
                  sep=';', index=False, header=["MOMENTO_CRITICO", "ID_RESPUESTA", "PARENT_TOPICS", "TOPICS", "TOPIC_SENTIMENT_SCORE", "TOPIC_SENTIMENT_LABEL", "SENTIMENT", "SENTIMENT_SCORE", "SENTIMENT_POLARITY"])
    else:
        print("Empty DF")


def gen_final_landing_file(dayofrun):
    # Generated one file with all de files in the directory
    path = ZONDA_DIR + '/tmp/nps/tags'
    all_files = glob.glob(path + "/*" + dayofrun.replace('-', '') + ".csv")

    li = []

    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0, delimiter=";")
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True, keys=None)

    # Write Inbound File
    filename = ZONDA_DIR + '/inbound/nps/fact/tags/nps_tags_{}.csv'.format(dayofrun.replace('-', ''))
    frame.to_csv(filename, sep=";", index=False)

    if ZONDA_DIR.__contains__('airflow'):
        s3_connection = create_s3_connection()
        s3_connection.Bucket(AWS_BUCKET_S3).upload_file(Filename=filename,
                                                        Key='data/in/nps_tags_{}.csv'.format(dayofrun.replace('-', '')))
        rm = 'rm {}'.format(filename)
        rm = rm.split()
        p = subprocess.Popen(rm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
            print(line.strip())


def delete_tmp_files():
    folder = ZONDA_DIR + '/tmp/nps/tags'
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--dayofrun', metavar='dayofrun', required=True,
                        help='dayofrun')

    args = parser.parse_args()

    download_nps_files(dayofrun=args.dayofrun)
