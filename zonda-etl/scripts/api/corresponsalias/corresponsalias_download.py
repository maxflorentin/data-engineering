# coding=utf-8

import requests
import json
import os
import io
import pandas as pd
import argparse
import subprocess
import boto3
from botocore.client import Config


ZONDA_DIR = os.getenv('ZONDA_DIR')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_S3 = os.getenv('AWS_BUCKET_S3')


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


def download_corresp_files(partition_date,partition_date_filter):

    # base_url = "https://backoffice-webapi-ponsa-bo.apps.ocpprd.ar.bsch/api/Reports/nps"
    base_url = "https://backoffice-webapi-ponsa-bo.apps.ocpprd.ar.bsch/api/Reports/nps?"

    headers = {
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJodHRwOi8vc2NoZW1hcy54bWxzb2FwLm9yZy93cy8yMDA1LzA1L2lkZW50aXR5L2NsYWltcy9uYW1laWRlbnRpZmllciI6Ik5QUyIsImh0dHA6Ly9zY2hlbWFzLnhtbHNvYXAub3JnL3dzLzIwMDUvMDUvaWRlbnRpdHkvY2xhaW1zL25hbWUiOiJOUFMiLCJpc3MiOiJjb3JyZXNwb25zYWxpYXMuc2FudGFuZGVyLmNvbS5hciIsImF1ZCI6ImNvcnJlc3BvbnNhbGlhcy5zYW50YW5kZXIuY29tLmFyIn0.V_lG0MnsonVeeQHoO28lF1DvcnKWbtA9T48dbhDPiJM"
    }

    # url_corresponsalias = "https://backoffice-webapi-ponsa-bo.apps.ocpprd.ar.bsch/api/Reports/nps?Date=2020-09-28&Page=1"

    #url = base_url+'Date='+partition_date+'&Page=1'
    url=base_url+'Date='+partition_date+'&Page=1'
    print("Start Download: {}".format(url))

    download_request_response = requests.get(url, headers=headers, verify=False)
    rdata = download_request_response.json()
    df = pd.DataFrame(rdata["data"],columns=['correspondent', 'serviceName', 'accountNumber', 'nup', 'operationDate', 'creditCardNumber', 'amount', 'subsidiaryId', 'subsidiaryName'])
    print("printing df data: {}".format(df))

    inbound_path = ZONDA_DIR+"/inbound/corresponsalias/fact/corresponsalias/"
    file_name = 'corresponsalias_'+partition_date_filter+'.csv'

    file_full_path = inbound_path+file_name

    df.to_csv(file_full_path, index=False, header=True)
    print("file: {} successfully generated".format(file_name))

    if ZONDA_DIR.__contains__('airflow'):
        s3_connection = create_s3_connection()
        s3_connection.Bucket(AWS_BUCKET_S3).upload_file(Filename=file_full_path, Key='data/in/corresponsalias_{}.csv'
                                                        .format(partition_date_filter))
        rm = 'rm {}'.format(file_full_path)
        rm = rm.split()
        p = subprocess.Popen(rm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
            print(line.strip())


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--partition_date', metavar='partition_date', required=True, help='partition_date')
    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True, help='partition_date_filter')

    args = parser.parse_args()

    download_corresp_files(partition_date=args.partition_date, partition_date_filter=args.partition_date_filter)