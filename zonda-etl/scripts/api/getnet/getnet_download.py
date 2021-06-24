# coding=utf-8

import requests
import json
import os
import pandas as pd
import argparse
import io
import subprocess
import boto3
from botocore.client import Config


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

def download_getnet_files(partition_date_filter, next_partition_date_filter):

    # base_url = "https://geo-bi-api-getnet-jobs.apps.ocpdmz.ar.bsch/"
    base_url = "https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/"

    headers = {
        "content-type": "application/json"
    }

    config = read_config(ZONDA_DIR+"/repositories/zonda-etl/scripts/api/getnet/getnet_config.json")
    print("Config File Read")

    for getnet_name, getnet_data in config.items():

        v_page = 0
        continue_executing = True

        columns = getnet_data.get('col')
        print("Columns selected: {}".format(columns))
        df = pd.DataFrame(columns=columns)

        while continue_executing:

            #https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/merchants?page=0&resourcesPerPage=100&search=createdAt<20201105
            # https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/merchants?resourcesPerPage=100&page=0&search=createdAt:20201103
            # url_merchants    = "https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/merchants?resourcesPerPage=100000&search=createdAt>20200101,createdAt<20200819"
            # url_transactions = "https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/transactions?resourcesPerPage=50000&search=transactionStatus:APPROVED,transactionDate>20200101,transactionDate<20200819"
            # url_dongles      = "https://geo-bi-api-getnet-geo-bi-api.apps.ocpdaz1.ar.bsch/dongles?search=createdAt:20201104&resourcesPerPage=100"


            if getnet_data.get('item') == 'merchants':
               date_filter = next_partition_date_filter
            else:
                date_filter = partition_date_filter

            url = base_url+getnet_data.get('item')+'?page='+str(v_page)+'&'+getnet_data.get('filter')+date_filter
            print("Start Download: {}".format(url))

            v_page = v_page + 1

            download_request_response = requests.get(url, headers=headers, verify=False)
            rdata = download_request_response.json()

            #columns = getnet_data.get('col')
            #print("Columns selected: {}".format(columns))

            df_total = pd.DataFrame(rdata)
            df_total.info()

            df1 = pd.DataFrame(rdata["contents"], columns=columns)

            if getnet_data.get('item') == 'merchants':
                df1['zendesk_id'] = df1['zendesk_id'].astype(str)
                df1['zendesk_id'] = df1['zendesk_id'].str.replace('nan', '')
                df1['zendesk_id'] = df1['zendesk_id'].str.replace("\.0", '')

            if getnet_data.get('item') == 'transactions':
                df1['response'] = df1['response'].str.replace(',', '')
                df1['response'] = df1['response'].str.replace('\n', ' ')

            df = df.append(df1)

            continue_executing = rdata["has_next"]

            print("has_next: {}".format(continue_executing))

        inbound_path = ZONDA_DIR+"/inbound/getnet/fact/{}/".format(getnet_data.get('item'))
        file_name = "getnet_{}_{}.csv".format(getnet_data.get('item'), partition_date_filter)

        file_full_path = inbound_path + file_name

        df.to_csv(file_full_path, index=False, header=True)
        print("File: {} successfully generated".format(file_name))

        if ZONDA_DIR.__contains__('airflow'):
            s3_connection = create_s3_connection()
            s3_connection.Bucket(AWS_BUCKET_S3).upload_file(Filename=file_full_path, Key='data/in/getnet_{}_{}.csv'.
                                                            format(getnet_data.get('item'), partition_date_filter))
            rm = 'rm {}'.format(file_full_path)
            rm = rm.split()
            p = subprocess.Popen(rm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                print(line.strip())


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create dates schema')

    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True, help='partition_date_filter')
    parser.add_argument('--next_partition_date_filter', metavar='next_partition_date_filter', required=True, help='next_partition_date_filter')

    args = parser.parse_args()

    download_getnet_files(partition_date_filter=args.partition_date_filter, next_partition_date_filter=args.next_partition_date_filter)