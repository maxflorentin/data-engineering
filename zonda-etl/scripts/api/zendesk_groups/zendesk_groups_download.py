# coding=utf-8

import http.client
import re
import requests
import json
import os
import pandas as pd
import argparse
import datetime
import time

ZONDA_DIR = os.getenv('ZONDA_DIR')

def read_config(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data

def limpiar(var: object) -> object:
    #val = re.sub('[^A-Za-z0-9- &áéíóúÁÉÍÓÚ/\.,]+?¡!$%()*¿_ñÑ@#', ' ', var)
    val = re.sub('\n', ' ', var)
    val1= re.sub('\r', ' ', val)
    return val1

def limpiar2(var: object) -> object:
    #val = re.sub('[^A-Za-z0-9- &áéíóúÁÉÍÓÚ/\.,]+?¡!$%()*¿_ñÑ@#', ' ', var)
    val = re.sub('channel', '==channel==', var)
    val1 = re.sub('source', '==source==', val)
    return val1


def ifnull(var, val):
    if var.strip(" ") is None or var.strip(" ") == "" or var.strip(" ")==" ":
        return val
    return var.strip(" ")

def procesamiento(val, json_item, partition_date_filter):

    tot = val
    cote = len(tot)
    print(str(len(tot)))

    try:
        for i in range(0,cote):

            ids = str(tot[i].get('id'))
            urls = str(tot[i].get('url'))
            names = str(tot[i].get('name'))
            descriptions = str(tot[i].get('description'))
            defaults = str(tot[i].get('default')) #bool
            deleteds = str(tot[i].get('deleted')) #bool
            created_ats = str(tot[i].get('created_at'))
            updated_ats = str(tot[i].get('updated_at'))


            linea = str(ids)+"^"+ifnull(urls,"0")+"^"+ifnull(str(names),"0")+"^"+ifnull(str(descriptions),"0")+"^"+ifnull(str(defaults),"0")+"^"+ifnull(str(deleteds),"0")+"^"+ifnull(str(created_ats),"0")+"^"+ifnull(str(updated_ats),"0")
            archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_gr/dim/{}/zendesk_gr_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
            archivo_sal.write(linea+'\n')
            archivo_sal.close()
    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_tk_files(partition_date_filter, partition_date_unix):


    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_groups/zendesk_groups_config.json")
    print("Config File Read")

    for zendesk_tk_name, zendesk_tk_data in config.items():

        payload = {}

        proxies = {
            "http": "http://proxy.ar.bsch:8080",
            "https": "http://proxy.ar.bsch:8080",
        }

        if zendesk_tk_data.get('item') == 'santander-ar':
            print("COMIENZA A PROCESAR SANTANDER-AR")
            headers = {
                'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOmkzMmhGN1YzdHBTeXJ6T2kyY1diTFM1MDM5UUFuSnU3Qjg0VGRtWmQ='
            }
        else:
            if zendesk_tk_data.get('item') == 'comex-santander':
                print("COMIENZA A PROCESAR COMEX-SANTANDER")
                headers = {
                    'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOlhGTnc2RkdwM3l5U1hWWDloUkxFV09lTE1mVGdQdFZzZFpyRnFYTmw='
                }
            else:
                print("COMIENZA A PROCESAR PUC-SANTANDER")
                headers = {
                    'Authorization': 'Basic amJyYWdhenppQHNhbnRhbmRlcnRlY25vbG9naWEuY29tLmFyL3Rva2VuOnVJMlJKV0JGZmh5U2lsNE5mcm00VXlsNFdnMkMxUXUwVFNkSmY3UFU='
                }

        conn = zendesk_tk_data.get('conn')
        print("conn: {}".format(conn))

        base_url = zendesk_tk_data.get('url')
        print("base_url: {}".format(base_url))

        v_start_time = partition_date_unix
        print("v_start_time: {}".format(v_start_time))

        end_executing = False

        while  end_executing!='None':

            print("1- download_request_response")
            download_request_response = requests.request("GET", base_url, headers=headers, data=payload, proxies=proxies)
            rdata = download_request_response.json()

            base_url = str(rdata["next_page"])

            procesamiento(rdata["groups"], zendesk_tk_data.get('item'), partition_date_filter)

            end_executing = str(rdata["next_page"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create dates schema')

    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True,
                        help='partition_date_filter')

    parser.add_argument('--partition_date_unix', metavar='partition_date_unix', required=True,
                        help='partition_date_unix')

    args = parser.parse_args()

    download_zendesk_tk_files(partition_date_filter=args.partition_date_filter, partition_date_unix=args.partition_date_unix)
