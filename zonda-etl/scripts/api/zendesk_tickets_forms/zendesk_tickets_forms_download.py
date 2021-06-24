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
            raw_names = str(tot[i].get('raw_name'))
            display_names = str(tot[i].get('display_name')) #bool
            raw_display_names = str(tot[i].get('raw_display_name')) #bool
            end_user_visibles = str(tot[i].get('end_user_visible'))
            positions = str(tot[i].get('position'))
            ticket_field_idss = str(tot[i].get('ticket_field_ids'))
            actives = str(tot[i].get('active'))
            defaults = str(tot[i].get('default'))
            created_atss = str(tot[i].get('created_at'))
            updated_atss = str(tot[i].get('updated_at'))
            in_all_brandss = str(tot[i].get('in_all_brands'))
            restricted_brand_idss = str(tot[i].get('restricted_brand_ids'))
            end_user_conditionss = str(tot[i].get('end_user_conditions'))
            agent_conditionss = str(tot[i].get('agent_conditions'))




            linea = str(ids)+"^"+ifnull(urls,"0")+"^"+ifnull(str(names),"0")+"^"+ifnull(str(raw_names),"0")+"^"+ifnull(str(display_names),"0")+"^"+ifnull(str(raw_display_names),"0")+"^"+ifnull(str(end_user_visibles),"0")+"^"+ifnull(str(positions),"0")+"^"+ifnull(str(ticket_field_idss),"0")+"^"+ifnull(str(actives),"0")+"^"+ifnull(str(defaults),"0")+"^"+ifnull(str(created_atss),"0")+"^"+ifnull(str(updated_atss),"0")+"^"+ifnull(str(in_all_brandss),"0")+"^"+ifnull(str(restricted_brand_idss),"0")+"^"+ifnull(str(end_user_conditionss),"0")+"^"+ifnull(str(agent_conditionss),"0")

            archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_tk_forms/dim/{}/zendesk_tk_forms_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
            archivo_sal.write(linea+'\n')
            archivo_sal.close()
    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_tk_files(partition_date_filter, partition_date_unix):


    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_tickets_forms/zendesk_tickets_forms_config.json")
    print("Config File Read")

    for zendesk_tk_name, zendesk_tk_data in config.items():

        #base_url_santander = "https://santander-ar.zendesk.com/api/v2/incremental/tickets.json"
        #base_url_comex = "https://comex-santander.zendesk.com/api/v2/incremental/tickets.json"
        #base_url_puc = "https://puc-santander.zendesk.com/api/v2/incremental/tickets.json"

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
                    'Authorization': 'Basic IGpicmFnYXp6aUBzYW50YW5kZXJ0ZWNub2xvZ2lhLmNvbS5hci90b2tlbjp1STJSSldCRmZoeVNpbDROZnJtNFV5bDRXZzJDMVF1MFRTZEpmN1BV'
                }

        conn = zendesk_tk_data.get('conn')
        print("conn: {}".format(conn))

        base_url = zendesk_tk_data.get('url')
        print("base_url: {}".format(base_url))

        v_start_time = partition_date_unix
        print("v_start_time: {}".format(v_start_time))

        end_executing = False

        #columns = zendesk_tk_data.get('col')
        #print("Columns selected: {}".format(columns))
        #df = pd.DataFrame(columns=columns)

        while  end_executing!='None':
            # https://santander-ar.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://comex-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://puc-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237

            #url = base_url+'?start_time='+str(v_start_time)
            #print("Start Download: {}".format(url))

            print("1- download_request_response")
            download_request_response = requests.request("GET", base_url, headers=headers, data=payload, proxies=proxies)
            rdata = download_request_response.json()

            print("2- Info")
            df_total = pd.DataFrame(rdata)
            df_total.info()

            print("3- va a hacer el append")
            #df1 = pd.DataFrame(rdata["ticket_forms"], columns=columns)

            procesamiento(rdata["ticket_forms"], zendesk_tk_data.get('item'), partition_date_filter)

            #v_start_time = rdata["end_time"]
            #print("Next start_time: {}".format(v_start_time))

            end_executing = str(rdata["next_page"])
            #print("End executing: {}".format(end_executing))

            #time.sleep(30) #espera en segundos
            #print("delay: 30 seg")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create dates schema')

    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True,
                        help='partition_date_filter')

    parser.add_argument('--partition_date_unix', metavar='partition_date_unix', required=True,
                        help='partition_date_unix')

    args = parser.parse_args()

    download_zendesk_tk_files(partition_date_filter=args.partition_date_filter, partition_date_unix=args.partition_date_unix)
