# coding=utf-8

import http.client
import re
import requests
import json
import os
import pandas as pd
import argparse
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
    val = re.sub('event_type', '==event_type==', var)
    val1 = re.sub('status', '==status==', val)

    return val1

def ifnull(var, val):
    if var.strip(" ") is None or var.strip(" ") == "" or var.strip(" ")==" ":
        return val
    return var.strip(" ")
def procesamiento(val, json_item, partition_date_filter, partition_date):
    tot = val
    cote = len(tot)
    print("long")
    print(str(len(tot)))
    try:
        for i in range(0,cote):

            ids = str(tot[i].get('id'))
            child_eventss = limpiar(limpiar2(str(tot[i].get('child_events'))))
            ticket_ids = str(tot[i].get('ticket_id'))
            timestamps = str(tot[i].get('timestamp'))
            created_ats = str(tot[i].get('created_at'))
            updater_ids = str(tot[i].get('updater_id'))
            vias = str(tot[i].get('via'))
            systems = str(tot[i].get('system'))
            event_types = str(tot[i].get('event_type'))
            updated_frm = created_ats[0:10]

            if updated_frm >= partition_date:
                linea = str(ids)+"^"+ifnull(child_eventss,"0")+"^"+ifnull(str(ticket_ids),"0")+"^"+ifnull(str(timestamps),"0")+"^"+ifnull(str(created_ats),"0")+"^"+ifnull(str(updater_ids),"0")+"^"+ifnull(str(vias),"0")+"^"+ifnull(str(systems),"0")+"^"+ifnull(str(event_types),"0")
                archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_tk_events/fact/{}/zendesk_tk_events_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
                archivo_sal.write(linea+'\n')
                archivo_sal.close()


    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_tk_files(partition_date_filter, partition_date_unix, partition_date):


    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_tickets_events/zendesk_tickets_events_bis_config.json")
    print("Config File Read")

    for zendesk_tk_events_name, zendesk_tk_events_data in config.items():

        #base_url_santander = "https://santander-ar.zendesk.com/api/v2/incremental/tickets.json"
        #base_url_comex = "https://comex-santander.zendesk.com/api/v2/incremental/tickets.json"
        #base_url_puc = "https://puc-santander.zendesk.com/api/v2/incremental/tickets.json"

        payload = {}

        proxies = {
            "http": "http://proxy.ar.bsch:8080",
            "https": "http://proxy.ar.bsch:8080",
        }

        if zendesk_tk_events_data.get('item') == 'santander-ar':
            print("COMIENZA A PROCESAR SANTANDER-AR")
            headers = {
                'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOmkzMmhGN1YzdHBTeXJ6T2kyY1diTFM1MDM5UUFuSnU3Qjg0VGRtWmQ='
            }
        else:
            if zendesk_tk_events_data.get('item') == 'comex-santander':
                print("COMIENZA A PROCESAR COMEX-SANTANDER")
                headers = {
                    'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOlhGTnc2RkdwM3l5U1hWWDloUkxFV09lTE1mVGdQdFZzZFpyRnFYTmw='
                }
            else:
                print("COMIENZA A PROCESAR PUC-SANTANDER")
                headers = {
                    'Authorization': 'Basic amJyYWdhenppQHNhbnRhbmRlcnRlY25vbG9naWEuY29tLmFyL3Rva2VuOnVJMlJKV0JGZmh5U2lsNE5mcm00VXlsNFdnMkMxUXUwVFNkSmY3UFU='
                }

        conn = zendesk_tk_events_data.get('conn')
        print("conn: {}".format(conn))

        base_url = zendesk_tk_events_data.get('url')
        print("base_url: {}".format(base_url))

        v_start_time = round(float(partition_date_unix))
        print("v_start_time: {}".format(v_start_time))

        v_start_time_mas_uno = round(float(partition_date_unix)) + 86400
        print("v_start_time mas uni float: {}".format(v_start_time_mas_uno))


        end_executing = False

        columns = zendesk_tk_events_data.get('col')
        print("Columns selected: {}".format(columns))

        while not end_executing:
            # https://santander-ar.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://comex-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://puc-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237

            url = base_url+'?start_time='+str(v_start_time)
            print("Start Download: {}".format(url))

            print("1- download_request_response")
            download_request_response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
            rdata = download_request_response.json()

            print("2- Info")

            if "deleted_ticket_forms" in rdata:
                del rdata["deleted_ticket_forms"]

            print(rdata["ticket_events"])

            procesamiento(rdata["ticket_events"], zendesk_tk_events_data.get('item'), partition_date_filter, partition_date)

            v_start_time = rdata["end_time"]
            print("Next start_time: {}".format(v_start_time))

            if v_start_time > v_start_time_mas_uno:
                end_executing = True
                print("****corta por fecha : {}".format(v_start_time))
            time.sleep(30) #espera en segundos
            print("delay: 30 seg")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create dates schema')

    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True,
                        help='partition_date_filter')

    parser.add_argument('--partition_date_unix', metavar='partition_date_unix', required=True,
                        help='partition_date_unix')

    parser.add_argument('--partition_date', metavar='partition_date', required=True,
                        help='partition_date')

    args = parser.parse_args()

    download_zendesk_tk_files(partition_date_filter=args.partition_date_filter, partition_date_unix=args.partition_date_unix, partition_date=args.partition_date)
