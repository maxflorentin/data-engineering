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
            titles = str(tot[i].get('title'))
            raw_titles = str(tot[i].get('raw_title'))
            descriptions = str(tot[i].get('description')) #bool
            raw_descriptions = str(tot[i].get('raw_description')) #bool
            positions = str(tot[i].get('position'))
            actives = str(tot[i].get('active'))
            requireds = str(tot[i].get('required'))
            collapsed_for_agentss = str(tot[i].get('collapsed_for_agents'))
            regexp_for_validations = str(tot[i].get('regexp_for_validation'))
            title_in_portals = str(tot[i].get('title_in_portal'))
            raw_title_in_portals = str(tot[i].get('raw_title_in_portal'))
            visible_in_portals = str(tot[i].get('visible_in_portal'))
            editable_in_portals = str(tot[i].get('editable_in_portal'))
            required_in_portals = str(tot[i].get('required_in_portal'))
            tags = str(tot[i].get('tag'))
            created_ats = str(tot[i].get('created_at'))
            updated_ats = str(tot[i].get('updated_at'))
            removables = str(tot[i].get('removable'))
            agent_descriptions = str(tot[i].get('agent_description'))


            linea = str(ids)+"^"+ifnull(urls,"0")+"^"+ifnull(str(titles),"0")+"^"+ifnull(str(raw_titles),"0")+"^"+ifnull(str(descriptions),"0")+"^"+ifnull(str(raw_descriptions),"0")+"^"+ifnull(str(positions),"0")+"^"+ifnull(str(actives),"0")+"^"+ifnull(str(requireds),"0")+"^"+ifnull(str(collapsed_for_agentss),"0")+"^"+ifnull(str(regexp_for_validations),"0")+"^"+ifnull(str(title_in_portals),"0")+"^"+ifnull(str(raw_title_in_portals),"0")+"^"+ifnull(str(visible_in_portals),"0")+"^"+ifnull(str(editable_in_portals),"0")+"^"+ifnull(str(required_in_portals),"0")+"^"+ifnull(str(tags),"0")+"^"+ifnull(str(created_ats),"0")+"^"+ifnull(str(updated_ats),"0")+"^"+ifnull(str(removables),"0")+"^"+ifnull(str(agent_descriptions),"0")

            archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_tk_fields/dim/{}/zendesk_tk_fields_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
            archivo_sal.write(linea+'\n')
            archivo_sal.close()
    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_tk_files(partition_date_filter, partition_date_unix):


    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_tickets_fields/zendesk_tickets_fields_config.json")
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
            #df1 = pd.DataFrame(rdata["ticket_fields"], columns=columns)

            procesamiento(rdata["ticket_fields"], zendesk_tk_data.get('item'), partition_date_filter)

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
