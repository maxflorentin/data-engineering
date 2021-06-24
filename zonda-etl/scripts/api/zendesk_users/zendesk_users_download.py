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

def remover (valor):
    try:
        remo=os.remove(valor)
    except:
        remo=0
    return remo

def limpiar(var: object) -> object:
    #val = re.sub('[^A-Za-z0-9- &áéíóúÁÉÍÓÚ/\.,]+?¡!$%()*¿_ñÑ@#', ' ', var)
    val = re.sub('\n', ' ', var)
    return val

def limpiar2(var: object) -> object:
    #val = re.sub('[^A-Za-z0-9- &áéíóúÁÉÍÓÚ/\.,]+?¡!$%()*¿_ñÑ@#', ' ', var)
    val = re.sub('legajo', '==legajo==', var)
    val1 = re.sub('rol', '==rol==', val)
    val2 = re.sub('sucursal', '==sucursal==', val1)
    val3 = re.sub('nivel2', '==nivel2==', val2)
    val4 = re.sub('nivel3', '==nivel3==', val3)
    val5 = re.sub('nivel4', '==nivel4==', val4)
    val6 = re.sub('prioridad', '==prioridad==', val5)
    val7 = re.sub('cliente', '%%cliente%%', val6)
    val8 = re.sub('ig', '%%ig%%', val7)
    return val8


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
            urls = str(tot[i].get('url'))
            names = str(tot[i].get('name'))
            emails = str(tot[i].get('email'))
            created_ats = str(tot[i].get('created_at'))
            updated_ats = str(tot[i].get('updated_at'))
            updated_frm = updated_ats[0:10]
            time_zones = str(tot[i].get('time_zone'))
            iana_time_zones = str(tot[i].get('iana_time_zone'))
            phones = str(tot[i].get('phone'))
            shared_phone_numbers = tot[i].get('shared_phone_number') #bool
            photos = tot[i].get('photo')
            locale_ids = str(tot[i].get('locale_id'))
            locales = limpiar(tot[i].get('locale'))
            organization_ids = str(tot[i].get('organization_id'))
            roles = str(tot[i].get('role'))
            verifieds = tot[i].get('verified') #bool
            external_ids = str(tot[i].get('external_id'))
            tagss = tot[i].get('tags')
            aliass = str(tot[i].get('alias'))
            actives = tot[i].get('active') #bool
            shareds = tot[i].get('shared') #bool
            shared_agents = tot[i].get('shared_agent') #bool
            last_login_ats = tot[i].get('last_login_at')
            two_factor_auth_enableds = tot[i].get('two_factor_auth_enabled') #bool
            signatures = limpiar(str(tot[i].get('signature')))
            detailss = limpiar(str(tot[i].get('details')))
            notess = limpiar(str(tot[i].get('notes')))
            role_types = str(tot[i].get('role_type'))
            custom_role_ids = str(tot[i].get('custom_role_id'))
            moderators = tot[i].get('moderator') #bool
            ticket_restrictions = limpiar(str(tot[i].get('ticket_restriction')))
            only_private_commends = tot[i].get('only_private_comments') #bool
            restricted_agents = tot[i].get('restricted_agent')
            suspends = tot[i].get('suspended') #bool
            chat_onlys = tot[i].get('chat_only') #bool
            default_group_ids = str(tot[i].get('default_group_id'))
            report_csvs = tot[i].get('report_csv') #bool
            user_fieldss = limpiar2(str(tot[i].get('user_fields')))
            #print(updated_ats)
            if updated_frm >= partition_date:
                linea = str(ids)+"^"+ifnull(urls,"0")+"^"+ifnull(str(names),"0")+"^"+ifnull(str(emails),"0")+"^"+ifnull(str(created_ats),"0")+"^"+ifnull(str(updated_ats),"0")+"^"+ifnull(str(time_zones),"0")+"^"+ifnull(str(iana_time_zones),"0")+"^"+ifnull(str(phones),"0")+"^"+ifnull(str(shared_phone_numbers),"0")+"^"+ifnull(str(photos),"0")+"^"+ifnull(str(locale_ids),"0")+"^"+ifnull(str(locales),"0")+"^"+ifnull(str(organization_ids),"0")+"^"+ifnull(str(roles),"0")+"^"+ifnull(str(verifieds),"0")+"^"+ifnull(str(external_ids),"0")+"^"+ ifnull(str(tagss),"0")+"^"+ifnull(str(aliass),"0")+"^"+ifnull(str(actives),"0")+"^"+ifnull(str(shareds),"0")+"^"+ifnull(str(shared_agents),"0")+"^"+ifnull(str(last_login_ats),"0") +"^"+ifnull(str(two_factor_auth_enableds),"0") +"^"+ifnull(str(signatures),"0") +"^"+ifnull(str(detailss),"0") +"^"+ifnull(str(notess),"0") +"^"+ifnull(str(role_types),"0") +"^"+ifnull(str(custom_role_ids),"0") +"^"+ifnull(str(moderators),"0") +"^"+ifnull(str(ticket_restrictions),"0") +"^"+ifnull(str(only_private_commends),"0")+"^"+ifnull(str(restricted_agents),"0")+"^"+ifnull(str(suspends),"0")+"^"+ifnull(str(chat_onlys),"0")+"^"+ifnull(str(default_group_ids),"0")+"^"+ifnull(str(report_csvs),"0")+"^"+ifnull(str(user_fieldss),"0")
                archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_usr/fact/{}/zendesk_usr_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
                archivo_sal.write(linea+'\n')
                archivo_sal.close()
    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_user_files(partition_date_filter, partition_date_unix, partition_date):

    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_users/zendesk_users_config.json")
    print("Config File Read")

    for zendesk_usr_name, zendesk_usr_data in config.items():

        #base_url_santander = "https://santander-ar.zendesk.com/api/v2/incremental/users.json"
        #base_url_comex = "https://comex-santander.zendesk.com/api/v2/incremental/users.json"
        #base_url_puc = "https://puc-santander.zendesk.com/api/v2/incremental/users.json"

        payload = {}

        proxies = {
            "http": "http://proxy.ar.bsch:8080",
            "https": "http://proxy.ar.bsch:8080",
        }

        if zendesk_usr_data.get('item') == 'santander-ar':
            print("COMIENZA A PROCESAR SANTANDER-AR")
            headers = {
                'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOmkzMmhGN1YzdHBTeXJ6T2kyY1diTFM1MDM5UUFuSnU3Qjg0VGRtWmQ='
            }
        elif zendesk_usr_data.get('item') == 'comex-santander':
            print("COMIENZA A PROCESAR COMEX-SANTANDER")
            headers = {
                'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOlhGTnc2RkdwM3l5U1hWWDloUkxFV09lTE1mVGdQdFZzZFpyRnFYTmw='
            }
        elif zendesk_usr_data.get('item') == 'puc-santander':
            print("COMIENZA A PROCESAR PUC-SANTANDER")
            headers = {
                'Authorization': 'Basic amJyYWdhenppQHNhbnRhbmRlcnRlY25vbG9naWEuY29tLmFyL3Rva2VuOnVJMlJKV0JGZmh5U2lsNE5mcm00VXlsNFdnMkMxUXUwVFNkSmY3UFU='
            }
        else:
            print("COMIENZA A PROCESAR GETNET")
            headers = {
                'Authorization': 'Basic amJpY2htYW5Ac2FudGFuZGVydGVjbm9sb2dpYS5jb20uYXI6R2V0bmV0MTEyMA=='
            }

        conn = zendesk_usr_data.get('conn')
        print("conn: {}".format(conn))

        base_url = zendesk_usr_data.get('url')
        print("base_url: {}".format(base_url))

        v_start_time = round(float(partition_date_unix))
        print("v_start_time float: {}".format(v_start_time))

        end_executing = False

        columns = zendesk_usr_data.get('col')
        #print("Columns selected: {}".format(columns))
        df = pd.DataFrame(columns=columns)

        while not end_executing:
            # https://santander-ar.zendesk.com/api/v2/incremental/users.json?start_time=1606413237
            # https://comex-santander.zendesk.com/api/v2/incremental/users.json?start_time=1606413237
            # https://puc-santander.zendesk.com/api/v2/incremental/users.json?start_time=1606413237

            url = base_url+'?start_time='+str(v_start_time)
            print("Start Download: {}".format(url))

            #print("1- download_request_response")
            download_request_response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
            rdata = download_request_response.json()

            #print("2- Info")
            df_total = pd.DataFrame(rdata)
            df_total.info()

            #print("3- va a hacer el append")
            df1 = pd.DataFrame(rdata["users"], columns=columns)

            procesamiento(rdata["users"], zendesk_usr_data.get('item'), partition_date_filter, partition_date)

            v_start_time = rdata["end_time"]
            print("Next start_time: {}".format(v_start_time))

            end_executing = rdata["end_of_stream"]
            print("End executing: {}".format(end_executing))

            time.sleep(30) #espera en segundos
            print("delay: 30 seg")

        #print("sale del loop por executing")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create dates schema')

    parser.add_argument('--partition_date_filter', metavar='prev_partition_date_filter', required=True,
                        help='partition_date_filter')

    parser.add_argument('--partition_date_unix', metavar='partition_date_unix', required=True,
                        help='partition_date_unix')

    parser.add_argument('--partition_date', metavar='partition_date', required=True,
                        help='partition_date')

    args = parser.parse_args()

    download_zendesk_user_files(partition_date_filter=args.partition_date_filter, partition_date_unix=args.partition_date_unix, partition_date=args.partition_date)
