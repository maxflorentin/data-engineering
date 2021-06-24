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
    val = re.sub('channel', '==channel==', var)
    val1 = re.sub('source', '==source==', val)
    return val1


def ifnull(var, val):
    if var.strip(" ") is None or var.strip(" ") == "" or var.strip(" ")==" ":
        return val
    return var.strip(" ")

def procesamiento(val, json_item, partition_date_filter, partition_date):

    tot = val
    cote = len(tot)
    print(str(len(tot)))
    format1 = "%Y%b%a"

    try:
        for i in range(0,cote):

            urls = str(tot[i].get('url'))

            ids = str(tot[i].get('id'))

            external_ids = tot[i].get('external_id')

            vias = limpiar2(str(tot[i].get('via')))

            created_ats = str(tot[i].get('created_at'))

            updated_ats = str(tot[i].get('updated_at'))
            updated_frm = updated_ats[0:10]

            types = tot[i].get('type')

            subjects = limpiar(tot[i].get('subject'))

            raw_subjects = limpiar(tot[i].get('raw_subject'))

            descriptions = limpiar(tot[i].get('description'))

            prioritys = limpiar(str(tot[i].get('priority')))

            statuss = limpiar(tot[i].get('status'))

            recipients = tot[i].get('recipient')

            requester_ids = str(tot[i].get('requester_id'))

            submitter_ids = str(tot[i].get('submitter_id'))

            assignee_ids = str(tot[i].get('assignee_id'))

            organization_ids = str(tot[i].get('organization_ids'))

            group_ids = str(tot[i].get('group_id'))

            collaborator_idss = str(tot[i].get('collaborator_ids'))

            follower_idss = str(tot[i].get('follower_ids'))

            email_cc_idss = str(tot[i].get('email_cc_ids'))

            forum_topic_ids = str(tot[i].get('forum_topic_id'))

            problem_ids = str(tot[i].get('problem_id'))

            has_incidentss = tot[i].get('has_incidents')

            is_publics = tot[i].get('is_public')

            due_ats = str(tot[i].get('due_at'))

            tagss = str(tot[i].get('tags'))

            custom_fieldss = str(tot[i].get('custom_fields'))

            satisfaction_ratings = limpiar(str(tot[i].get('satisfaction_rating')))

            sharing_agreement_idss = str(tot[i].get('sharing_agreement_ids'))

            fieldss = str(tot[i].get('fields'))

            followup_idss = str(tot[i].get('followup_ids'))

            ticket_form_ids = str(tot[i].get('ticket_form_id'))

            brand_ids = str(tot[i].get('brand_id'))

            satisfaction_probabilitys = limpiar(str(tot[i].get('satisfaction_probability')))

            allow_channelbacks = tot[i].get('allow_channelback')

            allow_attachmentss = tot[i].get('allow_attachments')

            result_types = str(tot[i].get('result_type'))
            if updated_frm >= partition_date:
                linea = ifnull(str(urls),"0")+"^"+str(ids)+"^"+ifnull(str(external_ids),"0")+"^"+vias+"^"+ifnull(str(created_ats),"0")+"^"+ifnull(str(updated_ats),"0")+"^"+ifnull(str(types),"0")+"^"+ifnull(str(subjects),"0")+"^"+ifnull(str(raw_subjects),"0")+"^"+ifnull(str(descriptions),"0")+"^"+ifnull(str(prioritys),"0")+"^"+ifnull(str(statuss),"0")+"^"+ifnull(str(recipients),"0")+"^"+ifnull(str(requester_ids),"0")+"^"+ifnull(str(submitter_ids),"0")+"^"+ifnull(str(assignee_ids),"0")+"^"+ifnull(str(organization_ids),"0")+"^"+ifnull(str(group_ids),"0")+"^"+collaborator_idss+"^"+follower_idss+"^"+email_cc_idss+"^"+ifnull(str(forum_topic_ids),"0")+"^"+ifnull(str(problem_ids),"0")+"^"+ifnull(str(str(has_incidentss)),"0")+"^"+ifnull(str(str(is_publics)),"0")+"^"+ifnull(str(due_ats),"0")+"^"+tagss+"^"+custom_fieldss+"^"+ifnull(str(satisfaction_ratings),"0")+"^"+sharing_agreement_idss+"^"+fieldss+"^"+followup_idss+"^"+ifnull(str(ticket_form_ids),"0")+"^"+ifnull(str(brand_ids),"0")+"^"+ifnull(str(satisfaction_probabilitys),"0")+"^"+ifnull(str(str(allow_channelbacks)),"0")+"^"+ifnull(str(str(allow_attachmentss)),"0")+"^"+ifnull(str(str(result_types)),"0")
                #print("archivo escribe linea{}".format(linea))
                archivo_sal = open(ZONDA_DIR + "/inbound/zendesk_tk/fact/{}/zendesk_tk_{}_{}.csv".format(json_item, json_item, partition_date_filter),'a')
                archivo_sal.write(linea+'\n')
                archivo_sal.close()
                #print("Escribe el archivo fecha:")
                #print(updated_frm)
            #else:
                #print("**No escribe el archivo fecha:")
                #print(updated_frm)

    except Exception as inst:
        print("error en parseo")
        print(inst)
        next

def download_zendesk_tk_files(partition_date_filter, partition_date_unix, partition_date):


    config = read_config(ZONDA_DIR + "/repositories/zonda-etl/scripts/api/zendesk_tickets/zendesk_tickets_config.json")
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
        elif zendesk_tk_data.get('item') == 'comex-santander':
            print("COMIENZA A PROCESAR COMEX-SANTANDER")
            headers = {
                'Authorization': 'Basic YXV0b21hdGljb0BzYW50YW5kZXIuY29tLmFyL3Rva2VuOlhGTnc2RkdwM3l5U1hWWDloUkxFV09lTE1mVGdQdFZzZFpyRnFYTmw='
            }
        elif zendesk_tk_data.get('item') == 'puc-santander':
            print("COMIENZA A PROCESAR PUC-SANTANDER")
            headers = {
                'Authorization': 'Basic amJyYWdhenppQHNhbnRhbmRlcnRlY25vbG9naWEuY29tLmFyL3Rva2VuOnVJMlJKV0JGZmh5U2lsNE5mcm00VXlsNFdnMkMxUXUwVFNkSmY3UFU='
            }
        else:
            print("COMIENZA A PROCESAR GETNET")
            headers = {
                'Authorization': 'Basic amJpY2htYW5Ac2FudGFuZGVydGVjbm9sb2dpYS5jb20uYXI6R2V0bmV0MTEyMA=='
            }

        conn = zendesk_tk_data.get('conn')
        print("conn: {}".format(conn))

        base_url = zendesk_tk_data.get('url')
        print("base_url: {}".format(base_url))

        v_start_time = partition_date_unix
        print("v_start_time: {}".format(v_start_time))

        end_executing = False

        columns = zendesk_tk_data.get('col')
        print("Columns selected: {}".format(columns))
        df = pd.DataFrame(columns=columns)

        while not end_executing:
            # https://santander-ar.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://comex-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://puc-santander.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237
            # https://globalgetnet.zendesk.com/api/v2/incremental/tickets.json?start_time=1606413237

            url = base_url+'?start_time='+str(v_start_time)
            print("Start Download: {}".format(url))

            print("1- download_request_response")
            download_request_response = requests.request("GET", url, headers=headers, data=payload, proxies=proxies)
            rdata = download_request_response.json()

            print("2- Info")

            if "deleted_ticket_forms" in rdata:
                del rdata["deleted_ticket_forms"]

            df_total = pd.DataFrame(rdata)
            df_total.info()

            print("3- va a hacer el append")
            df1 = pd.DataFrame(rdata["tickets"], columns=columns)

            procesamiento(rdata["tickets"], zendesk_tk_data.get('item'), partition_date_filter, partition_date)

            v_start_time = rdata["end_time"]
            print("Next start_time: {}".format(v_start_time))

            end_executing = rdata["end_of_stream"]
            print("End executing: {}".format(end_executing))

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
