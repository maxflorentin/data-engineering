# -*- coding: utf-8 -*-
import requests ,json
import urllib
from time import sleep
import os
import unicodedata
import re
import sys
import argparse
import boto3
from botocore.client import Config

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_BUCKET_S3 = os.getenv('AWS_BUCKET_S3')

ZONDA_DIR = os.getenv('ZONDA_DIR')

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

def remover (valor):
    try:
        remo=os.remove(valor)
    except:
        remo=0
    return remo

def limpiar(var: object) -> object:
    val=re.sub('[^A-Za-z0-9- &áéíóúÁÉÍÓÚ/\.,]+?¡!$%()*¿_ñÑ@#', ' ', var)
    return val

def ifnull(var, val):
    if var.strip(" ") is None or var.strip(" ") == "" or var.strip(" ")==" ":
        return val
    return var.strip(" ")

def procesamiento(val,partition_date_filter):
    tot=val

    try:
        for i in range(0,200):
            idx= limpiar(str(tot[i]['id']))
            name= limpiar(str(tot[i]['name'].encode('unicode-escape').decode('latin-1')))
            folder= limpiar(str(tot[i]['folderName'].encode('unicode-escape').decode('latin-1')))

            try:
                types= limpiar(str(tot[i]['type'].encode('unicode-escape').decode('latin-1')))
            except:
                types=''
            try:
                purpose= limpiar(str(tot[i]['purpose'].encode('unicode-escape').decode('latin-1')))
            except:
                purpose=''
            try:
                listName= limpiar(str(tot[i]['listName'].encode('unicode-escape').decode('latin-1')))
            except:
                listName=''
            #filterPaths= tot[i]['filterPaths']
            try:
                htmlMgePath= limpiar(str(tot[i]['htmlMessagePath'].encode('unicode-escape').decode('latin-1')))
            except:
                htmlMgePath=''
            #textMgePath= tot[i]['textMessagePath ']
            try:
                enableLinkTrack= limpiar(str(tot[i]['enableLinkTracking'].encode('unicode-escape').decode('latin-1')))
            except:
                enableLinkTrack=''
            try:
                enableExternTrack= limpiar(str(tot[i]['enableExternalTracking'].encode('unicode-escape').decode('latin-1')))
            except:
                enableExternTrack=''
            try:
                subject= limpiar(str(tot[i]['subject'].encode('unicode-escape').decode('latin-1')))
            except:
                subject=''
            try:
                fromName= limpiar(str(tot[i]['fromName'].encode('unicode-escape').decode('latin-1')))
            except:
                fromName=''
            try:
                useUTF8= limpiar(str(tot[i]['useUTF8'].encode('unicode-escape').decode('latin-1')))
            except:
                useUTF8=''
            try:
                locale= limpiar(str(tot[i]['locale'].encode('unicode-escape').decode('latin-1')))
            except:
                locale=''
            try:
                trackHTMLOpens= limpiar(str(tot[i]['trackHTMLOpens'].encode('unicode-escape').decode('latin-1')))
            except:
                trackHTMLOpens=''
            try:
                tipotrackConvers =limpiar(str(tot[i]['trackConversions'].encode('unicode-escape').decode('latin-1')))
            except:
                tipotrackConvers=''
            try:
                sendTextIfHTMLUnknown= limpiar(str(tot[i]['sendTextIfHTMLUnknown'].encode('unicode-escape').decode('latin-1')))
            except:
                sendTextIfHTMLUnknown=''
            try:
                unsubscrOptn= limpiar(str(tot[i]['unsubscribeOption'].encode('unicode-escape').decode('latin-1')))
            except:
                unsubscrOptn=''
            try:
                autoCloseOpt= limpiar(str(tot[i]['autoCloseOption'].encode('unicode-escape').decode('latin-1')))
            except:
                autoCloseOpt=''
            #autoCloseVal= tot[i]['autoCloseValue']
            try:
                mktstrategy= limpiar(str(tot[i]['marketingStrategy'].encode('unicode-escape').decode('latin-1')))
            except:
                mktstrategy=''
            try:
                mktprogram= limpiar(str(tot[i]['marketingProgram'].encode('unicode-escape').decode('latin-1')))
            except:
                mktprogram=''
            try:
                fromemail= limpiar(str(tot[i]['fromEmail'].encode('unicode-escape').decode('latin-1')))
            except:
                fromemail=''
            try:
                replytoemail= limpiar(str(tot[i]['replyToEmail'].encode('unicode-escape').decode('latin-1')))
            except:
                replytoemail=''
            #links= tot[i]['links']
            try:
                extid=limpiar(str(tot[i]['externalCampaignCode'].encode('unicode-escape').decode('latin-1')))
            except:
                extid=''
            try:
                visql= limpiar(str(tot[i]['refiningDataSourcePath'].encode('unicode-escape').decode('latin-1')))
            except:
                visql=''

            linea = str(idx)+";"+ifnull(name,"0")+";"+ifnull(str(folder),"0")+";"+ifnull(str(visql),"0")+";"+ifnull(str(extid),"0")+";"+ifnull(str(types),"0")+";"+ifnull(str(purpose),"0")+";"+ifnull(str(listName),"0")+";"+ifnull(str(htmlMgePath),"0")+";"+ifnull(str(enableLinkTrack),"0")+";"+ifnull(str(enableExternTrack),"0")+";"+ifnull(str(subject),"0")+";"+ifnull(str(fromName),"0")+";"+ifnull(str(useUTF8),"0")+";"+ifnull(str(locale),"0")+";"+ifnull(str(trackHTMLOpens),"0")+";"+ifnull(str(tipotrackConvers),"0")+";"+ ifnull(str(sendTextIfHTMLUnknown),"0")+";"+ifnull(str(unsubscrOptn),"0")+";"+ifnull(str(autoCloseOpt),"0")+";"+ifnull(str(mktstrategy),"0")+";"+ifnull(str(mktprogram),"0")+";"+ifnull(str(fromemail),"0")+";"+ifnull(str(replytoemail),"0")
            archivo_sal = open(ZONDA_DIR+"/inbound/responsys/fact/campanias/responsys_api_{}.txt".format(partition_date_filter),"a")
            archivo_sal.write(linea+'\n')
            archivo_sal.close()

            if not ZONDA_DIR.__contains__('airflow'):
                df_final.to_csv(filename, sep='^', index=False)
            else:
                df_final.to_csv(filename, sep='^', index=False)
                s3_connection = create_s3_connection()
                s3_connection.Bucket(AWS_BUCKET_S3).upload_file(Filename=filename,
                                                                Key='data/in/{}_{}.csv'.format(surveyName.lower(),
                                                                                               dayofrun.replace('-',
                                                                                                                '')))
                rm = 'rm {}'.format(filename)
                rm = rm.split()
                p = subprocess.Popen(rm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in io.TextIOWrapper(p.stdout, encoding="utf-8"):
                    print(line.strip())

    except Exception as inst:
        print (inst)
        next


def download_responsys(partition_date_filter):
    global data, headers, proxies, s
    procesar=200
    i=1


    headers = {'Content-type': 'application/x-www-form-urlencoded'}
    proxies={'http':'http://proxy.ar.bsch:8080', 'https': 'https://proxy.ar.bsch:8080'}

    # datos de conexion
    data = {
        "user_name": 'dmariescurrena@santanderrio.com.ar',
        "password": '/Santander02/',
        "auth_type": "password"
    }

    s = requests.Session()
    s.verify = False


    try:

        while procesar >=200:
            voff=i*200


            # se pasa por post la los datos de proxy y conexion
            response=s.post('https://login.rsys8.net/rest/api/v1.3/auth/token', data=data, headers=headers,proxies=proxies)
            auth=response.text


            # se pasa a diccionario para poder obtener por separado el token y la pag a consultar
            x = json.loads(auth)
            #type(x)

            token=x["authToken"]
            issuedat=x["issuedAt"]
            endpoint=x["endPoint"]
            head = {'Authorization': token}

            #print(endpoint)

            # consulta de campañas
            res2= s.get(endpoint+'/rest/api/v1.3/campaigns?limit=200&offset=' + str(voff) + '&type=email',proxies=proxies,headers=head)
            res2.text

            procesar=(len(res2.text))

            total =res2.text
            data1 = json.loads(total)
            #print(data)

            #print (total)

            for item in data1:
                tot=(data1['campaigns'])
            procesamiento(tot,partition_date_filter)

            i=i+1


    except Exception as ee:
        print (ee)
        raise ee

    s.close()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')

    parser.add_argument('--partition_date_filter', metavar='partition_date_filter', required=True, help='partition_date_filter')
    args = parser.parse_args()

    download_responsys(partition_date_filter=args.partition_date_filter)
