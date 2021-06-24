#!/usr/bin/python3
import json
import ast
import re
import os
import requests, io
import urllib
import argparse
import urllib.request
from config.config import *
from services.zip import Zip_File
from services.rename_file import File
import pandas as pd

def excel_tocsv(filepath):
    data_xls = pd.read_excel(filepath)
    data_xls.to_csv(filepath.replace('.xls', '.csv'), encoding='utf-8', index=False)
    os.remove(filepath)


def enacom_num_geof_download_dag(date, name, folder, wd_from, wd_to ):
    url= 'https://www.enacom.gob.ar/multimedia/noticias/archivos/202011/archivo_20201106091215_442.xls'
    proxies = {
        'http': ProductionConfig.PROXYS.get('http'),
        'https': ProductionConfig.PROXYS.get('https')
    }


    #Delete if file exists    
    File.Delete(wd_from,folder)

    #Donwload File
    fileName=wd_from + folder + name + '.xls'
    try:
        with open(fileName, "wb") as file:
            UrlR = requests.request('GET', url, proxies=proxies)
            if UrlR.status_code == 200:
                file.write(UrlR.content)
    except requests.RequestException as e:
        print(e)

    excel_tocsv(wd_from + folder + name + '.xls')

    #Rename File    
    File.Rename(wd_from, folder, name, date)

    #Move to file 
    File.Move(wd_from, wd_to, folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Coeficiente de Estabilización de Referencia (CER); Unidades de Valor Adquisitvo (UVA) y Unidades de Vivienda (UVI)')
    parser.add_argument('--date', metavar='date', required=True,
                        help='date')
    parser.add_argument('--name', metavar='name', required=True,
                        help='name')
    parser.add_argument('--folder', metavar='folder', required=True,
                        help='utlfile')
    parser.add_argument('--wd_from', metavar='wd_from', required=True,
                        help='workingdirect from', default=[])
    parser.add_argument('--wd_to', metavar='wd_to', required=True,
                        help='workingdirect to', default=[])
    args = parser.parse_args()
    enacom_num_geof_download_dag(args.date, args.name, args.folder, args.wd_from, args.wd_to)