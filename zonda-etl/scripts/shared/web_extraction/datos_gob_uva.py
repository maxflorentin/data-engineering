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

def datos_gob_UVA_download_dag(path, date, name, folder, wd_from, wd_to ):
    url= "https://infra.datos.gob.ar/{0}/{1}.csv".format(path,name)
    proxies = {
        'http': ProductionConfig.PROXYS.get('http'),
        'https': ProductionConfig.PROXYS.get('https')
    }


    #Delete if file exists    
    File.Delete(wd_from,folder)

    #Donwload File        
    File.DownloadFile(url, wd_from + folder + name + '.csv', proxies)

    #Rename File    
    File.Rename(wd_from, folder, name, date)

    #Move to file 
    File.Move(wd_from, wd_to, folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Coeficiente de Estabilización de Referencia (CER); Unidades de Valor Adquisitvo (UVA) y Unidades de Vivienda (UVI)')
    parser.add_argument('--path', metavar='path', required=True,
                        help='the path to workspace')
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
    datos_gob_UVA_download_dag(args.path, args.date, args.name, args.folder, args.wd_from, args.wd_to)