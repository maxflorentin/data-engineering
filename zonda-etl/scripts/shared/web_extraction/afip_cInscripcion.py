#!/usr/bin/python3
import json
import ast
import re
import os
import requests, io
import urllib
import argparse
from config.config import *
from services.zip import Zip_File
from services.rename_file import File


def afip_cInscripcion_download_dag(path, date, name, folder, wd_from, wd_to ):

    zip_file_url= "http://www.afip.gob.ar/{0}/{1}.zip".format(path,name) 

    proxies = {
            'http': ProductionConfig.PROXYS.get('http'),
            'https': ProductionConfig.PROXYS.get('https')        
    }  

    UrlR = requests.get(zip_file_url, verify=False, proxies=proxies)

    #Unzip File
    Zip_File.unzip(io.BytesIO(UrlR.content) ,wd_from)

    #Rename File
    name_file = 'afip_cInscripcion'
    File.Rename(wd_from, folder, name_file, date)

    #Move to file 
    File.Move(wd_from, wd_to, folder)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Constancia de Inscripción AFIP')

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


    afip_cInscripcion_download_dag(args.path, args.date, args.name, args.folder, args.wd_from, args.wd_to)