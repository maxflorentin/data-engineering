#!/usr/bin/python3
import json
import ast
import re
import os
import requests, zipfile, shutil
import urllib
valid_string= ''

def bcra_download_dag(path, files, yearmonth, folder, wd_from,wd_to):
    zip_file_url= "https://www.bcra.gob.ar/{path}/{yearmonth}.zip"
    zip_file_url= zip_file_url.replace("{path}", path)
    proxies = {
        "http": "http://proxy.ar.bsch:8080",
        "https": "http://proxy.ar.bsch:8080",
    }

    zip_file_url= zip_file_url.replace("{yearmonth}", yearmonth)
    import requests, zipfile, io
    r = requests.get(zip_file_url, verify=  False, proxies=proxies)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(wd_from)
    print(os.path.join(wd_from,folder))
    for file in os.listdir(os.path.join(wd_from,folder)):
        if len(files)==1:
            valid_string=files[0]
        else:
            for v_files in files:
                valid_string = valid_string + '|' + v_files
        regex_expression = '.*({valid_string}).*'
        regex_expression = regex_expression.replace("{valid_string}", valid_string)
        regex_expression = re.compile(regex_expression)
        if regex_expression.match(file):
            print(os.path.join(wd_from, folder ,file))
            print(os.path.join(wd_to ,file))
            shutil.move(os.path.join(wd_from, folder ,file), os.path.join(wd_to ,file.replace(".txt", "_"+ yearmonth + ".txt")))
        else:
            os.remove(os.path.join(wd_from, folder,file))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('--path', metavar='path', required=True,
                        help='the path to workspace')
    parser.add_argument('--yearmonth', metavar='yearmonth', required=True,
                        help='yearmonth')
    parser.add_argument('--folder', metavar='cheques', required=True,
                        help='cheques')
    parser.add_argument('--files' ,  type= str, nargs='+', required=True,
                        help='cheques')
    parser.add_argument('--wd_from', metavar='wd_from', required=True,
                        help='workingdirect from', default=[])
    parser.add_argument('--wd_to', metavar='wd_to', required=True,
                        help='workingdirect to', default=[])
    args = parser.parse_args()

    bcra_download_dag(path=args.path, files=args.files, yearmonth=args.yearmonth,folder=args.folder,wd_from=args.wd_from,wd_to=args.wd_to)
