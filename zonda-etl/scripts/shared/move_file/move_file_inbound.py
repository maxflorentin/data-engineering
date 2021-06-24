import os
import ast
import datetime as dt
import json
import re
import requests
import argparse
from services.file_date import date_input
from services.util_cm import *

def move_to_inbound(pjson):

    with open(pjson + 'path.json') as df: #Path Origen
        df1 = json.load(df)        
        for dirc in df1['path']:
            try:    
                for file in os.listdir(dirc):                 
                    filename, file_extension = os.path.splitext(file)                        
                    with open( pjson + 'files.json') as df4: #Archivos
                        for c in json.load(df4):
                            if file.startswith(c['name']):                         
                                isdate = re.findall(r'\d+',file)
                                name_file = str(c['name'])
                                path_dest = str(c['path_dest'])
                                date_change = str(c['properties']['date_change'])
                                date_type = str(c['properties']['date_type'])
                                if len(isdate[-1]) == 6: 
                                    if date_change == 'True':                                         
                                        date_file =  getattr(date_input, str(date_type) ) 
                                        rename(os.path.join(dirc + file) ,dirc,name_file,date_file(isdate),file_extension)                              
                                        shutil_move( os.path.join(os.path.join(dirc + name_file + "_" +  date_file(isdate) +  file_extension)) , os.path.join(path_dest +  name_file + "_" +  date_file(isdate) +  file_extension))                              
                                elif len(isdate[-1]) == 8:
                                    if date_change == 'True':
                                        date_file =  getattr(date_input, str(date_type) ) 
                                        shutil_move( os.path.join(os.path.join(dirc + file)) , os.path.join(os.path.join(path_dest + name_file + "_" + date_file(isdate) +  file_extension)) ) 
                                    else:    
                                        shutil_move(  os.path.join(os.path.join(dirc + file)) , os.path.join(os.path.join(path_dest + file)) ) 
                                else:
                                    print("Review the file structure --> {}".format(file))                                                                              
            except (FileNotFoundError, IOError):
                print("File path not found --> {}".format(dirc))    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Move to Inbound')
    parser.add_argument('--pjson', metavar='pjson', required=True,
                        help='the path to json')
    args = parser.parse_args()                        
    move_to_inbound(args.pjson) 
