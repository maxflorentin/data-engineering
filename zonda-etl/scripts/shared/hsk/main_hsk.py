#!/usr/bin/python3

import subprocess
from datetime import datetime
import json
import argparse


def zonda_housekeeping(config,date_allowed,path,test):
    p1 = subprocess.Popen(["hadoop","fs","-ls","-R", path], stdout=subprocess.PIPE)
    p2 = subprocess.Popen(["grep", "^-"], stdin=p1.stdout, stdout=subprocess.PIPE,universal_newlines=True)
    p1.stdout.close()
    output = p2.communicate()[0]
    output = output.split(('\n'))
    with open(config, 'r') as serverfile:
        config_data = json.loads(serverfile.read())
    allowed_list_paths=[]
    for config_data_path in config_data['path']:
        if config_data_path['keep'] :
            allowed_list_paths.append(config_data_path['path'])

    for line in output:
        try:
            process_line = True
            for path in allowed_list_paths:
                if path in line[line.find("/"):]:
                    process_line = False
                    print (line[line.find("/"):] + ' exception')
                    break
            if (process_line) and (datetime.strptime(line[ (line.find("/")-17):(line.find("/")-7) ], '%Y-%m-%d').date() <= datetime.strptime(date_allowed, '%Y-%m-%d').date()):
                    print('ALERT :' + line[line.find("/"):] + ' could be deleted because of date : ' + line[(line.find("/") - 17):(line.find("/") - 7)])
                    if not(test):
                        p = subprocess.Popen( ["hdfs", "dfs" , "-rm", line[line.find("/"):]], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
                        print('DELETED :' + line[line.find("/"):])
        except:
            print('ERROR')
            print(line)
            try:
                print(p.stdout)
            except:
                pass
            pass
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Execute ZONDA HSK')
    parser.add_argument('--config', metavar='config', required=True,
                        help='the path to workspace')
    parser.add_argument('--date_allowed', metavar='date_allowed', required=True,
                        help='the date allowed to keep files')
    parser.add_argument('--path', metavar='path', required=True,
                        help='the path to workspace', default="/santander/bi-corp/landing/")
    parser.add_argument('--test', metavar='test', required=False,
                        help='test the process', default=False)
    args = parser.parse_args()
    zonda_housekeeping(config=args.config,date_allowed=args.date_allowed,path=args.path,test=args.test)