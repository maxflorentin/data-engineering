#!/usr/bin/python3
import json
import ast
import re
import os
partition_date='2019-12-02'

def load_cfg_dag(*args, **kwargs):
    with  open('config/server_properties.json', 'r') as serverfile:
        servers = json.loads(serverfile.read())
    with open('config/risk_cfg.json', 'r') as configfile:
        data = json.loads(configfile.read())
        for table in data:
            command = json.loads(table)
            command = command['command']
            print(command)
            for eachServerCfg in servers[table['serverConfig']]:
                command = command.replace('{' + eachServerCfg + '}', eachServerCfg[eachServerCfg])
            for eachTableCfg in table:
                command = command.replace('{' + eachTableCfg + '}', eachTableCfg[eachTableCfg])
            command=command.replace('{partition_date}', partition_date)
            print(command)



if __name__ == "__main__":
    load_cfg_dag()