# -*- coding: utf-8 -*-
# LEGAJO: A309423 -- v1.0
import glob
import ntpath
import os
import json
import sys
from ddlparse import DdlParse

def sqoop_json():
    print("""
    __/__/__/__/__/__/__/__/__/__/__/
            SQOOP GENERATOR
    _/__/__/__/__/__/__/__/__/__/__/""")

    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    # PATH FILES SQL
    path = os.path.dirname(os.path.abspath(__file__))
    list_sql = glob.glob(os.path.join(path, "*.sql"))    
    
    if len(list_sql) > 0:

        #  GLOBAL VARIABLES
        partition_date = "${partition_date}"

        # PARSE DDL
        try:
            for sql in list_sql:
                # TABLE VARIABLES
                t_name = path_leaf(sql.title()).lower().replace('.sql', '')
                db_name = t_name.split('--')[0]
                type_table = t_name.split('--')[1]
                t_name = t_name.split('--')[2]
                prefix = 'bi_corp_staging.'+db_name+'_'

                sch_name = '{}_schema'.format(t_name)
                location = "/santander/bi-corp/landing/{}/{}/{}/partition_date={}".format(db_name, type_table, t_name, partition_date)

                with open(sql, 'r', encoding='utf-8') as f:
                    sql_str = f.read()
                    table = DdlParse().parse(ddl=sql_str, source_database=DdlParse.DATABASE.oracle ) #
                    tbl_fields = table.to_bigquery_fields()
                    d = json.loads(tbl_fields)
                    columns = []
                    dict_table = []
                    for i in range(0, len(d)):
                        columns.append(d[i]['name'])
                        dict_columns = {'name': d[i]['name'].lower(), 'type': 'string'}
                        dict_table.append(dict_columns)
               
                    # SQOOP COMMAND VARIABLES
                    s_table = r'{table}'
                    s_fulltable = r'\"{table}\"'
                    s_servername = r"{serverName}"
                    s_port = r"{port}"
                    s_service = r"{service}"
                    s_username = r"{username}"
                    s_database = r"{database}"
                    s_columns0 = str(columns).replace('[', '').replace(']', '').replace("'", "")
                    s_columns = r'\"'+s_columns0+r'\"'
                    s_landing_path = r"{landing_path}"
                    s_partition_date = r"{partition_date}"
                    s_password = r"{password}"
                    nn = r'\n'
                    command = "sqoop import -Dmapreduce.output.basename= {} -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@{}:{}:{}' --username '{}' --table {} --columns {} --bindir '/aplicaciones/bi/zonda/sqoop/{}/{}' --target-dir  '{}/partition_date={}' --delete-target-dir --fields-terminated-by '^' --num-mappers 1 --password '{}'  --verbose{}".format(s_table, s_servername, s_port, s_service, s_username, s_fulltable, s_columns, db_name, s_table, s_landing_path, s_partition_date, s_password, nn)
                    
                    # print(command) 
                    
                    # CREATE JSON
                    out_file = {"serverAka": db_name,
                                "database": "CSM",
                                "table": t_name,
                                "table_type": type_table,
                                "environment": db_name.upper(),
                                "landing_path": location,
                                "command": command
                                }

                    # EXPORT JSON
                    data = json.dumps(out_file, indent=4, ensure_ascii=False)
                    output = os.path.join(path, '{}_cfg.json'.format(t_name))
                    savefile = open(output, 'w', encoding='utf_8')
                    savefile.write(data)
                    savefile.close()
                    print("Completed OK: {}".format(sch_name))
        except:
            print('Please check the sql files and the format.')

        return print("\nSuccess! created files for %s tables.\n" % len(list_sql))
    else:
        print('There are no .sql files in this path\n')
    
if __name__ == sqoop_json():
    sys.exit(sqoop_json())
