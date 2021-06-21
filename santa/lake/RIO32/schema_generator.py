# -*- coding: utf-8 -*-
# LEGAJO: A309423 -- v1.0
import glob
import ntpath
import os
import json
import sys
from ddlparse import DdlParse

def schema_json():
    print("""             __                              _                      
  ___ ____  / /  ___   __ _  ___ _          (_)  ___ ___   ___      
 (_-</ __/ / _ \/ -_) /  ' \/ _ `/         / /  (_-</ _ \ / _ \     
/___/\__/ /_//_/\__/ /_/_/_/\_,_/       __/ /  /___/\___//_//_/     
                                       |___/ 
                                       """)

    def path_leaf(path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    # PATH FILES SQL
    path = os.path.dirname(os.path.abspath(__file__))
    list_sql = glob.glob(os.path.join(path, "*.sql"))    
    
    if len(list_sql) > 0:

        #  GLOBAL VARIABLES
        partition_date_filter = "${partition_date_filter}"
        partition_date = "${partition_date}"
        delimiter = "^"

        # PARSE DDL
        try:
            for sql in list_sql:
                # TABLE VARIABLES
                t_name = path_leaf(sql.title()).lower().replace('.sql', '')
                db_name = t_name.split('--')[0]
                type_table = t_name.split('--')[1]
                t_name = t_name.split('--')[2]
                prefix = 'bi_corp_staging.'+db_name+'_'

                stg_name = prefix+t_name
                sch_name = '{}_schema'.format(t_name)
                location = "/santander/bi-corp/landing/{}/{}/{}/partition_date={}".format(db_name, type_table, t_name, partition_date_filter)
                destination = "/santander/bi-corp/staging/{}/{}/{}".format(db_name, type_table, t_name)
                tempDirectory = "/santander/bi-corp/temp/{}".format(db_name)

                with open(sql, 'r', encoding='utf-8') as f:
                    sql_str = f.read()
                    table = DdlParse().parse(ddl=sql_str, source_database=DdlParse.DATABASE.oracle)
                    tbl_fields = table.to_bigquery_fields()
                    d = json.loads(tbl_fields)
                    columns = []
                    dict_table = []
                    for i in range(0, len(d)):
                        columns.append(d[i]['name'])
                        dict_columns = {'name': d[i]['name'].lower(), 'type': 'string'}
                        dict_table.append(dict_columns)

                    # CREATE JSON
                    out_file = {'file': sch_name, 'createTable': stg_name, 'location': location,\
                                'destination': destination, 'tempDirectory': tempDirectory, 'extension': 'csv',\
                                'delimiter': delimiter, 'header': False, 'parquetFiles': 1, 'compression': 'gzip',\
                                'columns': dict_table, \
                                'extraColumns': [{'name': 'partition_date','value': partition_date,\
                                'type': 'string', 'partitionColumn': True, 'partitionOrder': 1}]}

                    # EXPORT JSON
                    data = json.dumps(out_file, indent=4, ensure_ascii=False)
                    output = os.path.join(path, '{}.json'.format(sch_name))
                    savefile = open(output, 'w', encoding='utf_8')
                    savefile.write(data)
                    savefile.close()
                    print("Completed OK: {}".format(sch_name))
        except:
            print('Please check the sql files and the format.')

        return print("\nSuccess! created files for %s tables.\n" % len(list_sql))
    else:
        print('There are no .sql files in this path\n')
    
if __name__ == schema_json():
    sys.exit(schema_json())
