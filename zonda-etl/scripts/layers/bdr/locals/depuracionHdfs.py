import os
import argparse
from pyzonda.hadoop import HDFS

def get_file_from_hdfs(argumentos,particiones):
    #partition_date = dayofrun
    #hdfs_path = '/santander/bi-corp/sandbox/rda/staging/reporte_nosis/clientes/partition_date=' + partition_date + '/'
    pre = '/santander/bi-corp/bdr'
    
    for argumento in argumentos:
        print("---------"+ argumento +"---------")
        for particion in particiones:
            hdfs_path = pre + argumento + '/partition_date=' + particion + '/'
            path_parquet = HDFS.ls(hdfs_path)
            for path in path_parquet:
                print(path["path"])

if __name__ == "__main__":
    argumento1 = "/jm_cal_emision" 
    argumento2 = "/jm_cal_ext_cl"
    argumentos = [argumento1,argumento2]
    particiones = ['2020-01','2020-02','2020-03']
    get_file_from_hdfs(argumentos,particiones)
    #parser = argparse.ArgumentParser(description='Create dates schema')
    #parser.add_argument('--path',       metavar='path',       required=True, help='path')
    #parser.add_argument('--date_from',  metavar='date_from',  required=True, help='date_from')
    #parser.add_argument('--months_old', metavar='months_old', required=True, help='months_old')
    #parser.add_argument('--days_old',   metavar='days_old',   required=True, help='days_old')
    #args = parser.parse_args()
    #depuracion(path=args.path,date_from=args.date_from, months_old=args.months_old,days_old=args.days_old)