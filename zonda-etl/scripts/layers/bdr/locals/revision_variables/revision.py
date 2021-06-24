import os
import argparse

def loop(path, executions, last_working_day):
    executions = int(executions)
    #hive -f path.hql
    path = '$ZONDA_DIR/repositories/zonda-etl/scripts/layers/bdr/locals/garantias/ETLV_test.hql'
    #command = "hive --hivevar iteracion='" + last_working_day + "' -f " + path
    #print(command)
    for i in range(executions):
        execution = i + 1
        command = "hive --hivevar iteracion='" + str(execution) + "' -f " + path
        status = os.system(command)
        if(status != 0):
            raise Exception('Loop ' + str(execution) + ' Failed')
        print('Finish execution: ' + str(execution))
    print('Finish')

if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Create dates schema')
    parser.add_argument('--path',             metavar='path',             required=True, help='path')
    parser.add_argument('--executions',       metavar='executions',       required=True, help='executions')
    parser.add_argument('--last_working_day', metavar='last_working_day', required=True, help='last_working_day')
    args = parser.parse_args()
    loop(path=args.path,executions=args.executions,last_working_day=args.last_working_day)