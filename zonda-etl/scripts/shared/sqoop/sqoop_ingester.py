#!/usr/bin/python3
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators import ZondaHiveOperator
from airflow.operators import ZondaSlackOperator
import json
import os
valid_string= ''


def on_error_notifier(context,channels):
    """
    Send error notification alerts
    """
    slack_alert_error = ZondaSlackOperator(task_id="slack_alert_error", status=1, channels=channels)
    slack_response = slack_alert_error.execute(context=context)

    return slack_response


def sqoop_ingester_dag(server_file, table_file,channels,zonda_dir,fixed_tables,extra_args_var,partition_date_value): ##,context):
    extra_args = eval(extra_args_var)

    ## Read if tables comes with a fixed execution
    if fixed_tables:
        table_list = fixed_tables.split(",")
    else:
        table_list = []
    ## Read server-file
    with  open(server_file, 'r') as serverfile:
        servers = json.loads(serverfile.read())
    ## Read Table-file
    with open(table_file, 'r') as configfile:
        data = json.loads(configfile.read())
        for table in data['tables']:
            partition_date = partition_date_value
            command = table['command']
            for server in servers['servers']:
                if table['serverConfig'] == server['serverName']:
                    serverCommand = server
            for eachServerCfg in serverCommand:
                command = command.replace('{' + eachServerCfg + '}', serverCommand.get(eachServerCfg))
            for eachTableCfg in table:
                command = command.replace('{' + eachTableCfg + '}', table.get(eachTableCfg))
                #if the execution have an specific date to process in the table cfg
                if eachTableCfg == 'date_to_load':
                    partition_date = extra_args.get(table.get(eachTableCfg))
                    print('partition_date changes to {}'.format(partition_date))
            command = command.replace('{partition_date}', partition_date)
            print(fixed_tables)
            if ((fixed_tables == 'None') or (table.get('table') in table_list)):
                try:
                    ## SQOOP EXECUTION
                    conf = {'date_from': partition_date}
                    ## execute sqoop command
                    t = BashOperator(task_id='internal_task', bash_command=command, retries=3)
                    t.execute(context=extra_args_var)
                    print("SUCCESFULL SQOOP COMMAND {} ".format(command))

                    ## parquetize it with SparkSubmitOperator  - GUYRA
                    conf = {}
                    conf['spark.yarn.appMasterEnv.partition_date_filter'] = partition_date
                    conf['spark.sql.sources.partitionOverwriteMode'] = "dynamic"
                    json_data = os.path.join('/santander/bi-corp/config' , table.get('table').lower() +'_schema.json')
                    s = SparkSubmitOperator(task_id='internal_task', retries=3,
                                            name='STAGING_' + table.get('table') + '_Parquetize',
                                            conn_id='cloudera_spark2', executor_cores=5, num_executors=70,
                                            application=os.path.join(zonda_dir ,'repositories/guyra/src/guyra.py'),
                                            py_files=os.path.join(zonda_dir , 'repositories/guyra/src/config_file.py'),
                                            application_args=[json_data], conf=conf)
                    s.execute(context=extra_args_var)
                    print("SUCCESFULL GUYRA SUBMIT COMMAND {} ".format('STAGING_' + table.get('table') + '_Parquetize'))

                    ## Create Table if not exists with ZondaHiveOperator
                    create_table_file = os.path.join(zonda_dir, "/repositories/zonda-etl/scripts/layers/staging",
                                                 table.get('environment'), table.get('table_type'),
                                                 table.get('table').lower(), "DDLT-Create_Staging_Table.hql")

                    create_table_script = open(create_table_file).read()
                    for options in extra_args:
                        create_table_script = create_table_script.replace('{' + options + '}',str(extra_args.get(options)))
                    internal_task = ZondaHiveOperator(vars=extra_args, task_id='internal_task',
                                                      connection_id="cloudera_hive_beeline", schema="default",
                                                      is_hql_file=False, hql=create_table_script)
                    internal_task.execute(context=extra_args_var)
                    print("SUCCESFULL HIVE CREATE TABLE IF EXISTS ")

                    ## Add Partition with ZondaHiveOperator
                    add_partition_file = os.path.join(zonda_dir, "repositories/zonda-etl/scripts/layers/staging",
                                                 table.get('environment'), table.get('table_type'), table.get('table').lower(), "ETLV-Alter_Partitions.hql")

                    add_partition_script = open(add_partition_file).read()
                    for options in extra_args:
                        add_partition_script = add_partition_script.replace('{' + options + '}',
                                                                            str(extra_args.get(options)))
                    internal_task = ZondaHiveOperator(vars=extra_args, task_id='internal_task',
                                                      connection_id="cloudera_hive_beeline", schema="default",
                                                      is_hql_file=False, hql=add_partition_script)
                    internal_task.execute(context=extra_args_var)
                    # ti = TaskInstance(task=task, execution_date=datetime.now())
                    # task.execute()
                    print("SUCCESFULL HIVE ADD PARTITION COMMAND {} ".format(add_partition_script))
                except Exception as ex:
                    print(ex)
                    on_error_notifier(context=extra_args_var, channels=channels)
                    pass

if __name__ == "__main__":
    import argparse
    import ast
    workpath= os.path.dirname(os.path.realpath(__file__))
    print(workpath)


    #def list_type(data):
    #    return (data[0])

    parser = argparse.ArgumentParser(description='Create a Sqoop Ingestion Process')
    #parser.add_argument('--context', metavar='context', required=True,
    #                    help='zonda_dir')
    parser.add_argument('--table_file', metavar='table_file', required=True,
                        help='table_file')
    parser.add_argument('--partition_date', metavar='partition_date', required=True,
                        help='partition_date')
    parser.add_argument('--extra_args', metavar='extra_args', required=True,
                        help='extra_args')
    parser.add_argument('--schema', metavar='schema', required=False,
                        help='zonda_dir', default='bi_corp_staging')
    parser.add_argument('--server_file', metavar='server_file', required=False,
                        help='yearmonth', default=os.path.join(workpath , 'config/server_properties.json'))
    parser.add_argument('--channels', metavar='channels', required=False,
                        help='table_file', default=['zonda-alerts'])
    parser.add_argument('--zonda_dir', metavar='zonda_dir', required=False,
                        help='zonda_dir', default='/aplicaciones/bi/zonda')
    parser.add_argument('--fixed_tables', metavar='fixed_tables', required=False,
                        help='fixed_tables')

    args = parser.parse_args()
    args.extra_args = ast.literal_eval(args.extra_args)
    args.extra_args.update({'ti': args.extra_args})
    args.extra_args.update({'run_id': os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')})
    args.extra_args.update({'dag': os.environ.get('AIRFLOW_CTX_DAG_ID')})
    args.extra_args.update({'execution_date': os.environ.get('AIRFLOW_CTX_EXECUTION_DATE')})
    args.extra_args.update({'ti': args.extra_args })
    args.extra_args.update({'task': os.environ.get('AIRFLOW_CTX_TASK_ID')})
    
    print(args.extra_args)
    
    sqoop_ingester_dag(
        server_file=args.server_file, table_file= args.table_file, channels=args.channels, zonda_dir=args.zonda_dir
        , fixed_tables=args.fixed_tables, extra_args_var=args.extra_args, partition_date_value=args.partition_date)
       # , context=extra_args_var)
