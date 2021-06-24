import json
import os
from pyzonda.hadoop import HDFS
from slack import WebClient
from slack.errors import SlackApiError
import argparse
import datetime
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import sys
import os
from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark import SQLContext, SparkContext
import math
import argparse
import pyspark.sql.functions as psf
from pyspark.sql.functions import input_file_name

log = None

SLACK_API_TOKEN = os.getenv('SLACK_API_TOKEN')
ZONDA_DIR = os.getenv('ZONDA_DIR')
proxy = 'http://proxy.ar.bsch:8080'


def get_contexts():
    spark = SparkContext.getOrCreate()
    spark.setLogLevel("INFO")
    spark_sql = SQLContext.getOrCreate(spark)
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    hadoop = spark._jvm.org.apache.hadoop
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('check inbound')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark, spark_sql, spark_session, hadoop

def yesterday_holidays(date,spark,hadoop,spark_sql):
    query=''
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    df_table_count = spark_sql.sql(query)
    value = df_table_count.select('feriado').collect()[0]['feriado']
    try:
        if value == 1:
            return True
        else:
            return False
    except:
        return False

def zonda_inbound_control_files(date_control):

    client = WebClient(token=SLACK_API_TOKEN, ssl=False, proxy=proxy)
    landing_path = '/santander/bi-corp/landing'
    config_files=os.listdir(ZONDA_DIR + os.sep + "repositories/zonda-etl/scripts/shared/inbound/config")
    cdo_mailist = []
    for origin_file in config_files:
        with open(origin_file, 'r') as f:
            jobs_control_m = json.load(f)
        print("Json Config Readed")
        data_masters_to_notify = []
        data_masters_it_to_notify = []
        missing_list_files = []

        ZONDA_DIR_INB = ZONDA_DIR+"/inbound"

        for job in jobs_control_m['jobs']:

            #REMOVER CASUISTICA

            if 'rio6/fact/cotizaciones' in job['path']:
                path_to_check = '{}/ivct_fe_operacion={}'.format(job['path'].replace(ZONDA_DIR_INB, landing_path),
                                                              date_control if job.get('today', False) is False
                                                              else (datetime.datetime.strptime(date_control, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
            else:
                path_to_check = '{}/partition_date={}'.format(job['path'].replace(ZONDA_DIR_INB, landing_path),
                                                              date_control if job.get('today', False) is False
                                                              else (datetime.datetime.strptime(date_control, '%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))

            landing_dates = job.get('landing_dates')

            if job.get('control', False) is True:
                if isinstance(landing_dates, list) and int(datetime.datetime.now().strftime("%w")) in landing_dates:
                    if datetime.datetime.now().hour > job.get('landing_time'):
                        if not HDFS.exists(path_to_check):
                            app_error_name = job['application']
                            conf_error = {
                                "data_master": job['data_master'],
                                "data_master_it": job['data_master_it'],
                                "Path": job['path'],
                                "Partition": date_control,
                                "File": job['file'].replace('${partition_date}', date_control.replace('-', ''))
                            }
                            print(path_to_check)
                            missing_list_files.append(conf_error)
                            data_masters_to_notify.append(job['data_master'])
                            data_masters_it_to_notify.append(job['data_master_it'])
                print(missing_list_files)

        if missing_list_files:
            try:
                #
                df = pd.DataFrame(data=missing_list_files)

                # From y To del email
                origen = "zonda-alerts@santandertecnologia.com.ar"
                # destino_list = data_masters_to_notify + data_masters_it_to_notify + cdo_mailist
                destino_list = ["maurogonzalez@santandertecnologia.com.ar", "nbucardo@santandertecnologia.com.ar"]
                destino = ','.join([str(elem) for elem in destino_list])

                # Armamos el mensaje
                msg = MIMEMultipart('alternative')
                msg['Subject'] = "[IMPORTANTE - " + app_error_name.upper() + "] - Missing Files at Zonda Datalake "
                msg['From'] = origen
                msg['To'] = destino
                html = df.to_html(classes=["table-bordered", "table-striped", "table-hover"])
                text = 'Prueba'
                part1 = MIMEText(text, 'plain')
                part2 = MIMEText(html, 'html')

                # Injectamos el texto en html
                msg.attach(part1)
                msg.attach(part2)

                # Seteamos servidor SMTP.
                s = smtplib.SMTP('relay.ar.bsch', 25)
                s.set_debuglevel(1)
                # Enviamos el email
                s.sendmail(origen, destino, msg.as_string())
                s.quit()

            except SlackApiError as e:
                assert e.response["error"]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Execute ZONDA HDFS Control Files')

    parser.add_argument('--date_control', metavar='date_control', required=True,
                        help='the path to workspace')
    args = parser.parse_args()
    spark, spark_sql, spark_session, hadoop = get_contexts()


    if not(yesterday_holidays(args.date_control, spark, hadoop, spark_sql)):
        zonda_inbound_control_files(date_control=args.date_control)