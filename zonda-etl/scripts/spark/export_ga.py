from pyspark import SQLContext, SparkContext
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import subprocess
import sys


def main(desde, hasta, path, final_file):
    sc = SparkContext(master='yarn').getOrCreate()
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df = spark.sql('SELECT partition_date, full_visitor_id, segmento_de_cliente, tipo_de_identificacion, login_status, \
    device_mobileinputselector, user_network_location, hit_number, hit_type, hit_timestamp, canal, \
    hostname, screen, uri, nup, session_number, session_id, session_starttime, \
    session_duration, session_hits, trafficsource_channel, device_browser, device_browserversion, \
    device_browsersize, device_operatingsystem, device_operatingsystemversion, \
    device_mobiledevicebranding, device_mobiledevicemodel, device_mobiledeviceinfo, device_language, \
    device_screenresolution, device_devicecategory, user_country, user_city, user_network, \
    coordenadas_latitude, coordenadas_longitude FROM bi_corp_staging.ga_canales\
    WHERE partition_date BETWEEN "{}" AND "{}"'.format(desde, hasta))

    df.repartition(20).write.option("compression", "gzip").parquet('/santander/bi-corp/exportga/{}'.format(path))

    cmd = ['hdfs', 'dfs', '-copyToLocal', '/santander/bi-corp/exportga/{}'.format(path), '/aplicaciones/bi/exportga/']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)

    p.wait()
    if p.returncode != 0:
        raise Exception()

    cmd = ['tar', '/aplicaciones/bi/exportga/{}/*'.format(path), '{}.tar.gz'.format(final_file)]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)

    p.wait()
    if p.returncode != 0:
        raise Exception()

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])