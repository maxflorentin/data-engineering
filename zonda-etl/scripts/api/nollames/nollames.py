import sys
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql import functions as func
import json
import requests



def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('nollames')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def get_config_data(spark):
    control_file_path = '/user/{}/.sparkStaging/{app_id}/{file_name}'.format(spark.sparkContext.sparkUser(),
        app_id=spark._jsc.sc().applicationId(), file_name=sys.argv[1])
    config_raw = spark.read.option("multiline", "true").text(control_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
        psf.concat_ws("", psf.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)


def isInNoLlamesLotes(numbers):

    proxies = {
        "http": "http://proxy.ar.bsch:8080",
        "https": "http://proxy.ar.bsch:8080",
    }

    url = 'https://nollame.aaip.gob.ar/nollame-api/v1/api_call/callback_lotes'

    payload = {"numeros": numbers,
               "api_key": "143",
               "api_token": "3f5fbb74efb654d24b304995f3ef273006120bc9aa3872bbcde820492774c92c",
               "email": "cumplimiento_normativo@santanderrio.com.ar"
               }

    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(payload), headers=headers, proxies=proxies)

    return response.json()


def calculateInscripted(df, spark):

    numbers_concatenated = ''
    i = 0

    schema_newdf = StructType([
        StructField('error', StringType(), True),
        StructField('errores', StringType(), True),
        StructField('inscripta', StringType(), True),
        StructField('linea_tipo', StringType(), True),
        StructField('numero', StringType(), True)
    ])

    newdf = spark.createDataFrame([], schema_newdf)

    for interation in df.collect():
        if i == 0:
            numbers_concatenated = numbers_concatenated + interation['numero']
        else:
            numbers_concatenated = numbers_concatenated + ', ' + interation['numero']

        i += 1
        if i % 1000 == 0:
            loteNumeros = isInNoLlamesLotes(numbers_concatenated)
            tempdf  = spark.createDataFrame(loteNumeros, schema_newdf)
            newdf = newdf.unionAll(tempdf)

    if i % 1000 != 0:
        loteNumeros = isInNoLlamesLotes(numbers_concatenated)
        tempdf = spark.createDataFrame(loteNumeros, schema_newdf)
        newdf = newdf.unionAll(tempdf)

    return newdf

def run_nollames(spark, config_file):

    partition_value = os.path.expandvars(config_file["partition_value"])
    print("Processing to date: {partition_value}".format(partition_value=partition_value))

    for element in config_file.get("tables"):

        print("Initialize variables")
        schema = element["schema"]
        table = element["table"]
        fields = element["fields"]
        new_table_name = element["new_table_name"]
        base_path = element["destination_path"]
        partition_date_origin = element["partition_date_origin"]
        final_path = base_path + new_table_name

        print("Running query to get schema")
        #df_total = spark.sql("select distinct {fields} from {schema}.{table} where {partition_date_origin} = '{partition_value}' limit 10".format(fields=fields, schema=schema, table=table, partition_date_origin=partition_date_origin, partition_value=partition_value))
        df_total = spark.sql("select distinct {fields} from {schema}.{table} where {partition_date_origin} = '2021-06-07' limit 10".format(fields=fields, schema=schema, table=table, partition_date_origin=partition_date_origin, partition_value=partition_value))

        if df_total.count() > 0:
            print("Data encontrada en particion")
            particion_a_insertar = df_total.first()['partition_date']

            print("Getting info 'NoLLames'")
            df_total.show()
            newdf = calculateInscripted(df_total.select("numero").distinct(), spark)

            newdf = newdf.withColumn("partition_date", func.lit(f"{particion_a_insertar}")).join(df_total, on=['numero'], how='left')
            newdf.show()



            print("Writing Results")
            newdf \
                .write \
                .partitionBy(partition_date_origin) \
                .option("compression", 'gzip') \
                .saveAsTable(name=schema + '.' + new_table_name,
                             format='parquet',
                             mode='Overwrite',
                             path=final_path)

            print("Adding partition to table {schema}.{new_table_name}".format(schema=schema, new_table_name=new_table_name))
            #spark.sql("ALTER TABLE {schema}.{new_table_name} ADD IF NOT EXISTS PARTITION ({partition_date_origin} = '{partition_value}')".format(schema=schema, new_table_name=new_table_name, partition_date_origin=partition_date_origin, partition_value=partition_value))
            spark.sql("ALTER TABLE {schema}.{new_table_name} ADD IF NOT EXISTS PARTITION ({partition_date_origin} = '{particion_a_insertar}')".format(schema=schema, new_table_name=new_table_name, partition_date_origin=partition_date_origin, partition_value=particion_a_insertar))


if __name__ == "__main__":
    spark = get_contexts()
    config_file = get_config_data(spark)
    run_nollames(spark, config_file)