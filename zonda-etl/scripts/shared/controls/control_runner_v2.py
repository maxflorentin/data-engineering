import json
import os
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType
log = None


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('control-runner')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def get_queries(spark):
    query_file_path = '/user/srvcengineerbi/.sparkStaging/{app_id}/{file_name}'.format(
        app_id=spark._jsc.sc().applicationId(), file_name=sys.argv[1])
    queries_raw = spark.read.option("multiline", "true").text(query_file_path)
    config_raw_row = queries_raw.agg(
        psf.concat_ws("", psf.collect_list(queries_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)


def run_controls():
    print("Setting Spark Session")
    spark = get_contexts()

    print("Getting Control Queries")
    queries_dict = get_queries(spark)
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

    queries = queries_dict.get("queries")
    log_table = queries_dict.get("logTable")

    table=log_table.split('.')
    listAllTables=spark.catalog.listTables(table[0])

    if table[1] in listAllTables:
        mode = 'append'
        print("Define Results Table Schema")
        log_results = spark.sql("select * from {table} limit 0".format(table=log_table))
    else:
        mode = 'overwrite'
        result_schema = StructType([
                    StructField("fecha_proceso", StringType(), False),
                    StructField("cod_incidencia", StringType(), False),
                    StructField("num_reg_total", StringType(), False),
                    StructField("num_reg_error", StringType(), False),
                    StructField("op_timestamp", StringType(), False),
                    StructField("tabla", StringType(), False),

        ])
        log_results = spark.createDataFrame([], result_schema)

    log_results.printSchema()
    print("Executing Queries")
    for query in queries:
        query_result = spark.sql(os.path.expandvars(query))
        log_results = log_results.unionByName(query_result)

    print("Writing Results")
    log_results \
        .coalesce(1) \
        .write \
        .partitionBy("tabla") \
        .saveAsTable(name=log_table,
                     format='hive',
                     mode=mode)


if __name__ == "__main__":
    run_controls()

