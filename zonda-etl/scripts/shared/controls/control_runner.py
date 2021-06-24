import json
import os
import sys

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

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

    print("Define Results Table Schema")
    log_results = spark.sql("select * from {table} limit 0".format(table=log_table))
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
                     mode='append')


if __name__ == "__main__":
    run_controls()

