from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import json
import sys
import os


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('datavault')
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


def run_ren_contr(spark, config_file):

    partition_date = os.path.expandvars(config_file["partition_date"])

    for ejson in config_file.get('querys'):

        insert_table = ejson['mddtccn']['insert_table']
        from_table = ejson['mddtccn']['from_table']
        select_fields = ','.join(ejson['mddtccn']['fields'])
        w_partition_date = ejson['mddtccn']['partition_date']

        spark.sql("INSERT OVERWRITE TABLE {insert_table} partition(partition_date = '{partition_date}') select {select_fields} from {from_table} where {w_partition_date} = '{partition_date}'".format(
            insert_table=insert_table, partition_date=partition_date, select_fields=select_fields, from_table=from_table, w_partition_date=w_partition_date))


if __name__ == "__main__":
    spark = get_contexts()
    config_file = get_config_data(spark)
    run_ren_contr(spark, config_file)
