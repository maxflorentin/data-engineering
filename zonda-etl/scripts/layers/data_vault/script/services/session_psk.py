from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import json
import sys
import os

class C_PySpark:

    @staticmethod
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


