import sys
import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import json


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('flattener')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def get_config_data(spark):
    control_file_path = '/user/{}/.sparkStaging/{app_id}/{file_name}'.format(spark.sparkContext.sparkUser(),
                                                                             app_id=spark._jsc.sc().applicationId(),
                                                                             file_name=sys.argv[1])
    config_raw = spark.read.option("multiline", "true").text(control_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
        psf.concat_ws("", psf.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)


def find_table(spark, db, table):
    query = 'show tables in {db}'.format(db=db)
    table_list = spark.sql(query)
    table_found = table_list.filter(table_list.tableName == table).collect()
    return len(table_found) == 0


def run_flattener(spark, config_file):

    partition_value = os.path.expandvars(config_file["partition_value"])

    for element in config_file.get("tables"):

        print("Initialize variables")
        schema = element["schema"]
        table = element["table"]
        fields = element["fields"]
        new_table_suffix = element["new_table_suffix"]
        base_path = element["destination_path"]
        to_flat_partition_name = element["to_flat_partition_name"]
        flattened_partition_name = element["flattened_partition_name"]

        try:
            to_flat_months = element.get("to_flat_months")
            flat_table = table + '_' + new_table_suffix + '_' + str(to_flat_months) + 'months'
            dates = pd.date_range(start=datetime.strptime(partition_value, '%Y-%m-%d') + relativedelta(months=-to_flat_months),end=datetime.strptime(partition_value, '%Y-%m-%d'), freq='d')
        except:
            to_flat_start_date = element.get("to_flat_start_date")
            partition_filter = to_flat_start_date.replace('-', '')
            flat_table = table + '_' + new_table_suffix + '_' + partition_filter
            dates = pd.date_range(start=datetime.strptime(to_flat_start_date, '%Y-%m-%d'),end=datetime.strptime(partition_value, '%Y-%m-%d'), freq='d')

        print("Running query to get schema")
        df_total = spark.sql("select {fields} from {schema}.{table} where {to_flat_partition_name} = '{partition_value}' limit 0".format(fields=fields, schema=schema, table=table, to_flat_partition_name=to_flat_partition_name, partition_value=partition_value))
        df_total.printSchema()
        flat_path = base_path + flat_table
        print("table: {table}, flat_table: {flat_table}, flat_path: {flat_path}".format(table=table, flat_table=flat_table, flat_path=flat_path))

        for fecha in dates:
            df_new_data = spark.sql("select {fields} from {schema}.{table} where {to_flat_partition_name} = '{partition_value}'".format(fields=fields, schema=schema, table=table, to_flat_partition_name=to_flat_partition_name, partition_value=fecha.date()))
            df_total = df_total.unionByName(df_new_data)

        print("Rename flat partition column and add new partition column to df")
        df_total = df_total.withColumnRenamed(to_flat_partition_name, element["flattened_partition_name"] + '_' + new_table_suffix)
        df_total = df_total.withColumn(flattened_partition_name, psf.lit(partition_value))

        print("Writing Results")
        if find_table(spark, schema, flat_table):
            df_total \
                .repartition(1) \
                .write \
                .partitionBy(flattened_partition_name) \
                .option("compression", 'gzip') \
                .saveAsTable(name=schema + '.' + flat_table,
                             format='parquet',
                             mode='append',
                             path=flat_path)
        else:
            df_total \
                .repartition(1) \
                .write \
                .partitionBy(flattened_partition_name) \
                .mode('overwrite') \
                .option("compression",'gzip') \
                .parquet(flat_path)


        print("Adding partition to table {schema}.{flat_table}".format(schema=schema, flat_table=flat_table))
        spark.sql("ALTER TABLE {schema}.{flat_table} ADD IF NOT EXISTS PARTITION ({flattened_partition_name} = '{partition_value}')".format(schema=schema, flat_table=flat_table, flattened_partition_name=flattened_partition_name, partition_value=partition_value))


if __name__ == "__main__":
    spark = get_contexts()
    config_file = get_config_data(spark)
    run_flattener(spark, config_file)
