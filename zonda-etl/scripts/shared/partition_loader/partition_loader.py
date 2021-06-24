import sys
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import subprocess
import json

def remove(path):
    cmd = 'hdfs dfs -rm -f {}'.format(path)
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')


def write_json(json, path):
    cmd = 'echo "{}" | hdfs dfs -appendToFile - {}'.format(json,path)
    return subprocess.check_output(cmd, shell=True).decode('utf-8').strip().split('\n')


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


def get_contexts():
    spark = SparkSession.builder \
        .enableHiveSupport() \
        .getOrCreate()
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('tarjetas_loader')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark


def find_table(spark, db, table):
    query = 'show tables in {db}'.format(db=db)
    table_list = spark.sql(query)
    table_found = table_list.filter(table_list.tableName == table).collect()
    return len(table_found) != 0


def save_data(spark,df,schema,table,path, partition):
    if find_table(spark,schema,table):
        if df.count() != 0:
            spark.sql("refresh table {schema}.{table}".format(schema=schema, table=table))
            log.info("Writing partitioned data frame")
            df \
                .repartition(1) \
                .write \
                .partitionBy("partition_date") \
                .mode('Overwrite') \
                .option("compression",'gzip') \
                .parquet(path)

            #spark.catalog.refreshTable("{schema}.{table}".format(schema=schema, table=table))
            spark.sql("refresh table {schema}.{table}".format(schema=schema, table=table))
            spark.sql("ALTER TABLE {schema}.{table} ADD IF NOT EXISTS PARTITION (partition_date='{partition}')".format(schema=schema, table=table, partition=partition))

    else:
        log.info("Creating table and saving data frame")
        spark.sql("refresh table {schema}.{table}".format(schema=schema, table=table))
        df \
            .repartition(1) \
            .write \
            .partitionBy("partition_date") \
            .option("compression", 'gzip') \
            .saveAsTable(name=schema + '.' + table,
                         format='parquet',
                         mode='append',
                         path=path)
    #spark.catalog.refreshTable("{schema}.{table}".format(schema=schema, table=table))
    spark.sql("refresh table {schema}.{table}".format(schema=schema, table=table))


def get_data_frame(spark,parameters):
    query = parameters.get("query").format(partition_field=parameters.get("partition_field"), schema=parameters.get("schema"), table=parameters.get("table"), partition_date=os.path.expandvars(parameters.get("partition_date")))
    df_table = spark.sql(query)
    df_table.printSchema()
    df_table.show()
    return df_table


def loader(spark, control_file):
    processed_dates = []
    for element in control_file.get("tables").keys():
        data_frame = get_data_frame(spark, control_file.get("tables").get(element))
        processed_dates = processed_dates + partition_loader(spark, control_file.get("tables").get(element), data_frame)

    print("fechas procesadas: {}".format(processed_dates))
    processed_dates_list = list(set(processed_dates))
    json_str = "{\"dates\":" + json.dumps(processed_dates_list) + "}"
    print("json: {}".format(json_str))

    remove("{}/{}.json".format("/santander/bi-corp/tmp/partition_loader", control_file.get("dag_id")))
    write_json(json_str.replace('"', '\\"'),"{}/{}.json".format("/santander/bi-corp/tmp/partition_loader", control_file.get("dag_id")))


def partition_loader(spark, parameters, df_table):
    processed_dates = []
    if df_table.count() != 0:
        df_dates = df_table.select("new_partition_date").where(psf.col("new_partition_date").isNotNull())
        df_dates_distinct= df_dates.select("new_partition_date").distinct()
        print("distinct dates:")
        df_dates_distinct.show()

        for date in df_dates_distinct.collect():
            df_filtered = df_table.filter(df_table.new_partition_date == date[0])
            df_filtered=df_filtered.withColumnRenamed("partition_date","partition_date_staging")
            df_filtered=df_filtered.withColumnRenamed("new_partition_date","partition_date")

            if find_table(spark,parameters.get("schema"), parameters.get("new_table")):
                #spark.catalog.refreshTable("{schema}.{table}".format(schema=parameters.get("schema"), table=parameters.get("new_table")))
                spark.sql("refresh table {schema}.{table}".format(schema=parameters.get("schema"), table=parameters.get("new_table")))
                df_new_table = spark.sql("select * from {schema}.{table} "
                                          "where partition_date='{date}' "
                                          "and partition_date_staging <> '{partition_date}'".format(schema=parameters.get("schema"),
                                                                                                    table=parameters.get("new_table"),
                                                                                                    date=date[0],
                                                                                                    partition_date=os.path.expandvars(parameters.get("partition_date"))))
                df_total = df_filtered.union(df_new_table)
            else:
                df_total = df_filtered

            print("df_total count:{}".format(df_total.count()))
            save_data(spark, df_total, parameters.get("schema"), parameters.get("new_table"), parameters.get("path"), date[0])
            processed_dates.append(date[0])

        #Records with errors in new_partition_date field in 0000-00-00 partition
        df_errors = df_table.where(psf.col("new_partition_date").isNull())
        if df_errors.count() != 0:
            df_errors_query = spark.sql("select * from {schema}.{table} where partition_date='{partition_date}'".format(schema=parameters.get("schema"), table=parameters.get("new_table"),partition_date="0000-00-00"))
            if df_errors_query.count() != 0:
                df_errors_total = df_errors_query.union(df_errors)
            else:
                df_errors_total = df_errors

            df_errors_total = df_errors_total.withColumnRenamed("partition_date", "partition_date_staging")
            df_errors_total = df_errors_total.withColumn("partition_date", psf.lit("0000-00-00"))
            df_errors_total.show()
            save_data(spark,df_errors_total,parameters.get("schema"),parameters.get("new_table"),parameters.get("path"), "0000-00-00")
            df_errors_total.show()
            print("df_errors count:{}".format(df_errors_total.count()))

    else:
        print("Empty Data Frame")

    return processed_dates

if __name__ == "__main__":
    spark = get_contexts()
    control_file = get_config_data(spark)
    loader(spark,control_file)
