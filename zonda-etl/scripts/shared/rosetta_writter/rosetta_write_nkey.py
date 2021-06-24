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

def get_contexts():
    spark = SparkContext.getOrCreate()
    spark.setLogLevel("INFO")
    spark_sql = SQLContext.getOrCreate(spark)
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    hadoop = spark._jvm.org.apache.hadoop
    global log
    log = spark._jvm.org.apache.log4j.LogManager.getLogger('rosetta_writer')
    level = spark._jvm.org.apache.log4j.Level.INFO
    log.setLevel(level)
    return spark, spark_sql, spark_session, hadoop

def write_to_hdfs(dataframe,path,first_time_write):

  if first_time_write:
    write_mode = 'overwrite'
  else:
    write_mode = 'append'
  log.info("Writing to HDFS ")
  dataframe.repartition(1) \
      .write \
      .json(path, mode= write_mode)

def write_control_hdfs(dataframe,path,domain):

  log.info("Writing CONTROL -  HDFS ")
  dataframe.repartition(1) \
      .write \
      .csv(os.path.join(path,'CONTROL-'+ domain),mode= 'overwrite')

def execute_query(spark_sql, domain_code,spark,hadoop,partition_date):
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    path = '/santander/bi-corp/temp/rosetta_writter/nkey' + os.sep + domain_code

    query_array = "select count(*) as total_reg \
        from bi_corp_common.rosetta_nkey \
        where domain_code = '{domain_code}' and master_key is not null"

    query_row_num = " select r.partition_date,r.domain_code, r.legal_entity_code, r.native_key, r.master_key  \
        ,r.end_date, ROW_NUMBER() OVER (partition by r.partition_date order by r.native_key ) AS row_num \
        from  bi_corp_common.rosetta_nkey r \
        where r.domain_code = '{domain_code}' and r.master_key is not null "

    query_ex = " select named_struct( \
                'Date', partition_date, \
		        'DomainCode', domain_code, \
		        'LegalEntityCode', legal_entity_code, \
		        'Keys', collect_set( \
    	    	named_struct( \
		        'NativeKey', cast(native_key as string), \
				'MasterKey', cast(master_key as string), \
				'EndDate', end_date \
                ) \
                ) \
                ) \
            from turcoputo_nkey  \
            where row_num between ${row_nr_from} and ${row_nr_to} \
            group by partition_date, domain_code, legal_entity_code "

    query = "select \
    partition_date \
    Date, domain_code \
    DomainCode, legal_entity_code \
    LegalEntityCode, collect_set(named_struct('NativeKey', cast(native_key as string), 'MasterKey', cast( \
        master_key as string), 'EndDate', end_date )) Keys \
    from turcoputo_nkey \
    where row_num between ${row_nr_from} and ${row_nr_to} \
    group by partition_date, domain_code, legal_entity_code"

    query_array = query_array.replace("{domain_code}", domain_code)
    query_row_num = query_row_num.replace("{domain_code}", domain_code)
    df_table_count = spark_sql.sql(query_array)
    value = df_table_count.select('total_reg').collect()[0]['total_reg']
    file_to_write = int(math.ceil(float(value) / float(600000))) + 1
    log.info("total files to write: '{}'".format(file_to_write))
    df_table_count.show()

    df_table_row_num = spark_sql.sql(query_row_num)
    df_table_row_num.createOrReplaceTempView("turcoputo_nkey")
    df_table_row_num.write.mode("overwrite").saveAsTable("default.test_nkey", path='/santander/bi-corp/temp/turcoputo_nkey/')
    df_table_row_num.show()

    t = 1
    first_write = True
    for i in range(1, file_to_write):
            row_nr_from = t
            row_nr_to = row_nr_from + 600000
            query_to_execute = query.replace("${row_nr_from}", "{}".format(row_nr_from))
            query_to_execute = query_to_execute.replace("${row_nr_to}", "{}".format(row_nr_to))
            df_table = spark_sql.sql(query_to_execute)
            log.info("Writting")
            log.info(query_to_execute)
            log.info("Writing From {} to {}".format(row_nr_from, row_nr_to))
            write_to_hdfs(df_table,path,first_write)
            t = row_nr_to + 1
            first_write = False

    list_status = fs.listStatus(spark_sql._jvm.org.apache.hadoop.fs.Path(path))
    file_names = [file.getPath().getName() for file in list_status if file.getPath().getName().startswith('part-')]
    for file_name in file_names:
        new_filename = "RTTA_00017_" + partition_date + "_" + domain_code + "_00083_NKEY_00000_" + "%03d" % (file_names.index(file_name)) + '.json'
        log.info("submitted " + new_filename)
        fs.rename(hadoop.fs.Path(os.path.join(path , file_name)), hadoop.fs.Path(os.path.join(path , new_filename)))

def rosetta_writter(domain_code,partition_date):

    spark, spark_sql, spark_session, hadoop = get_contexts()

    ## AVAILABLE CONTEXT
    execute_query(spark_sql,domain_code,spark,hadoop,partition_date)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--domain_code", help="domain_code to generate NKEY domain Rosetta Files")
    parser.add_argument("--partition_date", help="partition_date of the last day of month to process")
    args = parser.parse_args()

    # validate arguments
    if not args.domain_code:
        raise Exception("Missing parameter: --domain_code")
    if not args.partition_date:
        raise Exception("Missing parameter: --partition_date")
    rosetta_writter(args.domain_code,args.partition_date)