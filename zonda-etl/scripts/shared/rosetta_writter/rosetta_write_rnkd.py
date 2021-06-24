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


    query_segm = "select distinct segmentation_code as segmentation_code \
        from bi_corp_common.rosetta_rnkd \
        where domain_code = '{domain_code}'  and global_value is not null"
    query_segm = query_segm.replace("{domain_code}", domain_code)
    df_segment = spark_sql.sql(query_segm)
    seg_list = df_segment.select('segmentation_code').collect()
    segmentations = [row.segmentation_code for row in seg_list]
    log.info("total segmentations writte: '{}'".format(segmentations))

    for segment in segmentations:
        path = '/santander/bi-corp/temp/rosetta_writter/rnkd' + os.sep + domain_code+ os.sep + segment

        query_array = "select count(*) as total_reg \
        from bi_corp_common.rosetta_rnkd \
        where domain_code = '{domain_code}' and  segmentation_code = '{segment}' and global_value is not null"

        query_row_num = "select r.domain_code, r.legal_entity_code, r.native_key \
        ,r.partition_date,r.segmentation_code,r.attribute_code \
        ,r.global_value,r.local_value,r.end_date, ROW_NUMBER() OVER (partition by r.partition_date order by r.native_key ) AS row_num \
        from  bi_corp_common.rosetta_rnkd r \
        where r.domain_code = '{domain_code}' and r.global_value is not null and r.segmentation_code = '{segment}'  "

        query = "select a.partition_date Date, \
          a.domain_code DomainCode, \
          a.legal_entity_code LegalEntityCode, \
          collect_set( \
            named_struct( \
              'NativeKey',cast(a.native_key as string), \
              'Dimensions',a.dimensions) ) DimKeys \
        from \
        (select r.partition_date, \
        r.domain_code, \
        r.legal_entity_code, \
        r.native_key, \
        collect_set( \
                named_struct( \
                  'SegmentationCode',cast(r.segmentation_code as string) , \
                  'AttributeCode',cast(r.attribute_code as string) , \
                  'Value', nvl(cast(r.global_value as string),'') , \
                  'LocalValue',nvl(cast(r.local_value as string),''), \
                  'EndDate', nvl(cast(r.end_date as string),'') \
                )) as dimensions \
        from turcoputo r \
        where row_num between ${row_nr_from} and ${row_nr_to}  \
        group by r.partition_date, r.domain_code, r.legal_entity_code, r.native_key \
        ) a \
        group by a.partition_date, a.domain_code, a.legal_entity_code"

        query_array = query_array.replace("{segment}", segment)
        query_array = query_array.replace("{domain_code}", domain_code)

        query_row_num = query_row_num.replace("{segment}", segment)
        query_row_num = query_row_num.replace("{domain_code}", domain_code)
        df_table_count = spark_sql.sql(query_array)
        value = df_table_count.select('total_reg').collect()[0]['total_reg']
        file_to_write = int(math.ceil(float(value) / float(600000))) + 1
        log.info("total files to write: '{}'".format(file_to_write))
        df_table_count.show()

        df_table_row_num = spark_sql.sql(query_row_num)
        df_table_row_num.createOrReplaceTempView("turcoputo")
        df_table_row_num.write.mode("overwrite").saveAsTable("default.test_rnkd",path='/santander/bi-corp/temp/turcoputo/')
        df_table_row_num.show()

        t = 1
        first_write = True
        for i in range(1, file_to_write):
            row_nr_from = t
            row_nr_to = row_nr_from + 600000
            query_to_execute = query.replace("${row_nr_from}", "{}".format(row_nr_from))
            query_to_execute = query_to_execute.replace("${row_nr_to}", "{}".format(row_nr_to))
            df_table = spark_sql.sql(query_to_execute)
            log.info("Running")
            log.info(query_to_execute)
            log.info("Writing From {} to {}".format(row_nr_from, row_nr_to))
            write_to_hdfs(df_table,path,first_write)
            t = row_nr_to + 1
            first_write = False


    path_to = '/santander/bi-corp/temp/rosetta_writter/rnkd' + os.sep + domain_code

    for segment in segmentations:
        path = '/santander/bi-corp/temp/rosetta_writter/rnkd' + os.sep + domain_code + os.sep + segment
        list_status = fs.listStatus(spark_sql._jvm.org.apache.hadoop.fs.Path(path))
        file_names = [file.getPath().getName() for file in list_status if file.getPath().getName().startswith('part-')]
        for file_name in file_names:
            new_filename="RTTA_00017_"+partition_date +"_"+domain_code+"_00083_RNKD_"+ segment +"_"+ "%03d" % (file_names.index(file_name)) + '.json'
            log.info("submitted " + new_filename)
            fs.rename(hadoop.fs.Path(os.path.join(path , file_name)), hadoop.fs.Path(os.path.join(path_to , new_filename)))

def rosetta_writter(domain_code,partition_date):

    spark, spark_sql, spark_session, hadoop = get_contexts()

    ## AVAILABLE CONTEXT
    execute_query(spark_sql,domain_code,spark,hadoop,partition_date)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--domain_code", help="domain_code to generate RNKD domain Rosetta Files")
    parser.add_argument("--partition_date", help="partition_date of the last day of month to process")
    args = parser.parse_args()

    # validate arguments
    if not args.domain_code:
        raise Exception("Missing parameter: --domain_code")
    if not args.partition_date:
        raise Exception("Missing parameter: --partition_date")
    rosetta_writter(args.domain_code, args.partition_date)