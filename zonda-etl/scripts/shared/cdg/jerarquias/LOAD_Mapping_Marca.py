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
  dataframe.repartition(1).write.mode(write_mode).parquet(path)


def execute_query(spark_sql,spark,hadoop,partition_date,max_level):
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())
    path = '/santander/bi-corp/common/rentabilidad/dim/dim_{dim}_marca/partition_date='+ partition_date

    ## tantas veces como niveles  - resultados 1-14
    query = "select cod_ren_cta_resultados, res.cod_marca \
                from bi_corp_common.dim_ren_ctaresultadosctr \
                inner join bi_corp_staging.param_marcas_resultado_general res on cod_ren_cta_resultados_niv_{lvl}=res.cod_nodo  \
                where res.cod_nivel='{lvl}'"

    for i in range(1, int(max_level)):
        query_to_execute = query.replace("{lvl}", str(i))
        df_table = spark_sql.sql(query_to_execute)
        log.info("Writting")
        log.info(query_to_execute)
        if i > 1:
            df_table_general = df_table_general.union(df_table)
        else:
            df_table_general = df_table
    dim='resultado'
    path_to = path.replace("{dim}", dim)
    write_to_hdfs(df_table_general, path_to, True)

    ## tantas veces como niveles  - producto
    query_prd = "select cod_ren_producto, prod.cod_marca \
        from bi_corp_common.dim_ren_productosctrldn \
        inner join bi_corp_staging.param_marcas_producto_general prod on cod_ren_producto_niv_{lvl}= prod.cod_nodo  \
        where prod.cod_nivel='{lvl}'"

    for i in range(1, int(max_level)):
        query_to_execute_prd = query_prd.replace("{lvl}", str(i))
        df_table_prd = spark_sql.sql(query_to_execute_prd)
        log.info("Writting")
        log.info(query_to_execute_prd)
        if i > 1:
            df_table_general_prod = df_table_general_prod.union(df_table_prd)
        else:
            df_table_general_prod = df_table_prd
    dim = 'producto'
    path_to = path.replace("{dim}", dim)
    write_to_hdfs(df_table_general_prod, path_to, True)

    ## tantas veces como niveles  - Area de Negocio
    query_prd = "select cod_ren_area_negocio, adn.cod_adn_n1, adn.ds_adn_n1, adn.cod_adn_n2,adn.ds_adn_n2, adn.cod_adn_n3,adn.ds_adn_n3, adn.cod_adn_n4, adn.ds_adn_n4 \
        from bi_corp_common.dim_ren_areanegocioctr \
        inner join bi_corp_staging.param_marcas_area_negocio adn on cod_ren_area_negocio_niv_{lvl}= adn.nodo  \
        where adn.nivel='{lvl}'"

    for i in range(1, int(max_level)):
        query_to_execute_prd = query_prd.replace("{lvl}", str(i))
        df_table_prd = spark_sql.sql(query_to_execute_prd)
        log.info("Writting")
        log.info(query_to_execute_prd)
        if i > 1:
            df_table_general_prod = df_table_general_prod.union(df_table_prd)
        else:
            df_table_general_prod = df_table_prd
    dim = 'areanegocio'
    path_to = path.replace("{dim}", dim)
    write_to_hdfs(df_table_general_prod, path_to, True)        



def jerarq_calculate(partition_date,max_level):

    spark, spark_sql, spark_session, hadoop = get_contexts()

    ## AVAILABLE CONTEXT
    execute_query(spark_sql,spark,hadoop,partition_date,max_level)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--partition_date", help="partition_date of the last day of month to process")
    parser.add_argument("--max_level", help="",default=14)
    args = parser.parse_args()

    # validate arguments
    if not args.partition_date:
        raise Exception("Missing parameter: --partition_date")
    if not args.max_level:
        raise Exception("Missing parameter: --max_level")
    jerarq_calculate(args.partition_date,args.max_level)