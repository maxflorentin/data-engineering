from pyspark import SQLContext, SparkContext
from pyspark.sql import functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import json
import sys

import zonda_trancos_config


def get_contexts():
    spark = SparkContext.getOrCreate()
    spark.setLogLevel("INFO")
    spark_sql = SQLContext.getOrCreate(spark)
    spark_session = SparkSession.builder.enableHiveSupport().getOrCreate()
    hadoop = spark._jvm.org.apache.hadoop
    return spark, spark_sql, spark_session, hadoop


def read_files(spark, sparkSQL, hadoop, configFile):

    # Initialize hadoop tools
    jvm = spark._jvm
    jsc = spark._jsc
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

    print(configFile.location[0])

    if configFile.model == 'transferencias':
        if not fs.exists(hadoop.fs.Path(configFile.location[1])):
            final = sparkSQL.read.format(configFile.extension).option('delimiter', configFile.delimiter).schema(configFile.schema).load(configFile.location[0])
        else:
            online = sparkSQL.read.format(configFile.extension).option('delimiter', configFile.delimiter).schema(configFile.schema).load(configFile.location[0])
            batch = sparkSQL.read.format(configFile.extension).option('delimiter', configFile.delimiter).schema(configFile.schema).load(configFile.location[1])
            final = online.unionAll(batch)
    elif configFile.model == 'errores':
        final = sparkSQL.read.format(configFile.extension).option('delimiter', configFile.delimiter).schema(configFile.schema).load(configFile.location[0])
    final = final.withColumn(configFile.columnDelimiter, f.split(configFile.columnDelimiter, '\\;')). \
        select("*", *(f.col(configFile.columnDelimiter).getItem(i).alias('col{}'.format(i)) for i in range(72))).drop(configFile.columnDelimiter)
    final = final.withColumn('partition_date', f.to_date('fecha', 'yyyy-MM-dd').cast('string')).select('*')
    final = final.withColumn('servicio', f.regexp_replace('servicio', ' ', ''))
    #Reemplazo Valores Nulos
    final = final.na.fill({'partition_date': 'INVALID_DATE'})
    final.show()
    print("read success")
    return final


def get_config_data(spark_sql, config_file_path):
    config_raw = spark_sql.read.option("multiline", "true").text(config_file_path)
    config_raw.printSchema()
    config_raw.show()
    config_raw_row = config_raw.agg(
        f.concat_ws("", f.collect_list(config_raw["value"])).alias("config")).distinct().collect()
    config_raw_str = config_raw_row[0].__getitem__("config")
    return json.loads(config_raw_str)


# Rename Header Columns
def rename_write_df(finalDF, configFile, spark_session):
    dfArray = [finalDF.select('*').where("servicio == '{}'".format(x.servicio)) for x in finalDF.select("servicio").distinct().where("servicio NOT IN ('PIRYP', 'PIRYPLOTE', 'NULL')").collect()]
    renamedDF = []
    for array in dfArray:
        serv = array.select("servicio").distinct().collect()[0]
        if serv.servicio.strip() == 'SAR' or serv.servicio.strip() == 'ENVIOPIRYP' or serv.servicio.strip() == 'PAGOSPIRYP':
            for key in configFile.trancos['especialServices']:
                if key == serv.servicio.strip():
                    for sch in array.schema.names:
                        for kk, v in configFile.trancos['especialServices'][key].items():
                            if sch == kk:
                                array = array.withColumnRenamed(sch, v)
                cols = [c for c in array.columns if c.lower().__contains__('col')]
                array = array.drop(*cols)
            renamedDF.append(array)
        else:
            for x in array.schema.names:
                for k, v in configFile.trancos['tramaHeader'].items():
                    if x == k:
                        array = array.withColumnRenamed(x, v)
            for k in configFile.trancos['service']:
                if k == serv.servicio.strip():
                    for sch in array.schema.names:
                        for kk, v in configFile.trancos['service'][k].items():
                            if sch == kk:
                                array = array.withColumnRenamed(sch, v)
                    cols = [c for c in array.columns if c.lower().__contains__('col')]
                    array = array.drop(*cols)
            renamedDF.append(array)
    c = df_order_column(renamedDF)
    result = unionDF(c, renamedDF)
    result.repartition(configFile.parquet_files).write.mode('append').partitionBy("partition_date", "servicio").parquet(configFile.destination)


def df_order_column(dfs):
    cols = set()
    for df in dfs:
        for x in df.columns:
            cols.add(x)
    cols = sorted(cols)
    return cols


def unionDF(cols, dataframes):
    dfs = {}
    for i, d in enumerate(dataframes):
        new_name = 'df' + str(i)  # New name for the key, the dataframe is the value
        dfs[new_name] = d
        # Loop through all column names. Add the missing columns to the dataframe (with value 0)
        for x in cols:
            if x not in d.columns:
                dfs[new_name] = dfs[new_name].withColumn(x, lit(''))
        dfs[new_name] = dfs[new_name].select(cols)  # Use 'select' to get the columns sorted

    # Now put it al together with a loop (union)
    result = dfs['df0']  # Take the first dataframe, add the others to it
    dfs_to_add = list(dfs)  # List of all the dataframes in the dictionary
    dfs_to_add.remove('df0')  # Remove the first one, because it is already in the result
    for x in dfs_to_add:
        result = result.union(dfs[x])
    result.show()
    return result


def trancos():
    spark, spark_sql, spark_session, hadoop = get_contexts()

    mapFile = get_config_data(spark_sql, sys.argv[1])
    configFile = zonda_trancos_config.ConfigFile(mapFile)
    transfDF = read_files(spark, spark_sql, hadoop, configFile)
    rename_write_df(transfDF, configFile, spark_session)


if __name__ == "__main__":
    trancos()
