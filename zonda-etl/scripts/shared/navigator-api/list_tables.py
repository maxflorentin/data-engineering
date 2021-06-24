from pyspark.sql import SparkSession, functions as f
import sys

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

db = sys.argv[1]

spark.sql("USE {db}".format(db=db))
db_table_list = spark.sql("SHOW TABLES").select("database", "tableName").where("not isTemporary")
db_table_list = db_table_list.withColumnRenamed("database", "database_name").withColumnRenamed("tableName",
                                                                                               "table_name")
tables_list = [str(row.table_name) for row in db_table_list.select('table_name').collect()]
table_created_results = spark.sql("SELECT created_at, table_name FROM bi_corp_staging.cloudera_logs_tables where partition_date=\"notFoundValue\"")
for table in tables_list:
    try:
        table_created_info = spark \
            .sql("desc formatted {db}.{tb}".format(db=db, tb=table)) \
            .select("data_type") \
            .where("col_name = 'Created Time'") \
            .withColumn("table_name", f.lit(table)) \
            .withColumnRenamed("data_type", "created_at")
    except Exception:
        table_created_info = spark.sql("SELECT \"{table}\" as table_name, \"Fri Dec 31 23:59:59 ART 9999\" as created_at".format(table=str(table)))
    table_created_results = table_created_results.unionByName(table_created_info)

db_table_list.join(table_created_results, ['table_name'], 'inner').show()
db_table_list = db_table_list \
    .join(table_created_results, ['table_name'], 'inner') \
    .withColumn('created_at',
                f.to_date(
                    f.concat_ws('-',
                                f.split('created_at', ' ')[5],
                                f.split('created_at', ' ')[1],
                                f.split('created_at', ' ')[2]
                                ),
                    'yyyy-MMM-dd'
                ),
                )

results = db_table_list.select("database_name", "table_name", "created_at")

results = results.distinct()
results = results.withColumn('partition_date', f.current_date())


results \
    .repartition(1) \
    .write \
    .format("csv") \
    .mode("Append") \
    .partitionBy('partition_date') \
    .option("header", "false") \
    .option("delimiter", ",") \
    .save("/santander/bi-corp/staging/cloudera/cloudera_logs_tables")

spark.sql("msck repair table bi_corp_staging.cloudera_logs_tables")
