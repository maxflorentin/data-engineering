from itertools import product
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StringType
import pyspark.sql.functions as psf

spark = SparkSession.builder \
    .enableHiveSupport() \
    .getOrCreate()

sge = []
for i in range(100000):
    sge.append(i)

sge_df = spark.createDataFrame(sge, StringType()) \
    .withColumnRenamed('value', 'id_origen') \
    .withColumn('source', psf.lit('sge'))

aqua_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'

aqua = []
for char in product(aqua_chars, repeat=5):
    aqua.append(''.join(char))

aqua_df = spark.createDataFrame(aqua, StringType()) \
    .withColumnRenamed('value', 'id_origen') \
    .withColumn('source', psf.lit('aqua'))

union_df = sge_df.unionByName(aqua_df)

ordered_df = union_df.withColumn("id_monotono", psf.monotonically_increasing_id())

window = Window.orderBy(psf.col('id_monotono'))

final_df = ordered_df \
    .withColumn('id_destino', psf.row_number().over(window)) \
    .select('source', 'id_origen', 'id_destino', 'id_monotono')

final_df \
    .repartition(5) \
    .write \
    .option("compression", 'gzip') \
    .saveAsTable(name='bi_corp_bdr.map_grupo_economico',
                 format='parquet',
                 mode='overwrite',
                 path='/santander/bi-corp/bdr/map_grupo_economico')
