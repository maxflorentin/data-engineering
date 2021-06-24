spark2-submit \
--master yarn \
--deploy-mode cluster \
--queue root.dataeng \
--num-executors 5 \
--executor-cores 5 \
--executor-memory 8G \
--name STAGING_WAFTC310_Parquetize \
--verbose \
--conf spark.yarn.appMasterEnv.partition_date=2020-03-21 \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/cupones/waftc310/waftc310_schema.json,/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/cupones/waftc310/waftc310.cob \
--py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py \
--jars /aplicaciones/bi/zonda/repositories/guyra/jars/spark-cobol-0.5.6.jar,/aplicaciones/bi/zonda/repositories/guyra/jars/cobol-parser-0.5.6.jar,/aplicaciones/bi/zonda/repositories/guyra/jars/scodec-core_2.11-1.10.3.jar \
/aplicaciones/bi/zonda/repositories/guyra/src/guyra.py waftc310_schema.json