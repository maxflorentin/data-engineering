
-- MS0_DM_DWH_CTA_RESULTADOS_CTR
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table "BWHCORE.MS0_DM_DWH_CTA_RESULTADOS_CTR" --where "to_char(TIMEST_UMO ,'YYYY-MM-DD') ='2020-07-10'" --target-dir  '/santander/bi-corp/landing/rio157/dim/ms0_dm_dwh_cta_resultados_ctr/partition_date=2020-07-10'  -m 1 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DM_DWH_CTA_RESULTADOS_CTR --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --null-non-string '\\N' --verbose


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-07-10 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/dim/ms0_dm_dwh_cta_resultados_ctr/ms0_dm_dwh_cta_resultados_ctr_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 5 --executor-cores 4 --executor-memory 16G --name STAGING_MS0_DM_DWH_CTA_RESULTADOS_CTR_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dm_dwh_cta_resultados_ctr_schema.json
*/


-- MS0_DM_JE_DWH_ENTIDADES_CTR 
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table "BWHCORE.MS0_DM_JE_DWH_ENTIDADES_CTR" --where "to_char(TIMEST_UMO ,'YYYY-MM-DD') ='2020-07-14'" --target-dir  '/santander/bi-corp/landing/rio157/dim/ms0_dm_je_dwh_entidades_ctr/partition_date=2020-07-14'  -m 1 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DM_JE_DWH_ENTIDADES_CTR --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --null-non-string '\\N' --verbose


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-07-14 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/dim/ms0_dm_je_dwh_entidades_ctr/ms0_dm_je_dwh_entidades_ctr_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 5 --executor-cores 4 --executor-memory 16G --name STAGING_MS0_DM_JE_DWH_ENTIDADES_CTR_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dm_je_dwh_entidades_ctr_schema.json
*/


-- MS0_DM_DWH_PRODUCTOS_CTR 
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table "BWHCORE.MS0_DM_DWH_PRODUCTOS_CTR" --where "to_char(TIMEST_UMO ,'YYYY-MM-DD') ='2020-07-14'" --target-dir  '/santander/bi-corp/landing/rio157/dim/ms0_dm_dwh_productos_ctr/partition_date=2020-07-14'  -m 1 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DM_DWH_PRODUCTOS_CTR --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --null-non-string '\\N' --verbose


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-07-14 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/dim/ms0_dm_dwh_productos_ctr/ms0_dm_dwh_productos_ctr_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 5 --executor-cores 4 --executor-memory 16G --name STAGING_MS0_DM_DWH_PRODUCTOS_CTR_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dm_dwh_productos_ctr_schema.json
*/


-- MS0_DM_DWH_AREA_NEGOCIO_CTR 
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table "BWHCORE.MS0_DM_DWH_AREA_NEGOCIO_CTR" --where "to_char(TIMEST_UMO ,'YYYY-MM-DD') ='2020-07-14'" --target-dir  '/santander/bi-corp/landing/rio157/dim/ms0_dm_dwh_area_negocio_ctr/partition_date=2020-07-14'  -m 1 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DM_DWH_AREA_NEGOCIO_CTR --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --null-non-string '\\N' --verbose


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-07-14 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/dim/ms0_dm_dwh_area_negocio_ctr/ms0_dm_dwh_area_negocio_ctr_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 5 --executor-cores 4 --executor-memory 16G --name STAGING_MS0_DM_DWH_AREA_NEGOCIO_CTR_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dm_dwh_area_negocio_ctr_schema.json
*/


-- MS0_DT_DWH_CLIENTE_RESULT 
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng -Doraoop.import.partitions='PMES_54_202005' --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi'  --table "BWHCORE.MS0_DT_DWH_CLIENTE_RESULT" --where "FEC_DATA = 20200531" --target-dir '/santander/bi-corp/landing/rio157/fact/ms0_dt_dwh_cliente_result/partition_date=2020-05-31'  -m 10 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DT_DWH_CLIENTE_RESULT --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --split-by "COD_PAIS" --null-non-string '\\N' --verbose



spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-31 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/fact/ms0_dt_dwh_cliente_result/ms0_dt_dwh_cliente_result_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 8 --executor-cores 5 --executor-memory 8G --name STAGING_MS0_DT_DWH_CLIENTE_RESULT_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dt_dwh_cliente_result_schema.json
*/


-- MS0_DT_DWH_GENERIC_RESULT 
/*
sqoop import -Dmapreduce.job.queuename=root.dataeng -Doraoop.import.partitions='PMES_54_202005' --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi'  --table "BWHCORE.MS0_DT_DWH_GENERIC_RESULT" --where "FEC_DATA = 20200531" --target-dir '/santander/bi-corp/landing/rio157/fact/ms0_dt_dwh_generic_result/partition_date=2020-05-31'  -m 1 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_DT_DWH_GENERIC_RESULT --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --null-non-string '\\N' --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-31 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio157/fact/ms0_dt_dwh_generic_result/ms0_dt_dwh_generic_result_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 8 --executor-cores 5 --executor-memory 8G --name STAGING_MS0_DT_DWH_GENERIC_RESULT_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py ms0_dt_dwh_generic_result_schema.json
	
*/


sqoop import -Dmapreduce.job.queuename=root.dataeng -Doraoop.import.partitions='PMES_54_202006' --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table "BWHCORE.MS0_FT_DWH_BLCE_RESULT" --where "FEC_DATA= REPLACE('2020-06-30','-','')" --target-dir  '/santander/bi-corp/landing/rio157/fact/ms0_ft_dwh_blce_result/partition_date=2020-06-30'  -m 10 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_FT_DWH_BLCE_RESULT --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --split-by "COD_PAIS" --null-non-string '\\N' --verbose



spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2008-02-28 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/malbgc/fact/bgdtmoh/bgdtmoh_schema.json,/aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/malbgc/fact/bgdtmoh/bgdtmoh.cob --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --jars /aplicaciones/bi/zonda/repositories/guyra/jars/spark-cobol-0.5.6.jar,/aplicaciones/bi/zonda/repositories/guyra/jars/cobol-parser-0.5.6.jar,/aplicaciones/bi/zonda/repositories/guyra/jars/scodec-core_2.11-1.10.3.jar --num-executors 5 --executor-cores 4 --executor-memory 8G --name STAGING_BGDTMOH_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py bgdtmoh_schema.json


RIO60
Clezatua3iat




hdfs dfs -copyFromLocal -f /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/cosmos/upload/Usuarios_20180101.csv /santander/bi-corp/landing/csm/fact/usuarios/partition_date=2018-01-01


spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2018-01-01 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/cosmos/fact/usuarios/usuarios_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 5 --executor-cores 4 --executor-memory 8G --name STAGING_usuarios_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py usuarios_schema.json


---------------------
-- BLCE_RESULT_DIA --
---------------------
sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --query 'SELECT * FROM BWHCORE.MS0_FT_DWH_BLCE_RESULT_DIA PARTITION(PDIA_54_20200731) WHERE FEC_DATA = 20200731 AND $CONDITIONS' --target-dir  '/santander/bi-corp/landing/rio157/fact/ms0_ft_dwh_blce_result_dia/partition_date=2020-07-31'  -m 8 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_FT_DWH_BLCE_RESULT_DIA --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --split-by "FEC_DATA" --verbose

-- OK
sqoop import -Dmapreduce.job.queuename=root.dataeng -Doraoop.import.partitions=PDIA_54_20200731 -- direct --connect 'jdbc:oracle:thin:@dblxmissrv01:7366/RIO157'  --username 'srvcbi' --table BWHCORE.MS0_FT_DWH_BLCE_RESULT_DIA --target-dir  '/santander/bi-corp/landing/rio157/fact/ms0_ft_dwh_blce_result_dia/partition_date=2020-07-31'  -m 8 --bindir /aplicaciones/bi/zonda/sqoop/rio157/MS0_FT_DWH_BLCE_RESULT_DIA --password '21mSC+8PKFrbPfZ'  --delete-target-dir --fields-terminated-by '|' --split-by "FEC_DATA" --verbose





