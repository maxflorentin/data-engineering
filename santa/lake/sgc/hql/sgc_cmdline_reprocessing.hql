'MANUAL LOAD SGC' (partition_date = '2020-06-07')

## TIPO_GESTION 
> 'DONE' ('2020-06-07') 
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.TIPO_GESTION" --target-dir  '/santander/bi-corp/landing/rio56/dim/tipo_gestion/partition_date=2020-06-07' --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/tipo_gestion/tipo_gestion_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_TIPO_GESTION_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py tipo_gestion_schema.json
*/


## TIPO_MEDIOS 
> 'DONE': '2020-05-13' ? ('2020-06-05' , '2020-06-06' , '2020-06-07' , '2020-06-08')
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.TIPO_MEDIOS" --where "to_char(FEC_MODF_MED ,'YYYY-MM-DD') ='2020-05-13'" --target-dir  '/santander/bi-corp/landing/rio56/dim/tipo_medios/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/tipo_medios/tipo_medios_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_TIPO_MEDIOS_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py tipo_medios_schema.json
*/


## CLASIF_CRM 
> 'DONE': '2020-06-07' ?  ('2020-05-10','2020-05-11','2020-05-12','2020-05-13','2020-05-14','2020-05-15','2020-05-16','2020-05-17','2020-05-18'
						  ,'2020-05-19','2020-05-20','2020-05-21','2020-05-22','2020-05-23','2020-05-24','2020-05-25','2020-05-26','2020-05-27'
						  ,'2020-05-28','2020-05-29','2020-05-30','2020-05-31','2020-06-01','2020-06-02','2020-06-03','2020-06-04','2020-06-05'
						  ,'2020-06-06','2020-06-08')
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.CLASIF_CRM" --target-dir  '/santander/bi-corp/landing/rio56/dim/clasif_crm/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/clasif_crm/clasif_crm_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_CLASIF_CRM_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py clasif_crm_schema.json
*/


## GC_CONCEPTOS_BCRA
> 'DONE': '2020-05-13'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_BCRA" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_bcra/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_bcra/gc_conceptos_bcra_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_BCRA_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_bcra_schema.json
*/


## PRODUCTO
> 'DONE': '2020-05-13'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.PRODUCTO" --target-dir  '/santander/bi-corp/landing/rio56/dim/producto/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/producto/producto_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_PRODUCTO_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py producto_schema.json
*/


## GC_CONCEPTOS_SAC
> 'DONE': '2020-05-13'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_SAC" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_sac/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_sac/gc_conceptos_sac_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_SAC_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_sac_schema.json
*/


## GC_CONCEPTOS_ESPANA
> 'DONE': '2020-05-13'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_ESPANA" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_espana/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_espana/gc_conceptos_espana_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_ESPANA_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_espana_schema.json
*/

## SUBPRODUCTO
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.SUBPRODUCTO" --target-dir  '/santander/bi-corp/landing/rio56/dim/subproducto/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --hive-drop-import-delims --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/subproducto/subproducto_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_SUBPRODUCTO_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py subproducto_schema.json
*/


## INFO_REQUERIDA_VALORES_HIJOS
> 'DONE': '2020-05-13'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.INFO_REQUERIDA_VALORES_HIJOS" --target-dir  '/santander/bi-corp/landing/rio56/dim/info_requerida_valores_hijos/partition_date=2020-05-13'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-05-13 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/info_requerida_valores_hijos/info_requerida_valores_hijos_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_INFO_REQUERIDA_VALORES_HIJOS_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py info_requerida_valores_hijos_schema.json
*/


## CIRCUITO
> 'DONE': '2020-05-13' (2020-05-14,2020-05-15,2020-05-16,2020-05-20,2020-05-21,2020-05-22,2020-05-27,2020-05-28,2020-05-29
						,2020-05-31,2020-06-01,2020-06-02,2020-06-03,2020-06-04,2020-06-05,2020-06-08)
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.CIRCUITO" --where "to_char(FEC_MODF_CIRC ,'YYYY-MM-DD') <='2020-06-25'" --target-dir  '/santander/bi-corp/landing/rio56/dim/circuito/partition_date=2020-06-25'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose



sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI'  --query "SELECT COD_ENTIDAD, IDE_CIRCUITO, COD_CIRCUITO, FEC_VIG_DDE_CIRC, FEC_VIG_HTA_CIRC, ,REGEXP_REPLACE(DESC_CIRC, '(['||chr(10)||chr(11)||chr(13)||chr(9)||']+) > ') DESC_CIRC , REGEXP_REPLACE(DESC_DETALL_CIRC, '(['||chr(10)||chr(11)||chr(13)||chr(9)||']+) > ') DESC_DETALL_CIRC , TPO_GESTION, COD_PROD, COD_SUBPROD, COD_CPTO, COD_SUBCPTO, TMP_ASIGN_ESPECIAL, TMP_PEDIDO_INFO, TMP_CIRC, INDI_GEST_PEND, INDI_MAIL_DEMORA, INDI_CIERR_AUTM, INDI_AUTORIZA_ITEM, COD_RECIBO, EST_CIRC, USER_ALT_CIRC, FEC_ALT_CIRC, USER_MODF_CIRC, FEC_MODF_CIRC, INDI_MODIF_DATOS, INDI_RECEP_RESP, INDI_SUCURSALES, INDI_DEJAR_PEND, COD_TPO_RESOL, UND_TIEMPO, TPO_VIS_GEST, INDI_SUMA_TMP_AUTZ, COD_CONFORME, INDI_JERARQUIA_AUTZ, INDI_SECUENCIA_AUTZ, INDI_ASIG_PARALELO, INDI_AUTORIZ_DEVOL, INDI_MODF_FEC, CIRCUITO_PREDECESOR, INDI_CRIT, INDI_INFO_MULTIVALUADA, INDI_ENVIAR_MAIL_RECEP, INDI_ENVIAR_SMS_RECEP, INDI_ENVIAR_MAIL_RESP, INDI_ENVIAR_SMS_RESP, INDI_ENVIAR_MAIL_DEMORA, COD_MODELO_MSJ, ID_CLASIF_SELECT, ID_PARRAFO_CUERPO_MAIL, INDI_USO_MONTO, INDI_ENVIAR_CARTA_RESP, COD_COMPROBANTE_CLIENTE, INDI_ENVIAR_MAIL_PROV, INDI_REAPERTURA, LONG_COMENTARIO_RECEP, LONG_COMENTARIO_CLIENTE, AVISO_VENC_GEST, INDI_ENVIAR_MAIL_NO_AUTZ, COD_ENTIDAD_AFECT FROM GEC01.CIRCUITO WHERE to_char(FEC_MODF_CIRC ,'YYYY-MM-DD') <= '2020-06-24' and \$CONDITIONS " --map-column-java "DESC_CIRC=String,DESC_DETALL_CIRC=String" --target-dir  '/santander/bi-corp/landing/rio56/dim/circuito/partition_date=2020-06-24'  --num-mappers 1  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --query "SELECT COD_ENTIDAD, IDE_CIRCUITO, COD_CIRCUITO, FEC_VIG_DDE_CIRC, FEC_VIG_HTA_CIRC, DESC_CIRC,  REGEXP_REPLACE(DESC_DETALL_CIRC, '(['||chr(10)||chr(11)||chr(13)||chr(9)||']+) > ') DESC_DETALL_CIRC, TPO_GESTION, COD_PROD, COD_SUBPROD, COD_CPTO, COD_SUBCPTO, TMP_ASIGN_ESPECIAL, TMP_PEDIDO_INFO, TMP_CIRC, INDI_GEST_PEND, INDI_MAIL_DEMORA, INDI_CIERR_AUTM, INDI_AUTORIZA_ITEM, COD_RECIBO, EST_CIRC, USER_ALT_CIRC, FEC_ALT_CIRC, USER_MODF_CIRC, FEC_MODF_CIRC, INDI_MODIF_DATOS, INDI_RECEP_RESP, INDI_SUCURSALES, INDI_DEJAR_PEND, COD_TPO_RESOL, UND_TIEMPO, TPO_VIS_GEST, INDI_SUMA_TMP_AUTZ, COD_CONFORME, INDI_JERARQUIA_AUTZ, INDI_SECUENCIA_AUTZ, INDI_ASIG_PARALELO, INDI_AUTORIZ_DEVOL, INDI_MODF_FEC, CIRCUITO_PREDECESOR, INDI_CRIT, INDI_INFO_MULTIVALUADA, INDI_ENVIAR_MAIL_RECEP, INDI_ENVIAR_SMS_RECEP, INDI_ENVIAR_MAIL_RESP, INDI_ENVIAR_SMS_RESP, INDI_ENVIAR_MAIL_DEMORA, COD_MODELO_MSJ, ID_CLASIF_SELECT, ID_PARRAFO_CUERPO_MAIL, INDI_USO_MONTO, INDI_ENVIAR_CARTA_RESP, COD_COMPROBANTE_CLIENTE, INDI_ENVIAR_MAIL_PROV, INDI_REAPERTURA, LONG_COMENTARIO_RECEP, LONG_COMENTARIO_CLIENTE, AVISO_VENC_GEST, INDI_ENVIAR_MAIL_NO_AUTZ, COD_ENTIDAD_AFECT FROM GEC01.CIRCUITO WHERE to_char(FEC_MODF_CIRC ,'YYYY-MM-DD') <= '2020-06-23' and \$CONDITIONS "  --target-dir  '/santander/bi-corp/landing/rio56/dim/circuito/partition_date=2020-06-23'  -m 1  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^'  --hive-drop-import-delims --map-column-java DESC_DETALL_CIRC=String --escaped-by '^' --delete-target-dir  --verbose




spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-23 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/circuito/circuito_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_CIRCUITO_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py circuito_schema.json
*/


## CONCEPTO
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.CONCEPTO" --target-dir  '/santander/bi-corp/landing/rio56/dim/concepto/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/concepto/concepto_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_CONCEPTO_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py concepto_schema.json
*/


## GC_CONCEPTOS_ESPANA_CIRC
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_ESPANA_CIRC" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_espana_circ/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_espana_circ/gc_conceptos_espana_circ_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_ESPANA_CIRC_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_espana_circ_schema.json
*/


## SECTORES
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.SECTORES" --target-dir  '/santander/bi-corp/landing/rio56/dim/sectores/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/sectores/sectores_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_SECTORES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py sectores_schema.json
*/


## GC_CONCEPTOS_BCRA_CIRC
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_BCRA_CIRC" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_bcra_circ/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_bcra_circ/gc_conceptos_bcra_circ_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_BCRA_CIRC_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_bcra_circ_schema.json
*/


## SUBCONCEPTO
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.SUBCONCEPTO" --target-dir  '/santander/bi-corp/landing/rio56/dim/subconcepto/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/subconcepto/subconcepto_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_SUBCONCEPTO_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py subconcepto_schema.json
*/

## INFO_REQUERIDA
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.INFO_REQUERIDA" --target-dir  '/santander/bi-corp/landing/rio56/dim/info_requerida/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/info_requerida/info_requerida_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_INFO_REQUERIDA_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py info_requerida_schema.json
*/


## GC_CONCEPTOS_SAC_CIRC
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_CONCEPTOS_SAC_CIRC" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_conceptos_sac_circ/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_conceptos_sac_circ/gc_conceptos_sac_circ_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_CONCEPTOS_SAC_CIRC_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_conceptos_sac_circ_schema.json
*/


## INFO_REQUERIDA_VALORES
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.INFO_REQUERIDA_VALORES" --target-dir  '/santander/bi-corp/landing/rio56/dim/info_requerida_valores/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/info_requerida_valores/info_requerida_valores_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_INFO_REQUERIDA_VALORES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py info_requerida_valores_schema.json
*/


## GC_EMPRESAS
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_EMPRESAS" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_empresas/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_empresas/gc_empresas_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_EMPRESAS_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_empresas_schema.json
*/


## CIRC_INFO_REQUE
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.CIRC_INFO_REQUE" --target-dir  '/santander/bi-corp/landing/rio56/dim/circ_info_reque/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/circ_info_reque/circ_info_reque_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_CIRC_INFO_REQUE_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py circ_info_reque_schema.json
*/


## GC_EMPRESA_GESTIONES
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_EMPRESA_GESTIONES" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_empresa_gestiones/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_empresa_gestiones/gc_empresa_gestiones_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_EMPRESA_GESTIONES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_empresa_gestiones_schema.json
*/


## GC_INDIVIDUO_GESTIONES
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_INDIVIDUO_GESTIONES" --target-dir  '/santander/bi-corp/landing/rio56/dim/gc_individuo_gestiones/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/gc_individuo_gestiones/gc_individuo_gestiones_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_INDIVIDUO_GESTIONES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_individuo_gestiones_schema.json
*/


## GC_GESTIONES
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_GESTIONES" --target-dir  '/santander/bi-corp/landing/rio56/fact/gc_gestiones/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/fact/gc_gestiones/gc_gestiones_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_GESTIONES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_gestiones_schema.json
*/


## GC_INDIVIDUOS
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.GC_INDIVIDUOS" --target-dir  '/santander/bi-corp/landing/rio56/fact/gc_individuos/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/fact/gc_individuos/gc_individuos_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GC_INDIVIDUOS_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gc_individuos_schema.json
*/


## INFORMACION_ADJUNTA
> 'DONE': '2020-06-07'
/* cmd
sqoop import --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --table "GEC01.INFORMACION_ADJUNTA" --target-dir  '/santander/bi-corp/landing/rio56/dim/informacion_adjunta/partition_date=2020-06-07'  --num-mappers 6  --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --delete-target-dir --split-by "IDE_GESTION_SECTOR"  --verbose

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2020-06-07 --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/dim/informacion_adjunta/informacion_adjunta_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_INFORMACION_ADJUNTA_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py informacion_adjunta_schema.json
*/













