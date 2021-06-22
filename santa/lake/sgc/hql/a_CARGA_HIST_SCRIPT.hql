#### SCRIPTS CARGA HISTORIA ####

### SGC GESTIONES ### 

## SQOOP (EJEMPLO 2019): /*

sqoop import -Dmapreduce.job.queuename=root.dataeng --connect 'jdbc:oracle:thin:@DBSORA6:1523/RIO56'  --username 'SRVCBI' --query " SELECT COD_ENTIDAD, IDE_GESTION_SECTOR, IDE_GESTION_NRO, IDE_CIRCUITO, INDI_TIPO_CIRC, INDI_DECISION_COMER, INDI_REPLTEO, INDI_RTA_INMED, INDI_IMPRE_CART, IMP_AUTZ_SOLICITADO, COD_MONE_SOLICITADO, IMP_AUTZ_AUTORIZADO, COD_MONE_AUTORIZADO, IMP_AUTZ_RESOLUCION, COD_MONE_RESOLUCION, TPO_PERS,COD_CRM,REGEXP_REPLACE(COMEN_CLIENTE , '(['||chr(10)||chr(11)||chr(13)||chr(9)||']+)', '> ') COMEN_CLIENTE,IDE_GEST_SECTOR_RELAC, IDE_GEST_NRO_RELAC, FEC_GESTION_ALTA, COD_BANDEJA_ACTUAL, FEC_BANDEJA_ACTUAL, COD_SECTOR_ACTUAL, INIC_USER_ALTA, INDI_MAIL, INDI_PRIORIDAD, FEC_MAX_RESOL, COD_CONFORME, COD_USER_ACTUAL,COD_GRUPO_EMPRESA, COD_TIPO_FAVORABILIDAD, FEC_AVISO_VENC FROM GEC01.GESTIONES WHERE to_char(FEC_GESTION_ALTA ,'YYYY-MM-DD') >= '2019-01-01' AND to_char(FEC_GESTION_ALTA ,'YYYY-MM-DD') <= '2019-12-31' and \$CONDITIONS " --target-dir  '/santander/bi-corp/landing/rio56/fact/gestiones/partition_date=2019-hist' -m 12 --split-by "IDE_GESTION_NRO" --password 'F5byf8ZRxc7_mezy' --fields-terminated-by '^' --hive-drop-import-delims --map-column-java COMEN_CLIENTE=String --escaped-by '^' --delete-target-dir --verbose

*/

## SPARK SUBMIT (EJEMPLO 2019): /*

spark2-submit --master yarn --conf spark.yarn.appMasterEnv.partition_date=2019-hist --conf spark.sql.sources.partitionOverwriteMode=dynamic --files /aplicaciones/bi/zonda/repositories/zonda-etl/scripts/layers/staging/rio56/fact/gestiones/gestiones_schema.json --py-files /aplicaciones/bi/zonda/repositories/guyra/src/config_file.py --num-executors 4 --executor-cores 4 --executor-memory 8G --name STAGING_GESTIONES_Parquetize --verbose --queue root.dataeng --deploy-mode cluster /aplicaciones/bi/zonda/repositories/guyra/src/guyra.py gestiones_schema.json

*/

# HIVE TEMP TABLE (EJEMPLO 2019):

	-- DROP TABLE gestiones_hist_2019
CREATE TEMPORARY TABLE gestiones_hist_2019 AS
	SELECT  cod_entidad, ide_gestion_sector, ide_gestion_nro
	, ide_circuito, indi_tipo_circ, indi_decision_comer
	, indi_replteo, indi_rta_inmed, indi_impre_cart
	, imp_autz_solicitado, cod_mone_solicitado
	, imp_autz_autorizado, cod_mone_autorizado
	, imp_autz_resolucion, cod_mone_resolucion
	, tpo_pers, cod_crm, comen_cliente, ide_gest_sector_relac
	, ide_gest_nro_relac, fec_gestion_alta, cod_bandeja_actual
	, fec_bandeja_actual, cod_sector_actual, inic_user_alta
	, indi_mail, indi_prioridad, fec_max_resol, cod_conforme
	, cod_user_actual, cod_grupo_empresa, cod_tipo_favorabilidad
	, fec_aviso_venc, TO_DATE(fec_gestion_alta) partition_date
	FROM bi_corp_staging.rio56_gestiones
	WHERE TO_DATE(fec_gestion_alta) >= '2019-01-03' 
		AND TO_DATE(fec_gestion_alta) <= '2019-12-31'
	ORDER BY TO_DATE(fec_gestion_alta) ASC;

	
# HIVE INSERT OVERWRITE (EJEMPLO 2019):

INSERT
	OVERWRITE TABLE bi_corp_staging.rio56_gestiones
	PARTITION(partition_date)
SELECT * FROM gestiones_hist_2019;


# HIVE DROP PARTITION HIST (EJEMPLO 2019):

ALTER TABLE bi_corp_staging.rio56_gestiones 
DROP IF EXISTS PARTITION(partition_date = '2019-hist')


# HDFS REMOVE partition_date=2019-hist

hdfs dfs -rm -r /santander/bi-corp/staging/rio56/fact/gestiones/partition_date=2019-hist

#### DONE! #### 