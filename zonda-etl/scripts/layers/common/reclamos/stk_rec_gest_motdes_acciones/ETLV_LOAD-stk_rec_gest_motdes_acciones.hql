SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_rec_gest_motdes_acciones
PARTITION(partition_date)
SELECT
	TRIM(a.id_gestion) cod_rec_gestion ,
	TRIM(a.id_accion) cod_rec_accion ,
	case when length(trim(a.valor_accion))=0 or trim(a.valor_accion)='null' then null else trim(a.valor_accion) end AS ds_rec_valor_accion ,
	case when length(trim(a.cod_grupo))=0 or trim(a.cod_grupo)='null'  then null else trim(a.cod_grupo) end AS cod_rec_grupo ,
	TRIM(a.ind_origen) cod_rec_origen ,
	a.partition_date
FROM bi_corp_staging.rio187_gest_motdes_acciones a
WHERE a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_gest_motdes_acciones', dag_id='LOAD_CMN_Cosmos') }}';