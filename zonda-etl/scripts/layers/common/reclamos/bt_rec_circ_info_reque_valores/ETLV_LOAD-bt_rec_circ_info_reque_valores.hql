"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.bt_rec_circ_info_reque_valores
	PARTITION(partition_date)
SELECT
	cod_entidad cod_rec_entidad ,
	cod_info_reque cod_rec_info_reque ,
	cod_valor cod_rec_valor ,
	nro_valor cod_rec_nro_valor ,
	desc_valor ds_rec_desc_valor ,
	est_valor cod_rec_estado_valor ,
	TO_DATE(fec_modf_valor) partition_date 
FROM
	bi_corp_staging.rio56_info_requerida_valores 
WHERE
	TO_DATE(fec_modf_valor) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
"