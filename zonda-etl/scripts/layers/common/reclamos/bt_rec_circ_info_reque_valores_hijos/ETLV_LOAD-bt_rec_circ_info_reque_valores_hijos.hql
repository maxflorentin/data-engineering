"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.bt_rec_circ_info_reque_valores_hijos
	PARTITION(partition_date)
SELECT 
	cod_entidad cod_rec_entidad ,
	cod_info_reque_p cod_rec_info_reque_p ,
	cod_valor_p cod_rec_valor_p ,
	nro_valor_p cod_rec_nro_valor_p ,
	cod_info_reque_h cod_rec_info_reque_h ,
	cod_valor_h cod_rec_valor_h ,
	nro_valor_h cod_rec_nro_valor_h ,
	desc_valor ds_rec_desc_valor ,
	est_valor cod_rec_estado_valor ,
	cod_valor_info_h cod_rec_valor_info_h ,
	cod_valor_info_p cod_rec_valor_info_p ,
	TO_DATE(fec_modf_valor) partition_date 
FROM
	bi_corp_staging.rio56_info_requerida_valores_hijos
WHERE
	TO_DATE(fec_modf_valor) = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
"