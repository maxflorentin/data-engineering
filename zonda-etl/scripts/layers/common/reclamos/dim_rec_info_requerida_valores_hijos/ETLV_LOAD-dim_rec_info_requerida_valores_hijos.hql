SET mapred.job.queue.name=root.dataeng;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_info_requerida_valores_hijos
SELECT
	cod_rec_entidad ,
	cod_rec_info_reque_p ,
	cod_rec_valor_p ,
	cod_rec_nro_valor_p ,
	cod_rec_info_reque_h ,
	cod_rec_valor_h ,
	cod_rec_nro_valor_h ,
	ds_rec_desc_valor ,
	cod_rec_estado_valor ,
	cod_rec_valor_info_h ,
	cod_rec_valor_info_p ,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM
	(
	SELECT
		cod_entidad cod_rec_entidad , cod_info_reque_p cod_rec_info_reque_p , cod_valor_p cod_rec_valor_p , nro_valor_p cod_rec_nro_valor_p , cod_info_reque_h cod_rec_info_reque_h , cod_valor_h cod_rec_valor_h , nro_valor_h cod_rec_nro_valor_h , desc_valor ds_rec_desc_valor , est_valor cod_rec_estado_valor , cod_valor_info_h cod_rec_valor_info_h , cod_valor_info_p cod_rec_valor_info_p , max(partition_date) AS partition_date
	FROM
		bi_corp_staging.rio56_info_requerida_valores_hijos
	GROUP BY
		cod_entidad , cod_info_reque_p , cod_valor_p , nro_valor_p , cod_info_reque_h , cod_valor_h , nro_valor_h , desc_valor , est_valor , cod_valor_info_h , cod_valor_info_p ) a;