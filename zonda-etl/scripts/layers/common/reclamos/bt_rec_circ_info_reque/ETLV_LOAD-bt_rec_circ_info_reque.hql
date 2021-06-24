"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.bt_rec_circ_info_reque
	PARTITION(partition_date)
SELECT
	circ_info_reque.cod_entidad cod_rec_entidad ,
	circ_info_reque.ide_circuito cod_rec_circuito ,
	circ_info_reque.indi_info_gpo flag_rec_info_gpo ,
	circ_info_reque.cod_info_reque cod_rec_info_reque ,
	circ_info_reque.indi_info_oblig flag_rec_info_oblig ,
	circ_info_reque.est_circ_info_reque cod_estado_circ_info_reque ,
	circ_info_reque.orden_info int_rec_orden_info ,
	circ_info_reque.cod_etapa cod_rec_etapa ,
	circ_info_reque.orden_etapa int_rec_orden_etapa ,
	info_requerida.ds_rec_info_reque ,
	info_requerida.cod_rec_tipo_data ,
	info_requerida.int_rec_long_info_reque ,
	info_requerida.fc_rec_cant_dec_info_reque ,
	info_requerida.int_rec_rang_dde_info_reque ,
	info_requerida.int_rec_rang_hta_info_reque ,
	info_requerida.dt_rec_dde_info_reque ,
	info_requerida.dt_rec_hta_info_reque ,
	info_requerida.cod_rec_estado_info_reque ,
	info_requerida.cod_rec_tipo_campo ,
	info_requerida.cod_rec_funcion_asco ,
	info_requerida.cod_rec_info_relac ,
	info_requerida.int_rec_tipo_info_especial ,
	info_requerida.cod_rec_sector_owner ,
	info_requerida.ds_rec_texto_ayuda ,
	info_requerida.cod_rec_info_condic ,
	info_requerida.cod_rec_valor_condic ,
	circ_info_reque.partition_date
FROM
	bi_corp_staging.rio56_circ_info_reque circ_info_reque
LEFT JOIN bi_corp_common.dim_rec_info_requerida info_requerida ON
	circ_info_reque.cod_info_reque = info_requerida.cod_rec_info_reque
WHERE
	circ_info_reque.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
"