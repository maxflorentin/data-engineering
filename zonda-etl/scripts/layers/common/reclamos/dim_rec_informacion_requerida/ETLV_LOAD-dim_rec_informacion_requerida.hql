set mapred.job.queue.name=root.dataeng;

INSERT overwrite TABLE bi_corp_common.dim_rec_informacion_requerida
SELECT 
			cod_rec_entidad,
			cod_rec_circuito,
			cod_rec_info_gpo,
			cod_rec_info_reque,
			flag_rec_info_oblig,
			cod_estado_circ_info_reque,
			int_rec_orden_info,
			cod_rec_etapa,
			int_rec_orden_etapa,
			ds_rec_info_reque,
			cod_rec_tipo_data,
			int_rec_long_info_reque,
			fc_rec_cant_dec_info_reque,
			int_rec_rang_dde_info_reque,
			int_rec_rang_hta_info_reque,
			dt_rec_dde_info_reque,
			dt_rec_hta_info_reque,
			cod_rec_esado_info_reque,
			cod_rec_tipo_campo,
			cod_rec_funcion_asco,
			cod_rec_info_relac,
			int_rec_tipo_info_especial,
			cod_rec_sector_owner,
			ds_rec_texto_ayuda,
			cod_rec_info_condic,
			cod_rec_valor_condic,
			cod_rec_valor,
			cod_rec_nro_valor,
			ds_rec_desc_valor,
			cod_rec_estado_valor,
			'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}' partition_date
FROM 
		(
		SELECT
			trim(circ_info_reque.cod_entidad) cod_rec_entidad,
			trim(circ_info_reque.ide_circuito) cod_rec_circuito,
			trim(circ_info_reque.indi_info_gpo) cod_rec_info_gpo,
			trim(circ_info_reque.cod_info_reque) cod_rec_info_reque,
			case when circ_info_reque.indi_info_oblig='S' then 1 else 0 end flag_rec_info_oblig,
			trim(circ_info_reque.est_circ_info_reque) cod_estado_circ_info_reque,
			trim(circ_info_reque.orden_info) int_rec_orden_info,
			trim(circ_info_reque.cod_etapa) cod_rec_etapa,
			trim(circ_info_reque.orden_etapa) int_rec_orden_etapa,
			trim(info_requerida.desc_info_reque) AS ds_rec_info_reque,
			trim(info_requerida.cod_tpo_dat) AS cod_rec_tipo_data,
			trim(info_requerida.long_info_reque) AS int_rec_long_info_reque,
			trim(info_requerida.cant_dec_info_reque) AS fc_rec_cant_dec_info_reque,
			trim(info_requerida.rang_dde_info_reque) AS int_rec_rang_dde_info_reque,
			trim(info_requerida.rang_hta_info_reque) AS int_rec_rang_hta_info_reque,
			trim(info_requerida.fec_dde_info_reque) AS dt_rec_dde_info_reque,
			trim(info_requerida.fec_hta_info_reque) AS dt_rec_hta_info_reque,
			trim(info_requerida.est_info_reque) AS cod_rec_esado_info_reque,
			trim(info_requerida.cod_tpo_campo) AS cod_rec_tipo_campo,
			trim(info_requerida.cod_funcion_asoc) AS cod_rec_funcion_asco,
			trim(info_requerida.cod_info_relac) AS cod_rec_info_relac,
			trim(info_requerida.tpo_info_especial) AS int_rec_tipo_info_especial,
			trim(info_requerida.sector_owner) AS cod_rec_sector_owner,
			trim(info_requerida.texto_ayuda) AS ds_rec_texto_ayuda,
			trim(info_requerida.cod_info_condic) AS cod_rec_info_condic,
			trim(info_requerida.cod_valor_condic) AS cod_rec_valor_condic,
			trim(info_requerida_valores.cod_valor) cod_rec_valor,
			trim(info_requerida_valores.nro_valor) cod_rec_nro_valor,
			trim(info_requerida_valores.desc_valor) ds_rec_desc_valor,
			trim(info_requerida_valores.est_valor) cod_rec_estado_valor,
			max(circ_info_reque.partition_date) AS partition_date
		FROM
			bi_corp_staging.rio56_circ_info_reque circ_info_reque
		LEFT JOIN bi_corp_staging.rio56_info_requerida info_requerida ON
			circ_info_reque.cod_info_reque = info_requerida.cod_info_reque
		LEFT JOIN bi_corp_staging.rio56_info_requerida_valores info_requerida_valores ON
			circ_info_reque.cod_info_reque = info_requerida_valores.cod_info_reque
		GROUP BY
			trim(circ_info_reque.cod_entidad),
			trim(circ_info_reque.ide_circuito),
			trim(circ_info_reque.indi_info_gpo),
			trim(circ_info_reque.cod_info_reque),
			case when circ_info_reque.indi_info_oblig='S' then 1 else 0 end,
			trim(circ_info_reque.est_circ_info_reque),
			trim(circ_info_reque.orden_info),
			trim(circ_info_reque.cod_etapa),
			trim(circ_info_reque.orden_etapa),
			trim(info_requerida.desc_info_reque),
			trim(info_requerida.cod_tpo_dat),
			trim(info_requerida.long_info_reque),
			trim(info_requerida.cant_dec_info_reque),
			trim(info_requerida.rang_dde_info_reque),
			trim(info_requerida.rang_hta_info_reque),
			trim(info_requerida.fec_dde_info_reque),
			trim(info_requerida.fec_hta_info_reque),
			trim(info_requerida.est_info_reque),
			trim(info_requerida.cod_tpo_campo),
			trim(info_requerida.cod_funcion_asoc),
			trim(info_requerida.cod_info_relac),
			trim(info_requerida.tpo_info_especial),
			trim(info_requerida.sector_owner),
			trim(info_requerida.texto_ayuda),
			trim(info_requerida.cod_info_condic),
			trim(info_requerida.cod_valor_condic),
			trim(info_requerida_valores.cod_valor),
			trim(info_requerida_valores.nro_valor),
			trim(info_requerida_valores.desc_valor),
			trim(info_requerida_valores.est_valor)
		) A;