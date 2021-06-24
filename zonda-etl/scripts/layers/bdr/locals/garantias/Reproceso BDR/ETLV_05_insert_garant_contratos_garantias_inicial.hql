set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_inicial_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT	/*+ MAPJOIN(aux_garant_garantias_contratos_inicial_div) */
     gc.cod_entidad
    ,gc.num_persona
    ,gc.id_cto_div
    ,gc.orden_contrato
    ,gc.imp_deuda
    ,gc.apr_sin_mitigar
    ,gc.orden_garantia
	,gc.cla_garantia
    ,gc.num_garantia
    ,gc.tip_cobertur
	,gc.cod_garantia
	,gc.fec_alta
	,gc.fec_bajrelac
	,gc.fec_vcto
	,gc.imp_total_garantia
    ,gc.imp_total_garantia_pesos
	,gc.orden_prelacion
    ,gc.nominal_cobertura_actual
	,gc.porc_cobertura_actual
	,gc.porc_reparto
	,gc.nominal_cobertura_inicial
	,gc.porc_cobertura_inicial
    ,gc.imp_deuda_remanente
    ,gc.imp_total_garantia_pesos_remantente
    ,gc.stage
	,gc.tip_instrumentacion
	,gc.pignoracion
	,gc.gar_cod_estado
	,gc.gar_cod_divisa
	,gc.tip_aval
FROM
    bi_corp_bdr.aux_garant_garantias_contratos_div gc
	LEFT OUTER JOIN bi_corp_bdr.aux_garant_garantias_contratos_inicial_div ini
		ON ( ini.cod_entidad = gc.cod_entidad and ini.num_persona = gc.num_persona and ini.id_cto_div = gc.id_cto_div and ini.num_garantia = gc.num_garantia)
WHERE
	gc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
	and gc.nominal_cobertura_actual > 0
	and ini.nominal_cobertura_actual is null
ORDER BY gc.num_persona, gc.orden_contrato asc, gc.orden_garantia asc;