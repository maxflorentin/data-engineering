set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- STAGE3: en base al nominal_cobertura calculado en stage2, se procede a calcular los porcentajes y remanentes.
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
      gc.cod_entidad
    , gc.num_persona
    , gc.id_cto_div
    , gc.orden_contrato
    , gc.imp_deuda
    , gc.apr_sin_mitigar
    , gc.orden_garantia
	, gc.cla_garantia
    , gc.num_garantia
    , gc.tip_cobertur
	, gc.cod_garantia
	, gc.fec_alta
	, gc.fec_bajrelac
	, gc.fec_vcto
	, gc.imp_total_garantia
    , gc.imp_total_garantia_pesos
	, CAST (row_number() OVER(PARTITION BY  gc.num_persona, gc.num_garantia order by gc.orden_contrato asc) as INT) as orden_prelacion
    , gc.nominal_cobertura_actual
	, CAST((gc.nominal_cobertura_actual * 100 / gc.imp_deuda ) as DECIMAL(9,6))  as porc_cobertura_actual
	, CAST((gc.nominal_cobertura_actual * 100 / gc.imp_total_garantia_pesos ) as DECIMAL(9,6)) as porc_reparto
	, CAST(NVL( ini.nominal_cobertura_actual, gc.nominal_cobertura_actual) as DECIMAL(19,4)) as nominal_cobertura_inicial
	, CAST(NVL( ini.porc_cobertura_actual, (gc.nominal_cobertura_actual * 100 / gc.imp_deuda )) as DECIMAL(9,6)) as porc_cobertura_inicial
    , CAST((gc.imp_deuda -
        (NVL(SUM(gc.nominal_cobertura_actual) OVER (PARTITION BY gc.num_persona,gc.id_cto_div ORDER BY gc.orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
      )  as DECIMAL(19,4)) as imp_deuda_remanente
    , CAST((gc.imp_total_garantia_pesos -
        (NVL(SUM(gc.nominal_cobertura_actual) OVER (PARTITION BY gc.num_persona, gc.num_garantia ORDER BY gc.orden_contrato,gc.orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
      ) as DECIMAL(19,4)) as imp_total_garantia_pesos_remantente
    , gc.stage
	, gc.tip_instrumentacion
	, gc.pignoracion
	, gc.gar_cod_estado
	, gc.gar_cod_divisa
	, gc.tip_aval
    , gc.tip_gara_bdr
	, gc.cod_garantia_bdr
	, gc.id_cto_bdr
    , gc.cod_entidad_bdr
FROM
    bi_corp_bdr.aux_garant_garantias_contratos_div gc
	LEFT OUTER JOIN
		bi_corp_bdr.aux_garant_garantias_contratos_inicial_div ini
		ON (ini.cod_entidad = gc.cod_entidad and ini.num_persona = gc.num_persona and ini.id_cto_div = gc.id_cto_div and ini.num_garantia = gc.num_garantia
		    and ini.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
WHERE
	gc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
	and gc.nominal_cobertura_actual > 0
ORDER BY gc.num_persona, gc.orden_contrato asc, gc.orden_garantia asc;