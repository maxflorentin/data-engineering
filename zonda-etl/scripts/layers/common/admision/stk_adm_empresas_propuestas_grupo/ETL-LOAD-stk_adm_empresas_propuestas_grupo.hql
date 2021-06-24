set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_propuestas_grupo
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(nro_pglobal as bigint) as cod_adm_nro_pglobal,
    cast(secuencia_pglobal as decimal(1, 0)) as cod_adm_secuencia_pglobal,
    penumgru as cod_adm_penumgru,
    cast(lim_prop as decimal(16, 2)) as fc_adm_lim_prop,
    cast(lim_actual as decimal(16, 2)) as fc_adm_lim_actual,
    estado_prop as cod_adm_estado_prop,
    date_format(fec_est_prop,'yyyy-MM-dd') as dt_adm_est_prop,
    date_format(fec_vto_prop,'yyyy-MM-dd') as dt_adm_vto_prop,
    cast(roa as decimal(4, 1)) as dec_adm_roa,
    cast(rorac_cc as decimal(4, 1)) as dec_adm_rorac_cc,
    posicion_otras_uni as ds_adm_posicion_otras_uni,
    garantias as ds_adm_garantias,
    peusualt as cod_adm_peusualt,
    date_format(pefecalt,'yyyy-MM-dd') as dt_adm_pefecalt,
    peusumod as cod_adm_peusumod,
    date_format(pefecmod,'yyyy-MM-dd') as dt_adm_pefecmod,
    origen as cod_adm_origen,
    tpo_pglobal as flag_adm_tpo_pglobal,
    resp_actual as ds_adm_resp_actual,
    recomendacion as ds_adm_recomendacion,
    cast(id_rentabilidad as bigint) as id_adm_rentabilidad,
    cast(nro_acta as bigint) as cod_adm_nro_acta,
    date_format(fecha_resolucion,'yyyy-MM-dd') as dt_adm_resolucion,
    cast(lim_bsch as decimal(16, 2)) as fc_adm_lim_bsch,
    cast(dispuesto_bsch as decimal(16, 2)) as fc_adm_dispuesto_bsch,
    cast(id_acta as bigint) as id_adm_acta,
    cast(valoracion_operaciones as decimal(3, 2)) as dec_adm_valoracion_operaciones,
    recomendacion_comercial as ds_adm_recomendacion_comercial,
    cast(lim_actual_om as decimal(16, 2)) as fc_adm_lim_actual_om,
    cast(lim_prop_om as decimal(16, 2)) as fc_adm_lim_prop_om,
    cast(lim_bsch_om as decimal(16, 2 )) as fc_adm_lim_bsch_om,
    cast(dispuesto_bsch_om as decimal(16, 2)) as fc_adm_dispuesto_bsch_om,
    comite_esp as flag_adm_comite_esp,
    date_format(fecha_env_esp,'yyyy-MM-dd') as dt_adm_env_esp,
    date_format(fecha_recep_esp,'yyyy-MM-dd') as dt_adm_recep_esp,
    cod_estado_accion as cod_adm_estado_accion,
    cod_estado_origen as cod_adm_estado_origen,
    fec_carga as ts_adm_carga,
    cod_usuario_net as cod_adm_usuario_net,
    fec_alta_prop as ts_adm_alta_prop,
    cast(id_comite as decimal(2, 0)) as id_adm_comite,
    date_format(fec_comite,'yyyy-MM-dd') as dt_adm_comite,
    rorac_con_compensacion as ds_adm_rorac_con_compensacion,
    rorac_sin_compensacion as ds_adm_rorac_sin_compensacion,
    motivo as ds_adm_motivo,
    fecha_cotizacion as ts_adm_cotizacion,
    moneda_lim_prop as cod_adm_moneda_lim_prop,
    mar_comite_enviado as flag_adm_comite_enviado,
    mar_balance_enviado as flag_adm_balance_enviado,
    mar_baja_enviado as flag_adm_baja_enviado,
    mar_confirming_enviado as flag_adm_confirming_enviado,
    date_format(fec_proximo_envio,'yyyy-MM-dd') as dt_adm_proximo_envio,
    date_format(fec_alerta_riesgo_bma,'yyyy-MM-dd') as dt_adm_alerta_riesgo_bma
from bi_corp_staging.sge_prop_global
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';