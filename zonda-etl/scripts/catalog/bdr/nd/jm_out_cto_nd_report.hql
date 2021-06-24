"set mapred.job.queue.name=root.dataeng;SELECT substr(g.data_date_part, 1,6) as periodo,
concat_ws('/',substr(R9750_feoperac, 5,2), substr(R9750_feoperac, 7,2), substr(R9750_feoperac, 1,4)) as fecha_analisis,
lpad(g.num_persona, 8, '0') as NUP,
g.cod_centro,
g.num_cuenta,
g.cod_producto,
g.cod_subprodu,
g.cod_divisa,
(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               a.R9739_IND_DEF
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               cl.R9741_IND_DEF
     else '0' END) as INDICADOR_DEFAULT,
(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               a.R9739_DIAS_PP
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               cl.R9741_DIAS_PP
    else '0' END) as DIAS_PROBATION_PERIODO,
(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               a.R9739_DIAS_ATR
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               cl.R9741_DIAS_ATR
    else '0' END) as DIAS_UMBRALES,
(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               concat_ws('/',substr(R9739_FEC_DEF, 5,2), substr(R9739_FEC_DEF, 7,2), substr(R9739_FEC_DEF, 1,4))
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               concat_ws('/',substr(R9741_FEC_DEF, 5,2), substr(R9741_FEC_DEF, 7,2), substr(R9741_FEC_DEF, 1,4))
    else '99991231' END) fec_entrada_newdefault,

(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               concat_ws('/',substr(R9739_FINI_PP, 5,2), substr(R9739_FINI_PP, 7,2), substr(R9739_FINI_PP, 1,4))
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               concat_ws('/',substr(R9741_FINI_PP, 5,2), substr(R9741_FINI_PP, 7,2), substr(R9741_FINI_PP, 1,4))
    else '99991231' END)  fec_inicio_probation_period,
(case
    when (cast(b.r9750_nivelapl as int) = 1 or cast(b.r9750_nivelapl as int) = 2) then
               concat_ws('/',substr(R9739_FFIN_PP, 5,2), substr(R9739_FFIN_PP, 7,2), substr(R9739_FFIN_PP, 1,4))
    when (cast(b.r9750_nivelapl as int) = 3 or cast(b.r9750_nivelapl as int) = 4) then
               concat_ws('/',substr(R9741_FFIN_PP, 5,2), substr(R9741_FFIN_PP, 7,2), substr(R9741_FFIN_PP, 1,4))
    else '99991231' END) fec_salida_probation_period,
b.r9750_nivelapl as nivelapl,
g.imp_irregular_total_moneda_local,
g.imp_riesgo_divisa_local_del_contrato,
(case
     WHEN R9750_feoperac IS NULL then '0'
     ELSE '1' end ) as FLAG_BDR_ACTIVO
FROM (select * from santander_business_risk_arda.contratos_garra where data_date_part= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='REPORTE_MARCAS_ND') }}' and cod_marca <> 'FA' and cod_marca <> ' ') as g
     left join (select * from bi_corp_bdr.normalizacion_id_contratos where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='REPORTE_MARCAS_ND') }}') as c on TRIM(g.cod_entidad) = TRIM(c.cred_cod_entidad) and TRIM(g.cod_centro) = TRIM(c.cred_cod_centro) and TRIM(g.num_cuenta) = TRIM(c.cred_num_cuenta) and TRIM(g.cod_producto) = TRIM(c.cred_cod_producto) and  TRIM(g.cod_subprodu) = TRIM(c.cred_cod_subprodu_altair)
     left join (select * from bi_corp_bdr.jm_contr_nd where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='REPORTE_MARCAS_ND') }}') as b on lpad(b.r9750_contra1,9,'0') = lpad(c.id_cto_bdr,9,'0')
     left join (select * from bi_corp_bdr.jm_out_cto_nd where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='REPORTE_MARCAS_ND') }}' and r9739_finalind = '00001') as a on lpad(b.r9750_contra1,9,'0') = lpad(a.r9739_contra1,9,'0')
     left join (select * from bi_corp_bdr.jm_out_cli_nd where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='REPORTE_MARCAS_ND') }}' and r9741_finalind = '00001') as cl on lpad(b.r9750_idnumcli,9,'0') = lpad(cl.r9741_idnumcli,9,'0')"
