set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_movimientoscacs
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}')

SELECT
case when trim(m.location_code) in ("null", "", null) then null else trim(m.location_code) end as ds_rcp_codigolocacion,
case when trim(m.acct_num) in ("null", "", null) then null else trim(m.acct_num) end as ds_rcp_acctnum,
case when substr(m.acct_num,1,2)='CL' THEN cast(trim(SUBSTR(m.acct_num,4,14)) as bigint) ELSE cast(trim(SUBSTR(m.acct_num,-12,12)) as bigint) end as cod_nro_cuenta,
case when trim(m.pay_amt) in ("null", "", null) then null else cast(m.pay_amt as decimal(17,2)) end as fc_rcp_montopago,
case when trim(m.letter_code) in ("null", "", null) then null else trim(letter_code) end as cod_rcp_soportemensaje,
case when trim(m.activity_date) in ("null") then null else substring(m.activity_date, 1, 10) end as dt_rcp_fechaactividad,
case when trim(m.coll_activity_code) in ("null", "", null) then null else trim(m.coll_activity_code) end as cod_rcp_actividadcoleccion,
case when trim(m.party_cntct_code) in ("null", "", null) then null else trim(m.party_cntct_code) end as ds_rcp_partycntctcode,
case when trim(m.place_called) in ("null", "", null) then null else trim(m.place_called) end as ds_rcp_lugarllamado,
case when trim(m.promise_amt_1) in ("null", "", null) then null else cast(m.promise_amt_1 as decimal(17,2)) end as fc_rcp_promesapago1,
case when trim(m.promise_date_1) in ("null") then null else substring(m.promise_date_1, 1, 10) end as dt_rcp_fechapromesapago1,
case when trim(m.promise_amt_2) in ("null", "", null) then null else cast(m.promise_amt_2 as decimal(17,2)) end as fc_rcp_promesapago2,
case when trim(m.promise_date_2) in ("null") then null else substring(m.promise_date_2, 1, 10) end as dt_rcp_fechapromesapago2,
case when trim(m.state_act) in ("null", "", null) then null else trim(m.state_act) end as ds_rcp_estadoactual,
case when trim(m.state_ant) in ("null", "", null) then null else trim(m.state_ant) end as ds_rcp_estadoanterior,
case when trim(m.total_delinq_amt) in ("null", "", null) then null else cast(m.total_delinq_amt as decimal(17,2)) end as fc_rcp_importeexigible,
case when trim(m.balance_amt) in ("null", "", null) then null else cast(m.balance_amt as decimal(17,2)) end as fc_rcp_importedeuda,
case when trim(m.branch_num) in ("null", "", null) then null else cast(m.branch_num as int) end as cod_suc_sucursal,
case when trim(m.resp_coll_id) in ("null", "", null) then null else trim(m.resp_coll_id) end as ds_rcp_resp_coll_id,
case when trim(m.collector_id) in ("null", "", null) then null else trim(m.collector_id) end as ds_rcp_collid,
case when trim(m.coded_hist_seq_num) in ("null", "", null) then null else trim(m.coded_hist_seq_num) end as ds_rcp_secuenciamovimiento,
case when trim(m.charge_off_amt) in ("null", "", null) then null else cast(m.charge_off_amt as decimal(17,2)) end as fc_rcp_montochargeoff,
case when trim(m.customer_info_num) in ("null", "", null) then null else trim(m.customer_info_num) end as ds_rcp_nupconletras,
case when trim(m.num_nup) in ("null", "", null) then null else cast(m.num_nup as bigint) end as cod_per_nup,
case when trim(m.customer_name) in ("null", "", null) then null else trim(m.customer_name) end as ds_rcp_nombrecliente,
case when trim(m.social_sec_num) in ("null", "", null) then null else trim(m.social_sec_num) end as ds_rcp_dnicuit,
case when trim(m.cycles) in ("null", "", null) then null else cast(trim(m.cycles) as int) end as ds_rcp_ciclo,
case when trim(m.cycles_old) in ("null", "", null) then null else cast(trim(m.cycles_old) as int) end as ds_rcp_cicloanterior,
case when trim(m.warn_bulletin_date) in ("null", "", null) then null else substring(m.warn_bulletin_date, 1, 10) end as dt_rcp_fechaentradamora,
case when trim(m.hold_date) in ("null", "", null) then null else substring(m.hold_date, 1, 10) end as dt_rcp_fechaholddate,
case when trim(m.nonpay_excuse_code) in ("null", "", null) then null else trim(m.nonpay_excuse_code) end as ds_rcp_excusanopago,
case when trim(m.time_zone) in ("null", "", null) then null else trim(m.time_zone) end as ds_rcp_timezone,
case when trim(m.value_code) in ("null", "", null) then null else trim(m.value_code) end as ds_rcp_codvaluacion,
case when trim(m.account_type) in ("null", "", null) then null else trim(m.account_type) end as ds_rcp_tipocuenta,
case when trim(m.recommended_act_cd) in ("null", "", null) then null else trim(m.recommended_act_cd) end as ds_rcp_codigoaccionrecomendada,
case when trim(m.risk_code) in ("null", "", null) then null else trim(m.risk_code) end as ds_rcp_codigoriesgo,
case when trim(m.credit_score) in ("null", "", null) then null else trim(m.credit_score) end as ds_rcp_scorecredito,
case when trim(m.zona) in ("null", "", null) then null else trim(m.zona) end as ds_rcp_zona,
case when trim(m.letra_score) in ("null", "", null) then null else trim(m.letra_score) end as ds_rcp_letrascore,
case when trim(m.numero_score) in ("null", "", null) then null else trim(m.numero_score) end as ds_rcp_numeroscore,
case when trim(m.escenario) in ("null", "", null) then null else trim(m.escenario) end as cod_rcp_escenario,
case when trim(m.desc_escenario) in ("null", "", null) then null else trim(m.desc_escenario) end as ds_rcp_escenario,
case when trim(m.fixed_action_1) in ("null", "", null) then null else trim(m.fixed_action_1) end as ds_rcp_solucion1,
case when trim(m.fixed_action_2) in ("null", "", null) then null else trim(m.fixed_action_2) end as ds_rcp_solucion2,
case when trim(m.timed_action_1) in ("null", "", null) then null else trim(m.timed_action_1) end as ds_rcp_accion1,
case when trim(m.timed_action_2) in ("null", "", null) then null else trim(m.timed_action_2) end as ds_rcp_accion2,
case when trim(m.timed_action_3) in ("null", "", null) then null else trim(m.timed_action_3) end as ds_rcp_accion3,
case when trim(m.timed_action_4) in ("null", "", null) then null else trim(m.timed_action_4) end as ds_rcp_accion4,
case when trim(m.timed_action_5) in ("null", "", null) then null else trim(m.timed_action_5) end as ds_rcp_accion5,
case when trim(m.mora_404) in ("null", "", null) then null else trim(m.mora_404) end as ds_rcp_mora404,
case when trim(m.veraz_behavior) in ("null", "", null) then null else trim(m.veraz_behavior) end as ds_rcp_verazbehavior,
case when trim(m.marca_bnp) in ("null", "", null) then null else trim(m.marca_bnp) end as ds_rcp_marcabnp,
case when trim(m.dato_renta) in ("null", "", null) then null else trim(m.dato_renta) end as ds_rcp_datorenta,
case when trim(m.score_alineado) in ("null", "", null) then null else cast(m.score_alineado as decimal(17,2)) end as fc_rcp_scorealineado,
case when trim(m.balance_risk_total) in ("null", "", null) then null else cast(m.balance_risk_total as decimal(17,2)) end as fc_rcp_balancerisktotal,
case when trim(m.balance_risk_unsecured) in ("null", "", null) then null else cast(m.balance_risk_unsecured as decimal(17,2)) end as fc_rcp_balanceriskunsecured,
case when trim(m.balance_risk_secured) in ("null", "", null) then null else cast(m.balance_risk_secured as decimal(17,2)) end as fc_rcp_balancerisksecured,
case when trim(m.scorecard) in ("null", "", null) then null else cast(m.scorecard as decimal(17,2)) end as fc_rcp_scorecard
FROM bi_corp_staging.cacs_movimientos m
WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_CacsEmerix-Daily') }}'
;