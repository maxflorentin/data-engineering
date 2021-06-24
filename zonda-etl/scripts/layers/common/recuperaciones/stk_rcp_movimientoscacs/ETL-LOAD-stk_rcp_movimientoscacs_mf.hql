set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS tmp;
CREATE TEMPORARY TABLE tmp AS
select distinct location_code, acct_num, coded_hist_seq_num, state_act, cycles, partition_date
from bi_corp_staging.view_cacs_movimientos
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_EMERIX_CACS-Daily') }}'
union all
select distinct location_code, acct_num, coded_hist_seq_num, state_act, cycles, partition_date
from bi_corp_staging.view_cacs_movimientos
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_CMN_RECUPERACIONES_EMERIX_CACS-Daily') }}'
;

DROP TABLE IF EXISTS last_state_cycle;
CREATE TEMPORARY TABLE last_state_cycle AS
select ls.location_code, ls.acct_num, ls.coded_hist_seq_num, ls.state_act, os.state_act as state_ant, trim(ls.cycles) as cycles, trim(os.cycles) as cycles_old
from tmp ls
left join tmp os on ls.location_code = os.location_code and ls.acct_num = os.acct_num and cast(ls.coded_hist_seq_num as bigint) = cast(os.coded_hist_seq_num as bigint) - 1
where ls.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_EMERIX_CACS-Daily') }}'
;

insert overwrite table bi_corp_common.stk_rcp_movimientoscacs
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_EMERIX_CACS-Daily') }}')

SELECT
trim(m.location_code) as ds_rcp_codigolocacion,
trim(m.acct_num) as ds_rcp_acctnum,
case when substr(m.acct_num,1,2)='CL' THEN cast(trim(SUBSTR(m.acct_num,4,14)) as bigint) ELSE cast(trim(SUBSTR(m.acct_num,-12,12)) as bigint) end as cod_rcp_cuenta,
cast(m.pay_amt as decimal(17,2)) as fc_rcp_montopago,
case when trim(m.letter_code) in ("null", "") then null else trim(letter_code) end as cod_rcp_soportemensaje,
m.activity_date as dt_rcp_fechaactividad,
trim(m.coll_activity_code) as cod_rcp_actividadcoleccion,
case when trim(m.party_cntct_code) in ("null", "") then null else trim(m.party_cntct_code) end as ds_rcp_partycntctcode,
case when trim(m.place_called) in ("null", "") then null else trim(m.place_called) end as ds_rcp_lugarllamado,
cast(m.promise_amt_1 as decimal(17,2)) as fc_rcp_promesapago1,
m.promise_date_1 as dt_rcp_fechapromesapago1,
cast(m.promise_amt_2 as decimal(17,2)) as fc_rcp_promesapago2,
m.promise_date_2 as dt_rcp_fechapromesapago2,
trim(m.state_act) as ds_rcp_estadoactual,
trim(ls.state_ant) as ds_rcp_estadoanterior,
cast(m.total_delinq_amt as decimal(17,2)) as fc_rcp_importeexigible,
cast(m.balance_amt as decimal(17,2)) as fc_rcp_importedeuda,
cast(m.branch_num as int) as cod_rcp_sucursal,
case when trim(m.resp_coll_id) in ("null", "") then null else trim(m.resp_coll_id) end as ds_rcp_resp_coll_id,
case when trim(m.collector_id) in ("null", "") then null else trim(m.collector_id) end as ds_rcp_collid,
trim(m.coded_hist_seq_num) as ds_rcp_secuenciamovimiento,
cast(m.charge_off_amt as decimal(17,2)) as fc_rcp_montochargeoff,
trim(m.customer_info_num) as ds_rcp_nupconletras,
cast(m.num_nup as bigint) as cod_rcp_nup,
trim(m.customer_name) as ds_rcp_nombrecliente,
trim(m.social_sec_num) as ds_rcp_dnicuit,
cast(trim(m.cycles) as int) as ds_rcp_ciclo,
cast(ls.cycles_old as int) as ds_rcp_cicloanterior,
m.warn_bulletin_date as dt_rcp_fechaentradamora,
m.hold_date as dt_rcp_holddate,
trim(m.nonpay_excuse_code) as ds_rcp_nombrecliente,
trim(m.time_zone) as ds_rcp_timezone,
case when trim(m.value_code) in ("null", "") then null else trim(m.value_code) end as ds_rcp_codvaluacion,
trim(m.account_type) as ds_rcp_tipocuenta,
case when trim(m.recommended_act_cd) in ("null", "") then null else trim(m.recommended_act_cd) end as ds_rcp_codigoaccionrecomendada,
case when trim(m.risk_code) in ("null", "") then null else trim(m.risk_code) end as ds_rcp_codigoriesgo,
case when trim(m.credit_score) in ("null", "") then null else trim(m.credit_score) end as ds_rcp_scorecredito,
case when trim(m.zona) in ("null", "") then null else trim(m.zona) end as ds_rcp_zona,
case when trim(m.letra_score) in ("null", "") then null else trim(m.letra_score) end as ds_rcp_letrascore,
case when trim(m.numero_score) in ("null", "") then null else trim(m.numero_score) end as ds_rcp_numeroscore,
case when trim(m.escenario) in ("null", "") then null else trim(m.escenario) end as ds_rcp_escenario,
case when trim(m.desc_escenario) in ("null", "") then null else trim(m.desc_escenario) end as ds_rcp_escescenario,
case when trim(m.fixed_action_1) in ("null", "") then null else trim(m.fixed_action_1) end as ds_rcp_solucion1,
case when trim(m.fixed_action_2) in ("null", "") then null else trim(m.fixed_action_2) end as ds_rcp_solucion2,
case when trim(m.timed_action_1) in ("null", "") then null else trim(m.timed_action_1) end as ds_rcp_accion1,
case when trim(m.timed_action_2) in ("null", "") then null else trim(m.timed_action_2) end as ds_rcp_accion2,
case when trim(m.timed_action_3) in ("null", "") then null else trim(m.timed_action_3) end as ds_rcp_accion3,
case when trim(m.timed_action_4) in ("null", "") then null else trim(m.timed_action_4) end as ds_rcp_accion4,
case when trim(m.timed_action_5) in ("null", "") then null else trim(m.timed_action_5) end as ds_rcp_accion5,
case when trim(m.mora_404) in ("null", "") then null else trim(m.mora_404) end as ds_rcp_mora404,
case when trim(m.veraz_behavior) in ("null", "") then null else trim(m.veraz_behavior) end as ds_rcp_verazbehavior,
case when trim(m.marca_bnp) in ("null", "") then null else trim(m.marca_bnp) end as ds_rcp_marcabnp,
case when trim(m.dato_renta) in ("null", "") then null else trim(m.dato_renta) end as ds_rcp_datorenta
FROM bi_corp_staging.view_cacs_movimientos m
LEFT JOIN last_state_cycle ls on ls.acct_num = m.acct_num and ls.location_code = m.location_code and ls.coded_hist_seq_num = m.coded_hist_seq_num
WHERE m.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_EMERIX_CACS-Daily') }}'
;