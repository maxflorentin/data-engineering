set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_rcp_contratosjudicializados
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}')

SELECT
cast(rc.`525_cod_centro` as int) as cod_suc_sucursal,
cast(rc.`525_num_contrato` as bigint) as cod_nro_cuenta,
trim(rc.`525_cod_producto`) as cod_prod_producto,
trim(rc.`525_cod_subprodu`) as cod_prod_subproducto,
trim(rc.`525_cod_divisa`) as cod_div_divisa,
cast(rc.`525_num_persona` as bigint) as cod_per_nup,
pr.`527_imp_reclamad` as fc_rcp_importereclamado,
trim(pr.`527_num_bufete`) as cod_rcp_buffete,
case when pr.`527_ind_suspproc` = 'N' then 1 else 0 end as flag_rcp_indicasuspensionproceso,
case when trim(pr.`527_fec_suspproc`) in ('9999-12-31', '0001-01-01') then NULL else trim(pr.`527_fec_suspproc`) end as dt_rcp_fechasuspensionproceso,
case when trim(pr.`527_fec_altareg`) in ('9999-12-31', '0001-01-01') then NULL else trim(pr.`527_fec_altareg`) end as dt_rcp_fechaaltaprocedimiento,
case when trim(pr.`527_fec_baja`) in ('9999-12-31', '0001-01-01') then NULL else trim(pr.`527_fec_baja`) end as dt_rcp_fechabajaprocedimiento,
cast(rc.`525_num_posicion` as bigint) as ds_rcp_numeroposicion,
cast(rc.`525_num_ordenjud` as bigint) as ds_rcp_numeroordenjudicial,
trim(rc.`525_cod_ordenjud`) as cod_rcp_ordenjudicial,
cast(rc.`525_num_anoauto` as int) as ds_rcp_nroanioauto,
cast(rc.`525_num_juzgado` as bigint) as ds_rcp_numjuzgado,
trim(rc.`525_cod_interven`) as cod_rcp_intervencion,
trim(rc.`525_num_ordinter`) as ds_rcp_nroordeninterno,
case when trim(rc.`525_fec_baja`) in ('9999-12-31', '0001-01-01') then NULL else trim(rc.`525_fec_baja`) end as dt_rcp_fechabajacontrato,
case when trim(rc.`525_fec_altareg`) in ('9999-12-31', '0001-01-01') then NULL else trim(rc.`525_fec_altareg`) end as dt_rcp_fechaaltacontrato
from bi_corp_staging.garra_rel_contrato_prj rc
left join bi_corp_staging.garra_procedimientos pr on rc.`525_num_posicion` = pr.`527_num_posicion` and rc.`525_num_ordenjud` = pr.`527_num_ordenjud` and pr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
where rc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_RECUPERACIONES_GarraMoria-Daily') }}'
;