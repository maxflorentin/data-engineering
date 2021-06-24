set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_estadosoperacion
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    cast(s.secu_f487 as int) as cod_adm_secu_f487,
    s.fec_log as dt_adm_fec_log,
    s.des_log as ds_adm_log,
    s.obs_log as ds_adm_obs_log,
    s.cod_usuario as cod_adm_usuario,
    d.ds_adm_estado as ds_adm_estado,
    d.cod_adm_estado as cod_adm_estado,
    d.cod_adm_estado_accion as cod_adm_estado_accion,
    d.cod_adm_estado_origen as cod_adm_estado_origen,
    d.ds_adm_sector as ds_adm_sector,
    cast(f.nro_prop as int) as cod_adm_nro_prop,
    f.cod_producto as cod_adm_producto,
    f.operacion_especial as flag_adm_operacion_especial,
    p.origen as ds_adm_origen,
    cast(p.cuit_cliente as bigint) as int_adm_cuit_cliente,
    p.cod_campania as cod_adm_campania
from
    bi_corp_staging.sge_stnd_log_f487 s
    left join bi_corp_common.dim_adm_pyme_estadosoperacion d on
        d.ds_adm_des_estado = s.des_log and
        d.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
    left join bi_corp_staging.sge_f487 f on
        f.secu_f487 = s.secu_f487 and
        f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
    left join bi_corp_staging.sge_propuesta p on
        p.nro_prop = f.nro_prop and
        p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
where
    s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';