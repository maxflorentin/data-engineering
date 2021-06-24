set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_estadospropuesta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select
    concat('F485', lpad(s.nro_prop, 16 , "0")) as cod_adm_tramite,
    cast(s.nro_prop as int) as cod_adm_nro_prop,
    s.fec_log as dt_adm_fec_log,
    s.des_log as ds_adm_log,
    s.obs_log as ds_adm_obs_log,
    s.cod_usuario as cod_adm_usuario,
    s.estado_prop as ds_adm_estado_prop,
    s.cod_estado_accion as cod_adm_estado_accion,
    s.cod_estado_origen as cod_adm_estado_origen,

    CASE
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se desasigna en ZG%'
            THEN 'ZONA GRIS'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se asigna en ZG%'
            THEN 'ZONA GRIS'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se desasigna en SUP%'
            THEN 'SUPERVISION'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se asigna en SUP%'
            THEN 'SUPERVISION'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se desasigna en CVC%'
            THEN 'CVC'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se asigna en CVC%'
            THEN 'CVC'
        WHEN s.cod_estado_accion = 'OV' AND s.cod_estado_origen IN ('AU')
            THEN 'SISTEMAS'
        WHEN s.des_log IN ('Requiere intervención de CVC.  Enviar legajo físico.')
            THEN 'SUCURSAL'
        WHEN d.ds_adm_des_estado IS NOT NULL
            THEN d.ds_adm_sector
        ELSE ''
        END AS ds_adm_sector_estado,

    CASE
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se desasigna%'
            THEN 'PENDIENTE'
        WHEN s.cod_estado_accion IS NULL AND s.des_log LIKE 'Se asigna%'
            THEN 'PENDIENTE'
        WHEN s.cod_estado_accion = 'OV' AND s.cod_estado_origen IN ('AU')
            THEN 'PENDIENTE'
        WHEN s.des_log IN ('Requiere intervención de CVC.  Enviar legajo físico.')
            THEN 'PENDIENTE SUC'
        WHEN d.ds_adm_des_estado IS NOT NULL
            THEN d.ds_adm_estado
        ELSE ''
    END as ds_adm_tipo_estado,

    p.origen as ds_adm_origen,
    p.penumper as cod_adm_penumper,
    cast(p.cuit_cliente as bigint) as int_adm_cuit_cliente,
    p.cod_campania as cod_adm_campania
from
    bi_corp_staging.sge_stnd_log_propuesta s
left join bi_corp_common.dim_adm_pyme_estadospropuesta d on
    s.des_log = d.ds_adm_des_estado and
    d.cod_adm_estado_accion = s.cod_estado_accion and
    d.cod_adm_estado_origen = s.cod_estado_origen and
    d.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_propuesta p on
    p.nro_prop = s.nro_prop and
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_estados e on
    e.estado_prop = p.estado_prop and
    e.cod_proceso = 'F485' and
    e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
where
    s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'

UNION ALL

select distinct
    concat('F485', lpad(p.nro_prop, 16 , "0")) as cod_adm_tramite,
    cast(p.nro_prop as int) as cod_adm_nro_prop,
    p.fecha as dt_adm_fec_log,

    CASE
        WHEN e.ds_adm_estado is null and p.estado = 'NE'
        THEN 'Nosis Error'
        ELSE e.ds_adm_estado
    END as ds_adm_log,

    p.obs_estado as ds_adm_obs_log,
    p.usuario as cod_adm_usuario,
    p.estado as ds_adm_estado_prop,
    p.cod_estado_accion as cod_adm_estado_accion,
    p.cod_estado_origen as cod_adm_estado_origen,

    CASE
        WHEN pr.origen = 'CPP'
            THEN 'CPP'
        WHEN pr.origen = 'PYME'
            THEN 'SUPERVISION'
        WHEN pr.origen = 'CORP'
            THEN 'CORPORATIVO'
        WHEN pr.origen = 'ATRI'
            THEN 'ATRIBUCION SUCURSALES'
        ELSE ''
	END as ds_adm_sector_estado,

    CASE
        WHEN e.ds_adm_estado is null and p.estado = 'NE'
        THEN 'Nosis Error'
        ELSE e.ds_adm_estado
    END as ds_adm_tipo_estado,

    pr.origen as ds_adm_origen,
    pr.penumper as cod_adm_penumper,
    cast(pr.cuit_cliente as bigint) as int_adm_cuit_cliente,
    pr.campania as cod_adm_campania
from
    bi_corp_staging.sge_estados_propuesta p
left join bi_corp_common.dim_adm_empresas_estadospropuesta e on
    e.cod_adm_estado = p.estado and
    e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
left join bi_corp_staging.sge_propuesta pr on
    pr.nro_prop = p.nro_prop and
    pr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}'
where
    p.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';