set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_estadosoperacion
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(e.penumper as int) as cod_adm_penumper,
    cast(e.secu_f487 as int) as cod_adm_secu_f487,
    e.fecha as dt_adm_fecha,
    e.estado as cod_adm_estado,
    e.usuario as cod_adm_usuario,
    cast(e.id_dev as int) as id_adm_dev,
    e.observaciones as ds_adm_observaciones,
    e.cod_cargo_autorizante as cod_adm_cargo_autorizante,
    e.cod_dentro_limite as cod_adm_dentro_limite,
    e.mar_dentro_limite as flag_adm_dentro_limite,
    e.mar_devolucion as flag_adm_devolucion,
    d.desc_estado as ds_adm_estado,
    f.cod_producto as cod_adm_producto,
    cast(f.nro_prop as int) as cod_adm_nro_prop,
    f.cod_operacion as cod_adm_operacion,
    f.operacion_especial as flag_adm_operacionespecial,
    cast(s.cuit_cliente as bigint) as int_adm_cuit_cliente,
    s.origen as cod_adm_origen
FROM bi_corp_staging.sge_estados_f487 e
left join bi_corp_staging.sge_estados d on d.estado_prop = e.estado and d.cod_proceso = 'F487' and d.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
left join bi_corp_staging.sge_f487 f on cast(f.secu_f487 as int) = cast(e.secu_f487 as int) and f.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
left join bi_corp_staging.sge_propuesta s on cast(f.nro_prop as int) = cast(s.nro_prop AS int) and s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
where e.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';