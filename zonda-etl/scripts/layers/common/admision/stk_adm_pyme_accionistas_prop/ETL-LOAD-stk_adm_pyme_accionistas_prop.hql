set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_pyme_accionistas_prop
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}')
select distinct
    cast(a.nro_prop as int ) as cod_adm_nro_prop,
    cast(a.secuencia as int ) as int_adm_secuencia,
    a.nombre as ds_adm_nombre,
    cast(a.porcentaje as double) as dec_adm_porcentaje,
    a.cargo as ds_adm_cargo,
    cast(a.solvencia as double) as dec_adm_solvencia,
    a.cliente as ds_adm_cliente,
    a.peusualt as cod_adm_peusualt,
    a.peusumod as cod_adm_peusumod,
    a.pefecalt as dt_adm_pefecalt,
    a.pefecmod as dt_adm_pefecmod,
    cast(a.penumper as int) as cod_adm_penumper,
    cast(a.nup as int) as cod_per_nup,
    cast(a.cuit as bigint) as cod_adm_cuit,
    a.tipo_doc as ds_adm_tipo_doc,
    cast(a.nro_doc as int) as cod_adm_nro_doc,
    cast(a.nro_orden as int) as int_adm_nro_orden,
    a.mar_sexo as flag_adm_mar_sexo,
    a.fec_nac as dt_adm_fec_nac,
    a.mar_error_altair as flag_adm_mar_error_altair,
    a.msj_error_altair as ds_adm_msj_error_altair,
    a.apellido as ds_adm_apellido,
    a.forma_juridica as ds_adm_forma_juridica,
    a.tipo_doc_conyuge as ds_adm_tipo_doc_conyuge,
    cast(a.nro_doc_conyuge as int) as int_adm_nro_doc_conyuge,
    a.cod_estado_civil as ds_adm_estado_civil
from
    bi_corp_staging.sge_accionariado_prop a
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Pyme-Daily') }}';