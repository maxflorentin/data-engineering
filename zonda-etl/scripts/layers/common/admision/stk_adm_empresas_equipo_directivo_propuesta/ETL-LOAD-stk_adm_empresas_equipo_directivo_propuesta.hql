set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_equipo_directivo_propuesta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    cast(secuencia as bigint) as cod_adm_secuencia,
    nombre as ds_adm_nombre,
    cargo as ds_adm_cargo,
    if(cliente = "S", 1, 0) as flag_adm_cliente,
    peusualt as cod_adm_peusualt,
    peusumod as cod_adm_peusumod,
    pefecalt as ts_adm_pefecalt,
    pefecmod as ts_adm_pefecmod,
    cast(penumper as int) as cod_adm_penumper,
    cast(nup as int) as cod_per_nup,
    cast(cuit as bigint) as cod_adm_cuit,
    tipo_doc as cod_adm_tipo_doc,
    cast(nro_doc as bigint) as cod_adm_nro_doc,
    cast(anio_nacimiento as int) as int_adm_anio_nacimiento,
    des_titulo_prof as ds_adm_titulo_prof,
    if(mar_inmuebles_propios = "S", 1, 0) as flag_adm_inmuebles_propios
from bi_corp_staging.sge_equipo_directivo_prop
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';