set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_rating_operaciones
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id_vo as bigint) as id_adm_vo,
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    cast(secuencia as int) as cod_adm_secuencia,
    cod_operacion as cod_adm_operacion,
    cast(plazo as decimal(16, 2)) as dec_adm_plazo,
    unidad_plazo as flag_adm_unidad_plazo,
    cast(atraso as int) as int_adm_atraso,
    cast(punt_plazo as decimal(16, 2)) as dec_adm_punt_plazo,
    cast(punt_antiguedad as decimal(16, 2)) as dec_adm_punt_antiguedad,
    cast(valoracion_operacion as decimal(16, 2)) as dec_adm_valoracion_operacion,
    peusualt as cod_adm_peusualt,
    pefecalt as ts_adm_pefecalt,
    peusumod as cod_adm_peusumod,
    pefecmod as ts_adm_pefecmod
from bi_corp_staging.sge_valoracion_operaciones
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';