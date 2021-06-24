set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_garantias_lineas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(nro_prop as bigint) as cod_adm_nro_prop,
    cod_operacion as cod_adm_operacion,
    cast(seq_lin_garan as bigint) as cod_adm_seq_lin_garan,
    cod_gartia as cod_adm_gartia,
    cast(porc_garantia as int) as int_adm_porc_garantia,
    cast(monto_garantia as decimal(16, 2)) as fc_adm_monto_garantia,
    detalle_garantia as ds_adm_detalle_garantia,
    peusualt as cod_adm_peusualt,
    date_format(pefecalt,'yyyy-MM-dd') as dt_adm_pefecalt,
    peusumod as cod_adm_peusumod,
    cast(secuencia as bigint) as cod_adm_secuencia,
    date_format(pefecmod,'yyyy-MM-dd') as dt_adm_pefecmod,
    cast(valoracion as decimal(5, 2)) as dec_adm_valoracion,
    cast(patrim_neto as decimal(16, 2)) as fc_adm_patrim_neto,
    marca_garantia_default as flag_adm_garantia_default,
    moneda_garantia as cod_adm_moneda_garantia,
    cast(opcion as int) as int_adm_opcion,
    cast(ltv as bigint) as cod_adm_ltv,
    if(fec_vencimiento is null, vencimiento, fec_vencimiento) as dt_adm_vencimiento,
    cast(porcentaje_cobertura as decimal(16, 4)) as dec_adm_porcentaje_cobertura,
    cast(codigo as int) as cod_adm_codigo,
    cast(monto_limite as decimal(16, 2)) as fc_adm_monto_limite
from bi_corp_staging.sge_lineas_garantias
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';