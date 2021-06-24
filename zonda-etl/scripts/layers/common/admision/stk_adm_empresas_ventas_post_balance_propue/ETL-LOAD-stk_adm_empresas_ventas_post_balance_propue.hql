set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_ventas_post_balance_propue
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(v.id as int) id_adm_ventas_post_balance_propue,
    cast(v.nro_prop as int) int_adm_nro_prop,
    v.penumper as ds_adm_penumper,
    cast(v.monto as decimal(16,2)) fc_adm_monto,
    v.mes as dt_adm_mes,
    cast(v.unidades_fisicas_ext as decimal(16,2)) dec_adm_unidades_fisicas_ext,
    cast(v.unidades_fisicas_int as decimal(16,2)) dec_adm_unidades_fisicas_int,
    cast(v.monto_dolares as decimal(16,2)) fc_adm_monto_dolares,
    cast(v.cotizacion_dolar as decimal(5,3)) dec_adm_cotizacion_dolar
FROM bi_corp_staging.sge_vtas_pos_blc_prop v
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';
