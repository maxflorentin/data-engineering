set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_ventas_post_balance_client
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(id as int) id_adm_ventas_post_balance_client,
    cast(nro_prop as int) int_adm_nro_prop,
    penumper as ds_adm_penumper,
    cast(monto as decimal(16,2)) fc_adm_monto,
    mes as dt_adm_mes,
    peusualt as ds_adm_peusualt,
    pefecalt as dt_adm_pefecalt,
    peusumod as ds_adm_peusumod,
    cast(unidades_fisicas_ext as decimal(16,2)) dec_adm_unidades_fisicas_ext,
    cast(unidades_fisicas_int as decimal(16,2)) dec_adm_unidades_fisicas_int,
    cast(monto_dolares as decimal(16,2)) fc_adm_monto_dolares,
    pefecmod as dt_adm_pefecmod,
    cast(cotizacion_dolar as decimal(5,3)) dec_adm_cotizacion_dolar,
    cast(nro_ultima_prop as int) int_adm_nro_ultima_prop
FROM bi_corp_staging.sge_vtas_pos_blc
WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';
