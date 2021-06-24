set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.dim_inv_canales_fondos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}')
select
    cast(c.agent_code as int) as cod_inv_agente_colocador,
    cast(c.code as int) as cod_inv_canal,
    c.hame as ds_inv_canal,
    cast(c.type as int) as cod_inv_tipo_canal,
    cast(c.status as int) as cod_inv_estado_canal,
    if(c.mark = 'BP', 1, 0) as flag_inv_bp_canal,
    c.origin as cod_inv_origen
from bi_corp_staging.fm_maestro_canales c
where partition_date=IF('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}'<'2021-03-17','2021-03-17','{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Inversiones_hist') }}');