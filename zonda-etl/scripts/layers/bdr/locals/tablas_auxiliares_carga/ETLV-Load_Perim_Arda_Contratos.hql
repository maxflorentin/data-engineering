set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with core_contratos_n as (
        select DISTINCT num_persona,
               concat_ws("_",a.cod_entidad,a.cod_centro,a.num_cuenta,a.cod_producto,a.cod_subprodu_altair) as pk_s_d
          FROM santander_business_risk_arda.contratos a
         WHERE a.data_date_part =  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_contratos', dag_id='BDR_LOAD_Tables-Monthly') }}'
           and (a.cod_marca != "FA" or a.cod_marca is null)
           and (    a.cod_estado_rel_cliente is null
                or not (    a.cod_estado_rel_cliente = "C"
                         and a.fec_baja_rel_cliente <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'))
)
insert overwrite table bi_corp_bdr.perim_arda_contratos
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select cc.num_persona, nc.id_cto_bdr, nc.id_cto_source as id_cto_contratos_sin_divisa
  from core_contratos_n cc
inner join bi_corp_bdr.normalizacion_id_contratos nc
    on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
   and cc.pk_s_d = nc.id_cto_source
;