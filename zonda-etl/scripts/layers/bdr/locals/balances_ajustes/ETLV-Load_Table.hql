set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.balances_ajustes 
PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select 
blc.cargabal,
blc.sociedad,
blc.sociedad_eliminacion,
sum(nvl(blc.importe,0))  as importe 
FROM (select trim(blc.cargabal) as cargabal,
            trim(blc.sociedad) as sociedad ,
            trim(blc.sociedad_eliminacion) as sociedad_eliminacion, 
            sum(cast(nvl(trim(blc.importe),0) as bigint)) as importe
        from bi_corp_staging.manual_balance blc
        where blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
        group by  trim(blc.cargabal), trim(blc.sociedad), trim(blc.sociedad_eliminacion)
      union all  
        select trim(pra.cargabal) as cargabal, 
                trim(pra.sociedad) as sociedad, 
                    trim(pra.sociedad_eliminacion) as sociedad_eliminacion, 
                    sum(cast(nvl(trim(pra.importe),0) as bigint)) as importe 
            from bi_corp_staging.manual_pr_ajuste pra
            where pra.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
             group by trim(pra.sociedad), trim(pra.cargabal), trim(pra.sociedad_eliminacion)
       union all
       select trim(sda.cargabal) as cargabal, 
                trim(sda.sociedad) as sociedad, 
                    trim(sda.sociedad_eliminacion) as sociedad_eliminacion, 
                    sum(cast(nvl(trim(sda.importe),0) as bigint)) as importe 
            from bi_corp_staging.manual_sd_ajuste sda
            where sda.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
            group by  trim(sda.sociedad), trim(sda.cargabal), trim(sda.sociedad_eliminacion)
        ) blc
GROUP BY        
    blc.cargabal,
    blc.sociedad,
    blc.sociedad_eliminacion; 