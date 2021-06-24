set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert into table bi_corp_bdr.jm_expos_no_con 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Sociedades-Monthly') }}' )
select
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Sociedades-Monthly') }}' as E0627_FEOPERAC
    ,'23100' as E0627_S1EMP 
    ,lpad(nvl(nic.id_cto_bdr,0),9,"0") as E0627_CONTRA1
    ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Sociedades-Monthly') }}' as E0627_FEC_MES
    ,lpad(" ", 40, " ") as E0627_CTA_CONT
    ,lpad(" ", 40, " ") as e0627_agrctacb
    ,case when COALESCE(prv.importe,0) >= 0
            then lpad(regexp_replace(format_number(cast(nvl(COALESCE(prv.importe,0),0) as double), 2),"\\,|\\.|\\-",""),17,"0")  
            else concat("-", lpad(regexp_replace(format_number(cast(nvl(COALESCE(prv.importe,0),0) as double), 2),"\\,|\\.|\\-",""),16,"0"))
    end as e0627_importh
    ,'N'  as e0627_idctacen
    ,'0001-01-01' as e0621_fecultmo 
    ,lpad(" ",40," ") as e0627_centctbl
    ,lpad("0",5,"0")  as e0627_entcgbal
    ,rpad(trim(prv.ctacgbal),40," ") as e0627_ctacgbal       
from 
    (
        select  ctacgbal,
                importe
        from bi_corp_staging.ifrs_provisiones prv 
        where prv.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Sociedades-Monthly') }}'
    ) prv
LEFT JOIN 
        (select * 
            from bi_corp_bdr.normalizacion_id_contratos nic 
            where   nic.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Sociedades-Monthly') }}'
                    and trim(nic.source) = 'ENC_ajustes_prov' 
        ) nic on
            trim(nic.blc_cargabal) = trim(prv.ctacgbal)
