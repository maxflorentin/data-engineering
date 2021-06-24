set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert into table bi_corp_bdr.test_jm_expos_no_con 
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' )
select
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0627_FEOPERAC
    ,lpad(nvl(ble.cod_baremo_global,0),5,"0") as E0627_S1EMP 
    ,lpad(nvl(nic.id_cto_bdr,0),9,"0") as E0627_CONTRA1
    ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0627_FEC_MES
    ,lpad(" ", 40, " ") as E0627_CTA_CONT
    ,lpad(" ", 40, " ") as e0627_agrctacb
    ,case when COALESCE(jpc.e0621_importh,0)  > COALESCE(blc.importe,0)
            then lpad(regexp_replace(format_number(cast(nvl(COALESCE(blc.importe,0) - COALESCE(jpc.e0621_importh,0),0) as double), 2),"\\,|\\.|\\-",""),17,"0")  
            else concat("-", lpad(regexp_replace(format_number(cast(nvl(COALESCE(blc.importe,0) - COALESCE(jpc.e0621_importh,0),0) as double), 2),"\\,|\\.|\\-",""),16,"0"))
    end as e0627_importh
    ,'N'  as e0627_idctacen
    ,'0001-01-01' as e0621_fecultmo 
    ,lpad(" ",40," ") as e0627_centctbl
    ,lpad("0",5,"0")  as e0627_entcgbal
    ,rpad(trim(blc.cargabal),40," ") as e0627_ctacgbal       
from 
    (
        select  blc.sociedad
                ,blc.cargabal
                , sum(cast(blc.importe as double) ) as importe
        from bi_corp_bdr.balances_ajustes blc 
        where blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        and blc.sociedad = '00083'
         group by blc.sociedad, blc.cargabal
    ) blc
inner join 
    (select 
            jpc.e0621_ctacgbal
            , sum(cast(jpc.e0621_importh as double)*-1  / 100) as e0621_importh
        from  bi_corp_bdr.jm_posic_contr jpc 
        where jpc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
        group by jpc.e0621_ctacgbal
    ) jpc
    on trim(jpc.e0621_ctacgbal) = trim(blc.cargabal)
LEFT JOIN (select * 
                from bi_corp_bdr.test_normalizacion_id_contratos nic 
                where nic.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                    and trim(nic.source) = 'ENC_ajustes' ) nic on  
        trim(nic.blc_sociedad) =  trim(blc.sociedad)
        and trim(nic.blc_cargabal) = trim(blc.cargabal)
left join  
    (select empresa, cod_baremo_local, cod_baremo_global
     from bi_corp_bdr.map_baremos_global_local
     where cod_negocio_local = '1'
    ) ble
    on cast(trim(ble.cod_baremo_local) as int) = cast(trim(blc.sociedad) as int)
where (COALESCE(blc.importe,0) - COALESCE(jpc.e0621_importh,0))  <> 0;