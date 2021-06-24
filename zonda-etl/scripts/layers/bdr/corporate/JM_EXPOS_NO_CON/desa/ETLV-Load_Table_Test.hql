set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.test_jm_expos_no_con partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' )
select
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0627_FEOPERAC
    ,lpad(nvl(ble.cod_baremo_global,0),5,"0") as E0627_S1EMP 
    ,lpad(nvl(nic.id_cto_bdr,0),9,"0") as E0627_CONTRA1
    ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' as E0627_FEC_MES
    ,lpad(" ", 40, " ") as E0627_CTA_CONT
    ,lpad(" ", 40, " ") as e0627_agrctacb
    ,case when cast(nvl(blc.importe,0) as bigint) >= 0 
            then concat("-", lpad(regexp_replace(format_number(cast(nvl(blc.importe,0) as bigint), 2),"\\,|\\.|\\-",""),16,"0")) 
            else lpad(regexp_replace(format_number(cast(nvl(blc.importe,0) as bigint), 2),"\\,|\\.|\\-",""),17,"0")  
    end as e0627_importh
    ,case when trim(blc.sociedad_eliminacion) <> '00000' then 'H' else 'N' end  as e0627_idctacen
    ,'0001-01-01' as e0621_fecultmo 
    ,lpad(" ",40," ") as e0627_centctbl
    ,lpad(blc.sociedad_eliminacion ,5,"0")  as e0627_entcgbal
    ,rpad(trim(blc.cargabal),40," ") as e0627_ctacgbal
FROM (select blc.* 
        from bi_corp_bdr.balances_ajustes blc 
        where (substring(trim(blc.cargabal),1,1) = '1' or substring(trim(blc.cargabal),1,3) in ('260','261','269'))
            and blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}' 
      union all        
      select blc.*
        from
          (select blc.* 
            from bi_corp_bdr.balances_ajustes blc 
            where blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
            and substring(trim(blc.cargabal),1,3) in ('262','263','264')
            and trim(blc.sociedad)  = '00083'
          ) blc
        left join 
          (select distinct rcp.cargabal 
              from bi_corp_staging.rubros_cargabal_provisiones rcp 
              where rcp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rubros_cargabal_provisiones', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
          ) rcp 
            on trim(blc.cargabal) = trim(rcp.cargabal) 
        where rcp.cargabal is null  
      union all
      select blc.* 
        from bi_corp_bdr.balances_ajustes blc 
        where trim(blc.cargabal) = '316'
            and blc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
      ) blc
left join  
          (select empresa, cod_baremo_local, cod_baremo_global
             from bi_corp_bdr.map_baremos_global_local
             where cod_negocio_local = '1'
               ) ble
                 on cast(trim(ble.cod_baremo_local) as int) = cast(trim(blc.sociedad) as int)
LEFT JOIN (select * 
                from bi_corp_bdr.test_normalizacion_id_contratos nic 
                where nic.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
                    and trim(nic.source) = 'balance sociedad' ) nic on  
        trim(nic.blc_sociedad) =  trim(blc.sociedad)
        and trim(nic.blc_cargabal) = trim(blc.cargabal)
        and trim(nic.blc_sociedad_eliminacion) = trim(blc.sociedad_eliminacion)
LEFT JOIN (select distinct jpc.e0621_ctacgbal, jpc.e0621_s1emp
            from  bi_corp_bdr.jm_posic_contr jpc 
            where jpc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly-Desa') }}'
            ) jpc on trim(blc.cargabal) = trim(jpc.e0621_ctacgbal) and trim(e0621_s1emp)  = lpad(ble.cod_baremo_global,5,"0")
WHERE jpc.e0621_s1emp is null;