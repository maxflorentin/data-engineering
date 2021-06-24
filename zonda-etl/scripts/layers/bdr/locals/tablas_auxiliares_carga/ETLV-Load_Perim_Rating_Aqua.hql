set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.perim_rating_aqua
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT  
        '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}'  as fec_dato
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='month', dag_id='BDR_LOAD_Tables-Monthly') }}' as fec_periodo
        ,mkg.nup as num_persona
        ,acsg.fec_autoriz_rating_act
        ,acsg.coef_rating
    from    (
                    select distinct unidad_operativa
                            ,rating_interno as coef_rating
                            , from_unixtime(unix_timestamp(fecha_rating_interno,'yyyymmdd'),'yyyy-mm-dd') as fec_autoriz_rating_act
                    from bi_corp_staging.aqua_clientes_asoc_geconomicos acsg  
                   where  rating_interno != 'SIN_CALIFI'  
                    and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
            ) acsg
        inner join 
            ( 
                    select mkg.nup
                        ,mkg.shortname
                    from bi_corp_bdr.perim_mdr_contraparte mkg
                    where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'               
            ) mkg
                on trim(acsg.unidad_operativa) = trim(mkg.shortname)
 