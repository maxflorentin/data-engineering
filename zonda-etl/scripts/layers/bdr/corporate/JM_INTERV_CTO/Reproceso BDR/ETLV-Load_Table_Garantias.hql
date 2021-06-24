set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


--Garantia Miga
INSERT INTO TABLE bi_corp_bdr.jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT        
        '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS G4128_FEOPERAC
        ,23100 AS G4128_S1EMP
        ,lpad(ngm.id_cto_bdr, 9, '0' ) AS G4128_CONTRA1
        ,lpad('2', 5, '0') AS G4128_TIPINTEV 
        ,lpad(bl10.cod_baremo_local, 5, '0') AS G4128_TIPINTV2 
        ,lpad(concat(cast (row_number() OVER(PARTITION BY ngm.id_cto_bdr order by ngm.id_cto_bdr,gc.orden_garantia  asc) as INT),'000000'),17,'0') AS G4128_NUMORDIN
        ,lpad(ngm.num_persona_garante, 9, '0') AS G4128_IDNUMCLI 
        ,lpad(nvl(vind.cod_baremo_global, 0), 5, '0') AS G4128_FORMINTV
        ,lpad(regexp_replace(format_number(coalesce (cast(gc.porc_cobertura_inicial as double), 0), 6),"\\,|\\.",""),9,"0")  AS G4128_PORPARTN
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS G4128_FECULTMO       
from
    (
        select *
        from bi_corp_bdr.aux_garant_garantias_contratos_div 
        where partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
    ) gc
INNER JOIN 
    (
            select distinct ngm.id_cto_bdr, gmg.num_persona_garante 
                from 
                    (
                        select * 
                        from bi_corp_bdr.normalizacion_id_contratos 
                        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                        and source = 'corresponsales-tactico'
                    ) ngm
                inner join 
                    (select *
                        from bi_corp_bdr.garant_miga gmg 
                        where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                            and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                    ) gmg                        
                on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
    ) ngm
 on trim(gc.id_cto_bdr) = trim(ngm.id_cto_bdr)  
left join
       bi_corp_bdr.v_baremos_local bl10
        on  bl10.cod_negocio_local = '10' AND bl10.cod_baremo_local = '5'
 left join bi_corp_bdr.v_map_baremos_global_local bgl10
        ON bgl10.cod_negocio_local = '10' AND bl10.cod_baremo_local = bgl10.cod_baremo_local
LEFT JOIN bi_corp_bdr.v_map_baremos_global_local vind
            on vind.cod_baremo_local = '7'
           and vind.cod_negocio_local = 11;
           
--Avalista o Garante
INSERT INTO TABLE bi_corp_bdr.jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
select
       ict.G4128_FEOPERAC
       ,ict.G4128_S1EMP
       ,ict.G4128_CONTRA1
       ,lpad(bgl10.cod_baremo_global, 5, '0') AS G4128_TIPINTEV
       ,lpad(bl10.cod_baremo_local, 5, '0') AS G4128_TIPINTV2
       ,lpad(concat(cast (row_number() OVER(PARTITION BY ict.G4128_CONTRA1 order by ict.G4128_CONTRA1,gcd.orden_garantia ,rbc.gttcrbc_rbc_num_secclien asc) as INT),'000000'),17,'0') AS G4128_NUMORDIN
       ,lpad(nvl(rbc.gttcrbc_rbc_num_persona,'0'), 9, '0') AS G4128_IDNUMCLI
       ,ict.G4128_FORMINTV
       ,ict.G4128_PORPARTN
       ,ict.G4128_FECULTMO
from
    (select *
        from bi_corp_bdr.jm_interv_cto ict
        where ict.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) ict
    inner join
    (
        select gc.*
        from     
            (
                select *
                from bi_corp_bdr.aux_garant_garantias_contratos_div
                where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
            ) gc
            left join 
            (
            select distinct ngm.id_cto_bdr
                from 
                    (
                        select * 
                        from bi_corp_bdr.normalizacion_id_contratos 
                        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                        and source = 'corresponsales-tactico'
                    ) ngm
                inner join 
                    (select *
                        from bi_corp_bdr.garant_miga gmg 
                        where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                            and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                    ) gmg                        
                on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
            ) gmg
                on trim(gc.id_cto_bdr) = trim(gmg.id_cto_bdr)                
        where trim(gmg.id_cto_bdr) is null   
        
    ) gcd on ict.g4128_contra1 = gcd.id_cto_bdr
    left join
    (select *
        from bi_corp_staging.gtdtrgb
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_gtdtrgb', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
    ) rgb on gcd.num_garantia = rgb.gttcrgb_rgb_num_garantia
    left join
    (select *
        from bi_corp_staging.gtdtrbc
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_gtdtrbc', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
    ) rbc on rbc.gttcrbc_rbc_num_bien = rgb.gttcrgb_rgb_num_bien
    left join
       bi_corp_bdr.v_baremos_local bl10
        on  bl10.cod_negocio_local = '10' AND bl10.cod_baremo_local = '5'
    left join bi_corp_bdr.v_map_baremos_global_local bgl10
        ON bgl10.cod_negocio_local = '10' AND bl10.cod_baremo_local = bgl10.cod_baremo_local;  