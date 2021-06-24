set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_bdr.jm_grup_rela PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' )
select distinct *
from 
(SELECT 
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as G5515_FEOPERAC,
    '23100' as G5515_S1EMP,
    lpad(cast(m.id_destino as string), 9, '0') as G5515_GRUP_ECO,
    lpad(nvl(s.nup,0), 9, '0') as G5515_IDNUMCLI,
    lpad(nvl(CASE rownum WHEN 1 THEN '00002' ELSE '00003' END,0),5,'0') AS G5515_ROL_JERA,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS G5515_FECULTMO
FROM 
    (
        select s.nup
                ,s.nro_grupo
        from bi_corp_staging.sge_grupos_economicos s 
        WHERE s.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_sge_grupos_economicos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  
            and s.nro_grupo is not null
            and to_date(from_unixtime(unix_timestamp(s.fecha_ejercicio ,'dd/MM/yyyy'), 'yyyy-MM-dd')) >= add_months(current_date,-24)  
    ) s
LEFT JOIN bi_corp_bdr.map_grupo_economico m on 
            m.id_origen = s.nro_grupo
            and m.source = 'sge'
LEFT JOIN   (
                SELECT nup, 
                        nro_grupo, 
                        row_number() over (partition by nro_grupo order by cast(nvl(trim(facturacion),0) as BIGINT) desc) as rownum 
                FROM bi_corp_staging.sge_grupos_economicos  
                WHERE nro_grupo is not null
                    and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_sge_grupos_economicos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  
            ) f on f.nro_grupo = s.nro_grupo 
                    and f.nup = s.nup
UNION ALL
SELECT 
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as G5515_FEOPERAC,
    '23100' as G5515_S1EMP,
    lpad(cast(m.id_destino as string), 9, '0') as G5515_GRUP_ECO,
    lpad(k.nup, 9, '0') as G5515_IDNUMCLI,
    lpad(nvl(CASE WHEN a.entidad_legal = 'GLCS Grupo' THEN '00002' ELSE '00003' END,0),5,'0') as G5515_ROL_JERA,
    '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS G5515_FECULTMO
FROM                      
    (                    
        select distinct gpe.entidad_legal 
        from bi_corp_staging.aqua_grupos_economicos gpe 
        where gpe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  
        and to_date(from_unixtime(UNIX_TIMESTAMP(concat(gpe.fecha_informacion,"1231"),"yyyyMMdd"))) >= add_months(current_date,-24) 
    ) gpe
inner join 
    (   select distinct acli.entidad_legal
                      , acli.unidad_operativa 
                      , acli.glcs_grupo
        from bi_corp_staging.aqua_clientes_asoc_geconomicos acli
        where acli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  
    ) a    
on trim(gpe.entidad_legal) = trim(a.glcs_grupo)
LEFT JOIN bi_corp_bdr.map_grupo_economico m on 
    trim(m.id_origen) = trim(a.glcs_grupo)
    and m.source = 'aqua'
INNER JOIN  ( 
                    select mkg.nup
                        ,mkg.shortname
                    from bi_corp_bdr.perim_mdr_contraparte mkg
                    where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'               
            ) k
    on trim(a.unidad_operativa) = trim(k.shortname)   
union all
select   '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as G5515_FEOPERAC
        ,'23100' as G5515_S1EMP
        ,lpad(cast(mnr.id_destino as string), 9, '0') as G5515_GRUP_ECO
        ,lpad(mnr.nup, 9, '0') as G5515_IDNUMCLI
        ,lpad(nvl(CASE gnr.rownum WHEN 1 THEN '00002' ELSE '00003' END,0),5,'0') AS G5515_ROL_JERA
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AS G5515_FECULTMO
from 
    (select ngr.empresa
        from bi_corp_staging.no_mrg_grupo_relacion ngr
        where ngr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_no_mrg_grupos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'  
    ) ngr
    left join 
        (select mnr.id_destino
                ,mnr.nup
                ,mnr.empresa
            from bi_corp_bdr.map_no_mrg mnr
            where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig',key='max_partition_map_no_mrg', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) mnr on trim(ngr.empresa) = trim(mnr.empresa)  
    LEFT JOIN (              
        SELECT gnr.nup 
                ,gnr.nro_grupo 
                ,row_number() over (partition by gnr.nro_grupo order by facturacion desc) as rownum 
            FROM (
                    select 
                        mnr.nup
                            ,ngr.empresa as nro_grupo 
                            ,case when trim(ngr.fecha_informacion) = '-' 
                                then '0001-01-01' 
                                else trim(ngr.fecha_informacion) 
                              end as fecha_ejercicio
                            ,cast(regexp_replace(regexp_replace(nvl(trim(ngr.facturacion_grupo ),0),"\\-","0"),"\\.","")  as double) as facturacion                           
                    from 
                        (select ngr.empresa
                                ,ngr.fecha_informacion
                                ,ngr.facturacion_grupo
                            from bi_corp_staging.no_mrg_grupos ngr
                            where ngr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_no_mrg_grupos', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                        ) ngr
                    left join 
                        (select mnr.nup
                                ,mnr.empresa
                            from bi_corp_bdr.map_no_mrg mnr
                            where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig',key='max_partition_map_no_mrg', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                        ) mnr on trim(ngr.empresa) = trim(mnr.empresa) 
                    ) gnr 
            where gnr.fecha_ejercicio >= add_months(current_date,-24)
        ) gnr
        on gnr.nup = mnr.nup and gnr.nro_grupo = mnr.empresa
) grl;