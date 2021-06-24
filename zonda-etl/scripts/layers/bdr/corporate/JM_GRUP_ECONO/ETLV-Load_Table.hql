set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.jm_grup_econo partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}') 
SELECT '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}' as G5512_FEOPERAC
        ,'23100' as G5512_S1EMP
        ,lpad(nvl(gee.penumgru,0),9,"0") as G5512_GRUP_ECO
        ,lpad("",9,"0") as G5512_DCODGRUP
        ,rpad(trim(regexp_replace(gee.pedesgru,'�', '?')),80, ' ') as G5512_DGRUPO
        ,lpad("",40," ") as G5512_IDSUCUR
        ,lpad(gee.codigo_iso_del_pais,5,"0") as G5512_ID_PAIS
        ,gee.facturacion as G5512_IMPFACTM
        ,gee.total_activo as G5512_TOT_ACTI
        ,lpad("",9,"0") as G5512_NUM_EMPL
        ,lpad("",5,"0") as G5512_ORIG_FAC
        ,lpad("",5,"0") as G5512_ORIG_ACT
        ,lpad("",5,"0") as G5512_ORIG_EMP
        ,lpad("",17,"0") as G5512_IMPT_RGO
        ,nvl(gee.fecha_ejercicio,'0001-01-01') as G5512_FECINFAC
        ,lpad(nvl(ble.cod_baremo_global,"0"),5,"0") as G5512_GRECOSEC
        ,lpad("",3," ") as G5512_CODDIV
        ,'0001-01-01' as G5512_FECULTMO
        ,lpad("",17,"0") as G5512_TDEUDAGR
        ,lpad("",1," ") as G5512_FLGEMPNO
FROM    (
            SELECT   cast(mge.id_destino as string) as penumgru
                ,sge.pedesgru 
                ,ffm.fecha_ejercicio
                ,ffm.actividad_bcra
                ,sge.codigo_iso_del_pais
                ,case when sge.facturacion >= 0
                      then concat("+",lpad(regexp_replace(sge.facturacion,"\\,|\\.|\\-",""),16,"0"))
                      else concat("-",lpad(regexp_replace(sge.facturacion,"\\,|\\.|\\-",""),16,"0"))
                end as facturacion
                ,case when sge.total_activo >= 0
                      then concat("+",lpad(regexp_replace(sge.total_activo,"\\,|\\.|\\-",""),16,"0"))
                      else concat("-",lpad(regexp_replace(sge.total_activo,"\\,|\\.|\\-",""),16,"0"))
                end as total_activo
            FROM 
                (
                    select sge.nro_grupo as penumgru 
                        ,mpe.pedesgru 
                         ,cast(nvl('32',0) as STRING) as codigo_iso_del_pais
                         ,cast(sum(regexp_replace(format_number(cast(nvl(regexp_replace(trim(sge.facturacion),"\\,","."),0) as double), 2),"\\,|\\.","")) as BIGINT) as facturacion
                         ,cast(sum(regexp_replace(format_number(cast(nvl(regexp_replace(trim(sge.total_activo),"\\,","."),0) as double), 2),"\\,|\\.","")) as BIGINT) as total_activo 
                    from
                        (select sge.nro_grupo
                                ,sge.facturacion
                                ,sge.total_activo
                        from bi_corp_staging.sge_grupos_economicos sge 
                        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_sge_grupos_economicos', dag_id='BDR_LOAD_Tables-Monthly') }}'
                        ) sge
                    left outer join 
                        (select distinct trim(mpe.penumgru) as penumgru
                            ,mpe.pedesgru 
                        from bi_corp_staging.malpe_pedt024 mpe 
                        where mpe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt024', dag_id='BDR_LOAD_Tables-Monthly') }}'   
                        ) mpe 
                    on  cast(trim(sge.nro_grupo) as BIGINT) = cast(trim(penumgru) as BIGINT)
                    group by sge.nro_grupo, mpe.pedesgru
                ) sge
            left outer join
                (select sge.* 
                    from
                        (select sge.nro_grupo
                                ,to_date(from_unixtime(unix_timestamp(sge.fecha_ejercicio ,'dd/MM/yyyy'), 'yyyy-MM-dd')) as fecha_ejercicio
                                ,sge.facturacion
                                ,sge.actividad_bcra
                                ,row_number() over(partition by sge.nro_grupo  ORDER BY cast(nvl(regexp_replace(trim(sge.facturacion),"\\,","."),0) as double)  desc) as seq
                        from bi_corp_staging.sge_grupos_economicos sge
                        where  sge.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_sge_grupos_economicos', dag_id='BDR_LOAD_Tables-Monthly') }}'
                         ) sge
                         where sge.seq = 1
                ) ffm on cast(trim(sge.penumgru) as BIGINT) = cast(trim(ffm.nro_grupo) as BIGINT)
            left outer join bi_corp_bdr.map_grupo_economico mge 
                        on mge.id_origen = trim(sge.penumgru)
                        and mge.source = 'sge'
            union all
            select cast(mge.id_destino as string) as penumgru
                    ,gpe.pedesgru
                    ,gpe.fecha_ejercicio
                    ,gpe.actividad_bcra
                    ,gpe.codigo_iso_del_pais
                    ,case when gpe.facturacion >= 0
                      then concat("+",nvl(lpad(regexp_replace(format_number(gpe.facturacion, 2),"\\,|\\.",""),16,"0"),0))
                      else concat("-",nvl(lpad(regexp_replace(format_number(gpe.facturacion, 2),"\\,|\\.",""),16,"0"),0))
                    end as facturacion
                    ,case when gpe.total_activo >= 0
                      then concat("+",nvl(lpad(regexp_replace(format_number(gpe.total_activo, 2),"\\,|\\.",""),16,"0"),0))
                      else concat("-",nvl(lpad(regexp_replace(format_number(gpe.total_activo, 2),"\\,|\\.",""),16,"0"),0))
                    end as total_activo
                    from 
                        (                        
                        select 
                            gpe.entidad_legal
                            ,gpe.nombre_largo as pedesgru
                            ,max(to_date(from_unixtime(UNIX_TIMESTAMP(concat(gpe.fecha_informacion,"1231"),"yyyyMMdd")))) as fecha_ejercicio
                            ,max(lpad(gpe.sector_aqua,5,"0")) as actividad_bcra
                            ,cast(nvl(gpe.codigo_iso_del_pais,0) as STRING) as codigo_iso_del_pais 
                            ,sum(cast(regexp_replace(nvl(( case when trim(gpe.divisa) = "ARS" and gpe.unidad_monetaria = "MM" then (gpe.facturacion * "1000000")  
                                     when trim(gpe.divisa) = "ARS" and gpe.unidad_monetaria = "M" then (gpe.facturacion * "1000")
                                     when trim(gpe.divisa) <> "ARS" and gpe.unidad_monetaria = "MM" then (gpe.facturacion * "1000000") * tcm.tipo_de_cambio
                                     when trim(gpe.divisa) <> "ARS" and gpe.unidad_monetaria = "M" then (gpe.facturacion * "1000") * tcm.tipo_de_cambio
                                     else gpe.facturacion
                            end),0) ,"\\,",".") as double)) as facturacion
                            ,sum(cast(regexp_replace(nvl(( case when trim(gpe.divisa) = "ARS" and gpe.unidad_monetaria = "MM" then (gpe.total_activos * "1000000")  
                                         when trim(gpe.divisa) = "ARS" and gpe.unidad_monetaria = "M" then (gpe.total_activos * "1000")
                                         when trim(gpe.divisa) <> "ARS" and gpe.unidad_monetaria = "MM" then (gpe.total_activos * "1000000") * tcm.tipo_de_cambio
                                         when trim(gpe.divisa) <> "ARS" and gpe.unidad_monetaria = "M" then (gpe.total_activos * "1000") * tcm.tipo_de_cambio
                                         else gpe.total_activos
                            end),0) ,"\\,",".") as double)) as total_activo
                            from                             
                                (
                                select  gpe.entidad_legal
                                        ,gpe.nombre_largo
                                        ,gpe.fecha_informacion
                                        ,gpe.codigo_iso_del_pais
                                        ,gpe.divisa
                                        ,gpe.unidad_monetaria
                                        ,gpe.facturacion
                                        ,gpe.total_activos
                                        ,gpe.sector_aqua
                                from bi_corp_staging.aqua_grupos_economicos gpe 
                                where gpe.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
                                ) gpe
                            left OUTER JOIN 
                                (select distinct substr(tcm.par_de_divisas,1,3) as par_de_divisas 
                                        ,tcm.tipo_de_cambio
                                   from bi_corp_staging.aqua_tipo_cambio tcm 
                                  where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_aqua_tipo_cambio', dag_id='BDR_LOAD_Tables-Monthly') }}' 
                                    and substr(tcm.par_de_divisas,5,3) = 'ARS' 
                                ) tcm on gpe.divisa = tcm.par_de_divisas
                            group by     gpe.entidad_legal
                                        ,gpe.nombre_largo 
                                        ,cast(nvl(gpe.codigo_iso_del_pais,0) as STRING)
                        ) gpe
                    inner join 
                        (   select distinct acli.glcs_grupo
                            from bi_corp_staging.aqua_clientes_asoc_geconomicos acli
                            where acli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}' 
                        ) acli    
                        on trim(gpe.entidad_legal) = trim(acli.glcs_grupo)                    
                    left outer join bi_corp_bdr.map_grupo_economico mge 
                        on mge.id_origen = trim(gpe.entidad_legal)
                        and mge.source = 'aqua'   
            union all                 
                select *
                from 
                    (select     
                        lpad(cast(mnr.id_destino as string), 9, '0') as penumgru
                        ,ngr.empresa as pedesgru
                        ,case when trim(ngr.fecha_informacion) = '-' 
                            then '0001-01-01' 
                            else trim(ngr.fecha_informacion) 
                        end as fecha_ejercicio
                        ,lpad(' ',5,"0") as actividad_bcra
                        ,case when pais = 'Argentina'
                            then '32'
                            else '0'
                            end as codigo_iso_del_pais 
                        ,case when cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double) >= 0
                            then concat("+",nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0))
                            else concat("-",nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0))
                        end as facturacion
                        ,case when cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double) >= 0
                            then concat("+",nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0))
                            else concat("-",nvl(lpad(regexp_replace(format_number(cast(regexp_replace(regexp_replace(nvl(trim(ngr.total_activos),0),"\\-","0"),"\\.","")  as double), 2),"\\,|\\.",""),16,"0"),0))
                        end as total_activo 
                    from 
                                    (select ngr.empresa
                                            ,ngr.fecha_informacion
                                            ,ngr.total_activos
                                            ,ngr.pais                                                                        
                                    from bi_corp_staging.no_mrg_grupos ngr
                                    where ngr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_no_mrg_grupos', dag_id='BDR_LOAD_Tables-Monthly') }}'  
                                    ) ngr
                                left join 
                                    (select mnr.id_destino
                                            ,mnr.empresa
                                    from bi_corp_bdr.map_no_mrg mnr
                                    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_map_no_mrg', dag_id='BDR_LOAD_Tables-Monthly') }}' 
                                    ) mnr on trim(ngr.empresa) = trim(mnr.empresa)  
                    ) mrg                                                  
            )  gee             
        LEFT JOIN 
          (select distinct empresa, cod_baremo_local, cod_baremo_global
             from bi_corp_bdr.map_baremos_global_local
             where cod_negocio_local = '77'
               ) ble
                 on cast(trim(ble.cod_baremo_local) as int) = cast(trim(gee.actividad_bcra) as int)                 
where gee.penumgru is not null
and fecha_ejercicio >= add_months(current_date,-24) ;   