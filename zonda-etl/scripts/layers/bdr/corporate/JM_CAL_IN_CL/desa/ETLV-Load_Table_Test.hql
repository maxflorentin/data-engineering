set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--Score_comportamiento
insert overwrite table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC 
        ,'23100' as E9954_S1EMP
        ,lpad(c.g4093_idnumcli,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(sc.fec_inf as string)),'0001-01-01') as E9954_FECCALI 
        ,lpad(nvl(bg.cod_baremo_global,'00000'),5,'0') as E9954_TIPMODEL
        ,lpad(nvl(bl.cod_baremo_local,'00000'),5,'0') as E9954_TIPMODE2
        ,lpad(nvl(bl.cod_baremo_local,'00000'),9,'0') as E9954_IDMODEL
        ,null as E9954_TIPO
        ,lpad(regexp_replace(format_number(coalesce (cast(sc.cod_score as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,nvl(to_date(cast(add_months(sc.fec_inf,2) as string)),'9999-12-31') as E9954_FECCADUC              
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        , '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC
    from 
        (
            select *
            from bi_corp_bdr.jm_client_bii c
            where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
         ) c
    inner join 
        (
        select *
            from santander_business_risk_arda.score_comportamiento sc
            where sc.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_score_comportamiento', dag_id='BDR_LOAD_CAL_IN_CL') }}'
        ) sc
        on lpad(sc.num_persona,9,'0') = c.g4093_idnumcli  
    left join 
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local 
                        from bi_corp_bdr.baremos_local bl24
                        where bl24.cod_negocio_local = '12'
        
        ) bl 
            on concat(trim(c.g4093_tipsegl2),'_',sc.cod_modelo) =  trim(cod_baremo_alfanumerico_local) 
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio 
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = '12'
        ) bg
            on bg.cod_baremo_local = bl.cod_baremo_local
            and bl.cod_negocio_local = bg.cod_negocio
where lpad(c.g4093_idnumcli,9,'0') != '000000000';

 --Score_admision
insert into table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')        
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC
        ,'23100' as E9954_S1EMP
        ,lpad(c.g4093_idnumcli,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(sa.fec_ingreso_sco as string)),'0001-01-01') as E9954_FECCALI 
        ,lpad(nvl(bg.cod_baremo_global,'00000'),5,'0') as E9954_TIPMODEL
        ,lpad(nvl(bl.cod_baremo_local,'00000'),5,'0') as E9954_TIPMODE2
        ,lpad(nvl(bl.cod_baremo_local,'00000'),9,'0') as E9954_IDMODEL          
        ,null as E9954_TIPO
        ,lpad(regexp_replace(format_number(coalesce (cast(sa.cod_final_score as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,'9999-12-31' as E9954_FECCADUC
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        , '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC
    from 
        (
            select *
            from bi_corp_bdr.jm_client_bii c
             where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
         ) c
    inner join  
    (
        select sa.*
        from 
           (
           select *
           from santander_business_risk_arda.scoring_admision sa
                where sa.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_scoring_admision', dag_id='BDR_LOAD_CAL_IN_CL') }}'
            ) sa
            inner join
            (select max(sa.num_solicitud) as num_solicitud, 
                    sa.num_documento
                from santander_business_risk_arda.scoring_admision sa
                where sa.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_scoring_admision', dag_id='BDR_LOAD_CAL_IN_CL') }}'
                group by sa.num_documento 
            ) sa1
        on trim(sa.num_documento) = trim(sa1.num_documento) and trim(sa.num_solicitud) = trim(sa1.num_solicitud)
         ) sa
            on lpad(sa.num_persona,9,'0')  = c.g4093_idnumcli 
    left join 
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local 
                        from bi_corp_bdr.baremos_local bl24
                        where bl24.cod_negocio_local = '12'
        
        ) bl 
            on concat(trim(c.g4093_tipsegl2),'_','ADMISION') =  trim(cod_baremo_alfanumerico_local) 
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio 
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = '12'
        ) bg
            on bg.cod_baremo_local = bl.cod_baremo_local
            and bl.cod_negocio_local = bg.cod_negocio
where lpad(c.g4093_idnumcli,9,'0') != '000000000';

--MAPA_SEGUIMIENTO
insert into table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')        
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC
        ,'23100' as E9954_S1EMP
        ,lpad(c.g4093_idnumcli,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(ms.fec_dato as string)),'0001-01-01') as E9954_FECCALI        
        ,lpad(nvl(bg.cod_baremo_global,'00000'),5,'0') as E9954_TIPMODEL
        ,lpad(nvl(bl.cod_baremo_local,'00000'),5,'0') as E9954_TIPMODE2
        ,lpad(nvl(bl.cod_baremo_local,'00000'),9,'0') as E9954_IDMODEL           
        ,null as E9954_TIPO
        ,lpad(regexp_replace(format_number(coalesce (cast(regexp_replace(ms.cod_mapa_seguimiento, 'P|C', '0') as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,nvl(to_date(cast(add_months(ms.fec_dato ,2) as string)),'9999-12-31') as E9954_FECCADUC
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC        
    from 
        (
            select *
            from bi_corp_bdr.jm_client_bii c
             where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
         ) c
    inner join  
        (    SELECT  
                    from_unixtime(unix_timestamp(fecha_dato,'dd/MM/yyyy'),'yyyy-MM-dd')  as fec_dato
                    ,periodo as fec_periodo
                    ,nup as num_persona
                    ,categoria as cod_mapa_seguimiento
            from bi_corp_risk.mapa_seguimiento 
            where periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month', dag_id='BDR_LOAD_CAL_IN_CL') }}'
        ) ms
        on lpad(ms.num_persona,9,'0')  = c.g4093_idnumcli
    left join 
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local 
                        from bi_corp_bdr.baremos_local bl24
                        where bl24.cod_negocio_local = '12'
        
        ) bl 
            on concat(trim(c.g4093_tipsegl2),'_',LOWER(substr(cod_mapa_seguimiento,1,1))) =  trim(cod_baremo_alfanumerico_local) 
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio 
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = '12'
        ) bg
            on bg.cod_baremo_local = bl.cod_baremo_local
            and bl.cod_negocio_local = bg.cod_negocio
where lpad(c.g4093_idnumcli,9,'0') != '000000000';
      
--Rating+/Rating SGE 
insert into table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')                
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC 
        ,'23100' as E9954_S1EMP
        ,lpad(c.g4093_idnumcli,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(sge.fec_periodo  as string)),'0001-01-01') as E9954_FECCALI         
        ,case
           when sge.tipo_rating = 'P' then
             lpad(nvl(bg.cod_baremo_global,'00000'),5,'0')
           when sge.tipo_rating = 'E' then
             lpad(nvl(bg2.cod_baremo_global,'00000'),5,'0')
           else
             '00000'
         end as E9954_TIPMODEL
        ,case
           when sge.tipo_rating = 'P' then
             lpad(nvl(bl.cod_baremo_local,'00000'),5,'0')
           when sge.tipo_rating = 'E' then
             lpad(nvl(bl2.cod_baremo_local,'00000'),5,'0')
           else
             '00000'
         end as E9954_TIPMODE2
        ,case
           when sge.tipo_rating = 'P' then
             lpad(nvl(bl.cod_baremo_local,'00000'),9,'0')
           when sge.tipo_rating = 'E' then
             lpad(nvl(bl2.cod_baremo_local,'00000'),9,'0')
           else
             '000000000'
         end as E9954_IDMODEL
        ,null as E9954_TIPO        
        ,lpad(regexp_replace(format_number(coalesce (cast(sge.coef_rating as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,nvl(to_date(cast(sge.fec_vencimiento as string)),'9999-12-31') as E9954_FECCADUC  
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        , '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC        
    from 
        (
            select *
            from bi_corp_bdr.jm_client_bii c
             where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
         ) c
    inner join  
        (select *
        from santander_business_risk_arda.rating_sge sge
        where sge.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_rating_sge', dag_id='BDR_LOAD_CAL_IN_CL') }}'
        ) sge
        on lpad(sge.num_persona,9,'0')   = c.g4093_idnumcli  
    left join
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local
          from bi_corp_bdr.baremos_local bl24
         where bl24.cod_negocio_local = '12'
        ) bl
          on (sge.tipo_rating = 'P' and concat(trim(c.g4093_tipsegl2),'_','RATING+') =  trim(bl.cod_baremo_alfanumerico_local))
    left join
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local
          from bi_corp_bdr.baremos_local bl24
         where bl24.cod_negocio_local = '12'
        ) bl2
          on (sge.tipo_rating = 'E' and concat(trim(c.g4093_tipsegl2),'_','RATING') =  trim(bl2.cod_baremo_alfanumerico_local))
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = 12
        ) bg
            on bg.cod_baremo_local = bl.cod_baremo_local
            and bl.cod_negocio_local = cast(bg.cod_negocio as string)
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = 12
        ) bg2
            on bg2.cod_baremo_local = bl2.cod_baremo_local
            and bl2.cod_negocio_local = cast(bg2.cod_negocio as string)
where lpad(c.g4093_idnumcli,9,'0') != '000000000';
         
--NoMRG
insert into table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')        
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC 
        ,'23100' as E9954_S1EMP
        ,lpad(no_mrg.identificador_cliente,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(no_mrg.fecha_informacion  as string)),'0001-01-01') as E9954_FECCALI      
        ,lpad(nvl(bg.cod_baremo_global,'00000'),5,'0') as E9954_TIPMODEL
        ,lpad(nvl(bl.cod_baremo_local,'00000'),5,'0') as E9954_TIPMODE2
        ,lpad(nvl(bl.cod_baremo_local,'00000'),9,'0') as E9954_IDMODEL  
        ,null as E9954_TIPO 
        ,lpad(regexp_replace(format_number(coalesce (cast(no_mrg.rating as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,nvl(to_date(cast(add_months(no_mrg.fecha_informacion ,12) as string)),'9999-12-31') as E9954_FECCADUC
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        , '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC
    from 
        (
        select *
        from bi_corp_staging.no_mrg_juridica no_mrg 
        where no_mrg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_no_mrg_juridica', dag_id='BDR_LOAD_CAL_IN_CL') }}'
        ) no_mrg
    left join 
        (
        select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local 
                        from bi_corp_bdr.baremos_local bl24
                        where bl24.cod_negocio_local = '12'        
        ) bl 
            on trim(cod_baremo_alfanumerico_local) = '_No_MRG' 
    left join
        (select distinct cod_baremo_global, cod_baremo_local, cod_negocio 
                    from bi_corp_bdr.map_baremos_global_local mb24
                    where cod_negocio = '12'
        ) bg
            on bg.cod_baremo_local = bl.cod_baremo_local
            and bl.cod_negocio_local = bg.cod_negocio
where lpad(no_mrg.identificador_cliente,9,'0') != '000000000';
        
--Corporate      
insert into table bi_corp_bdr.test_jm_cal_in_cl 
partition(partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}')                
select distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E9954_FEOPERAC 
        ,'23100' as E9954_S1EMP
        ,lpad(c.g4093_idnumcli,9,'0') as E9954_IDNUMCLI
        ,nvl(to_date(cast(an.fec_autoriz_rating_act  as string)),'0001-01-01') as E9954_FECCALI  
        ,lpad(nvl(bg.cod_baremo_global,'00000'),5,'0') as E9954_TIPMODEL
        ,lpad(nvl(bl.cod_baremo_local,'00000'),5,'0') as E9954_TIPMODE2
        ,lpad(nvl(bl.cod_baremo_local,'00000'),9,'0') as E9954_IDMODEL  
        ,null as E9954_TIPO         
        ,lpad(regexp_replace(format_number(coalesce (cast(an.coef_rating as double), 0), 7),"\\,|\\.",""),11,"0") as E9954_IDPUNSCO
        ,nvl(to_date(cast(add_months(an.fec_autoriz_rating_act ,12) as string)),'9999-12-31') as E9954_FECCADUC 
        ,lpad('0', 5, '0') as E9954_C1TARPUN
        ,lpad('0', 5, '0') as E9954_C1SPID
        ,lpad('0', 5, '0') as E9954_C1DIGCON
        ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_CAL_IN_CL') }}' as E0621_FECULTMO
        ,lpad('0', 5, '0') as E9954_MOTIVFOR
        ,lpad('0', 11, '0')  as E9954_IDPUNSC2
        ,'9999-12-31' as E9954_FECREPFI
        ,'0001-01-01' as E9954_FECINOFC
    from 
        (
            select *
            from bi_corp_bdr.jm_client_bii c
             where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
         ) c
    inner join 
        (
        select *
        from bi_corp_bdr.perim_rating_aqua an
        where an.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'
        ) an
        on lpad(an.num_persona,9,'0')  = c.g4093_idnumcli   
    left join (
            select mkg.nup                  
                    ,acsg.unidad_operativa  
                    ,acsg.modelo_rating_interno
            from   
            (select * 
                from bi_corp_staging.aqua_clientes_asoc_geconomicos acsg  
                where acsg.partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_CAL_IN_CL') }}'   
            ) acsg
            inner join             
            ( 
                    select mkg.nup
                        ,mkg.shortname
                    from bi_corp_bdr.perim_mdr_contraparte mkg
                    where  mkg.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_CAL_IN_CL') }}'               
            ) mkg
                on trim(acsg.unidad_operativa) = trim(mkg.shortname)           
            where mkg.nup <> '0'       
            ) acsg
            on lpad(acsg.nup,9,'0') = c.g4093_idnumcli
    left join 
                    (
                    select cod_baremo_local, cod_baremo_alfanumerico_local, desc_baremo_local, cod_negocio_local 
                                    from bi_corp_bdr.baremos_local bl24
                                    where bl24.cod_negocio_local = '12'        
                    ) bl 
                        on concat(trim(c.g4093_tipsegl2),'_',acsg.modelo_rating_interno)  = trim(cod_baremo_alfanumerico_local) 
                left join
                    (select distinct cod_baremo_global, cod_baremo_local, cod_negocio 
                                from bi_corp_bdr.map_baremos_global_local mb24
                                where cod_negocio = '12'
                    ) bg
                        on bg.cod_baremo_local = bl.cod_baremo_local
                        and bl.cod_negocio_local = bg.cod_negocio
where lpad(c.g4093_idnumcli,9,'0') != '000000000';