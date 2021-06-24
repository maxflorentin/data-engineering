set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

drop table gar_miga_usd;

CREATE TEMPORARY TABLE gar_miga_usd AS
select  lpad('',4,'0') as cod_entidad
        ,gmg.num_persona as num_persona
        ,ngm.id_cto_source as id_cto_div
        , 1 as orden_contrato
        , CAST (CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                THEN gmg.Imp_pesos
                ELSE ((imp.e0621_importh * -1) / 100) 
                END as DECIMAL(19,4)) as imp_deuda
        , CAST (CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                THEN gmg.Imp_pesos
                ELSE ((imp.e0621_importh * -1) / 100) 
                END as DECIMAL(19,4)) as apr_sin_mitigar
        , 1 as orden_garantia
        ,'000' as cla_garantia
        ,ngm.id_cto_bdr as num_garantia
        ,lpad(gmg.tip_cobertur,3,0) as tip_cobertur
        ,lpad(gmg.cod_garantia_bdr,3,0) as cod_garantia
        ,gmg.fec_alta as fec_alta
        ,gmg.fec_vcto as fec_bajrelac
        ,gmg.fec_vcto as  fec_vcto
        ,CAST((gmg.imp_limite) as DECIMAL(19,4)) as imp_total_garantia
        ,CAST((gmg.Imp_pesos) as DECIMAL(19,4)) as imp_total_garantia_pesos
        , 1 as orden_prelacion
        , CAST( CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                THEN gmg.Imp_pesos
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as nominal_cobertura_actual
        , CAST( (
                CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                    THEN gmg.Imp_pesos
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
                ) 
        as porc_cobertura_actual
        , CAST(CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                    THEN gmg.Imp_pesos
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END / gmg.Imp_pesos
         as DECIMAL(9,6)
         ) * 100 as porc_reparto
        , CAST (CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                THEN gmg.Imp_pesos
                ELSE ((imp.e0621_importh * -1) / 100) 
                END as DECIMAL(19,4))  as nominal_cobertura_inicial
        , CAST( (
                CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                    THEN gmg.Imp_pesos
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
                ) 
        as porc_cobertura_inicial
        ,CAST( gmg.Imp_pesos - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END
        as DECIMAL(19,4)) as imp_deuda_remanente
        ,CAST(gmg.Imp_pesos - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END
        as DECIMAL(19,4)) as imp_total_garantia_pesos_remantente
        , 0 as stage
        , gmg.tip_instrumentacion as tip_instrumentacion
        , gmg.pignoracion as pignoracion
        , lpad(gmg.cod_estado,3,0) as gar_cod_estado
        , imp.e0621_coddiv as gar_cod_divisa
        , lpad(gmg.tip_aval,2,0) as tip_aval
        , lpad(gmg.tip_gara_bdr,3,0) as tip_gara_bdr
        , lpad(gmg.cod_garantia_bdr,5,0) as cod_garantia_bdr
        , ngm.id_cto_bdr as id_cto_bdr
        , '23100' as cod_entidad_bdr
from 
    (
        select *
        from bi_corp_bdr.jm_posic_contr 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and e0621_coddiv = 'USD'
    ) imp 
inner join 
    (
        select * 
        from bi_corp_bdr.normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and source = 'corresponsales-tactico'
    ) ngm
on imp.e0621_contra1 =  ngm.id_cto_bdr
inner join 
    (
        
    select gmg.*,
            imp_limite * cmb.imp_cambio_fijo_vigente as Imp_Pesos
    from 
        (select *
            from bi_corp_bdr.garant_miga gmg 
            where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) gmg
        left join 
        (
          SELECT cod_divisa
                        , data_date_part
                        , (imp_cambio_fijo/100) as imp_cambio_fijo_vigente
                        , concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
                FROM santander_business_risk_arda.cotizacion
                WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
                        and ind_divisa = 'D' 
                        and ind_cotizacion = 'S' 
                        and cod_segmento = ''
                        and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
        ) cmb  
            on trim(gmg.cod_divisa) = trim(cmb.cod_divisa)    
    ) gmg
        on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8); 

drop table gar_miga_eur;

CREATE TEMPORARY TABLE gar_miga_eur AS
select  lpad('',4,'0') as cod_entidad
        ,gmg.num_persona as num_persona
        ,ngm.id_cto_source as id_cto_div
        , 2 as orden_contrato
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as imp_deuda
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as apr_sin_mitigar
        , 2 as orden_garantia
        ,'000' as cla_garantia
        ,ngm.id_cto_bdr as num_garantia
        ,lpad(gmg.tip_cobertur,3,0) as tip_cobertur
        ,lpad(gmg.cod_garantia_bdr,3,0) as cod_garantia
        ,gmg.fec_alta as fec_alta
        ,gmg.fec_vcto as fec_bajrelac
        ,gmg.fec_vcto as  fec_vcto
        ,CAST((gmg.imp_limite) as DECIMAL(19,4)) as imp_total_garantia
        ,CAST((gmg.Imp_pesos) as DECIMAL(19,4)) as imp_total_garantia_pesos
        , 2 as orden_prelacion
        , CAST(CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4))   as nominal_cobertura_actual
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
            ) 
        as porc_cobertura_actual
        ,CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END  / gmg.Imp_pesos
         as DECIMAL(9,6)) * 100 as porc_reparto
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as nominal_cobertura_inicial
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
                ) 
        as porc_cobertura_inicial
        , CAST(gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_deuda_remanente
        , CAST( gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_total_garantia_pesos_remantente  
        , 0 as stage
        , gmg.tip_instrumentacion as tip_instrumentacion
        , gmg.pignoracion as pignoracion
        , lpad(gmg.cod_estado,3,0) as gar_cod_estado
        , imp.e0621_coddiv as gar_cod_divisa
        , lpad(gmg.tip_aval,2,0) as tip_aval
        , lpad(gmg.tip_gara_bdr,3,0) as tip_gara_bdr
        , lpad(gmg.cod_garantia_bdr,5,0) as cod_garantia_bdr
        , ngm.id_cto_bdr as id_cto_bdr
        , '23100' as cod_entidad_bdr                        
from 
    (
        select *
        from bi_corp_bdr.jm_posic_contr 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and e0621_coddiv = 'EUR'
    ) imp 
inner join 
    (
        select * 
        from bi_corp_bdr.normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and source = 'corresponsales-tactico'
    ) ngm
on imp.e0621_contra1 =  ngm.id_cto_bdr
inner join 
    (
        
    select gmg.*,
            imp_limite * cmb.imp_cambio_fijo_vigente as Imp_Pesos
    from 
        (select *
            from bi_corp_bdr.garant_miga gmg 
            where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) gmg
        left join 
        (
          SELECT cod_divisa
                        , data_date_part
                        , (imp_cambio_fijo/100) as imp_cambio_fijo_vigente
                        , concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
                FROM santander_business_risk_arda.cotizacion
                WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
                        and ind_divisa = 'D' 
                        and ind_cotizacion = 'S' 
                        and cod_segmento = ''
                        and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
        ) cmb  
            on trim(gmg.cod_divisa) = trim(cmb.cod_divisa)    
    ) gmg
        on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
left join
    gar_miga_usd gru  
    on gru.num_persona = trim(gmg.num_persona) 
where gru.porc_reparto < 100;  
    
    
drop table gar_miga_jpy;
    
CREATE TEMPORARY TABLE gar_miga_jpy AS
select  lpad('',4,'0') as cod_entidad
        ,gmg.num_persona as num_persona
        ,ngm.id_cto_source as id_cto_div
        , 3 as orden_contrato
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as imp_deuda
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as apr_sin_mitigar
        , 3 as orden_garantia
        ,'000' as cla_garantia
        ,ngm.id_cto_bdr as num_garantia
        ,lpad(gmg.tip_cobertur,3,0) as tip_cobertur
        ,lpad(gmg.cod_garantia_bdr,3,0) as cod_garantia
        ,gmg.fec_alta as fec_alta
        ,gmg.fec_vcto as fec_bajrelac
        ,gmg.fec_vcto as  fec_vcto
        ,CAST((gmg.imp_limite) as DECIMAL(19,4)) as imp_total_garantia
        ,CAST((gmg.Imp_pesos) as DECIMAL(19,4)) as imp_total_garantia_pesos
        , 3 as orden_prelacion
        , CAST(CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4))   as nominal_cobertura_actual
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
            ) 
        as porc_cobertura_actual
        ,CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END  / gmg.Imp_pesos
         as DECIMAL(9,6)) * 100 as porc_reparto
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as nominal_cobertura_inicial
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
                ) 
        as porc_cobertura_inicial
        , CAST(gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_deuda_remanente
        , CAST( gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_total_garantia_pesos_remantente  
        , 0 as stage
        , gmg.tip_instrumentacion as tip_instrumentacion
        , gmg.pignoracion as pignoracion
        , lpad(gmg.cod_estado,3,0) as gar_cod_estado
        , imp.e0621_coddiv as gar_cod_divisa
        , lpad(gmg.tip_aval,2,0) as tip_aval
        , lpad(gmg.tip_gara_bdr,3,0) as tip_gara_bdr
        , lpad(gmg.cod_garantia_bdr,5,0) as cod_garantia_bdr
        , ngm.id_cto_bdr as id_cto_bdr
        , '23100' as cod_entidad_bdr  
from 
    (
        select *
        from bi_corp_bdr.jm_posic_contr 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and e0621_coddiv = 'JPY'
    ) imp 
inner join 
    (
        select * 
        from bi_corp_bdr.normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and source = 'corresponsales-tactico'
    ) ngm
on imp.e0621_contra1 =  ngm.id_cto_bdr
inner join 
    (
        
    select gmg.*,
            imp_limite * cmb.imp_cambio_fijo_vigente as Imp_Pesos
    from 
        (select *
            from bi_corp_bdr.garant_miga gmg 
            where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) gmg
        left join 
        (
          SELECT cod_divisa
                        , data_date_part
                        , (imp_cambio_fijo/100) as imp_cambio_fijo_vigente
                        , concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
                FROM santander_business_risk_arda.cotizacion
                WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
                        and ind_divisa = 'D' 
                        and ind_cotizacion = 'S' 
                        and cod_segmento = ''
                        and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
        ) cmb  
            on trim(gmg.cod_divisa) = trim(cmb.cod_divisa)    
    ) gmg
        on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
left join
    gar_miga_eur gru  
    on gru.num_persona = trim(gmg.num_persona) 
where gru.porc_reparto < 100  ;  
    

drop table gar_miga_arg;
    
CREATE TEMPORARY TABLE gar_miga_arg AS
select  lpad('',4,'0') as cod_entidad
        ,gmg.num_persona as num_persona
        ,ngm.id_cto_source as id_cto_div
        , 4 as orden_contrato
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as imp_deuda
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as apr_sin_mitigar
        , 4 as orden_garantia
        ,'000' as cla_garantia
        ,ngm.id_cto_bdr as num_garantia
        ,lpad(gmg.tip_cobertur,3,0) as tip_cobertur
        ,lpad(gmg.cod_garantia_bdr,3,0) as cod_garantia
        ,gmg.fec_alta as fec_alta
        ,gmg.fec_vcto as fec_bajrelac
        ,gmg.fec_vcto as  fec_vcto
        ,CAST((gmg.imp_limite) as DECIMAL(19,4)) as imp_total_garantia
        ,CAST((gmg.Imp_pesos) as DECIMAL(19,4)) as imp_total_garantia_pesos
        , 4 as orden_prelacion
        , CAST(CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4))   as nominal_cobertura_actual
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
            ) 
        as porc_cobertura_actual
        ,CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END  / gmg.Imp_pesos
         as DECIMAL(9,6)) * 100 as porc_reparto
        , CAST( CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                THEN gru.imp_total_garantia_pesos_remantente
                ELSE ((imp.e0621_importh * -1) / 100) 
        END as DECIMAL(19,4)) as nominal_cobertura_inicial
        , CAST( (
                CASE WHEN gru.imp_total_garantia_pesos_remantente <= ((imp.e0621_importh * -1) / 100) 
                    THEN gru.imp_total_garantia_pesos_remantente
                    ELSE ((imp.e0621_importh * -1) / 100) 
                END 
                / 
                ((imp.e0621_importh * -1) / 100) *100 
                ) as DECIMAL(9,6)
                ) 
        as porc_cobertura_inicial
        , CAST(gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_deuda_remanente
        , CAST( gru.imp_total_garantia_pesos_remantente - CASE WHEN gmg.Imp_pesos <= ((imp.e0621_importh * -1) / 100) 
                                THEN gmg.Imp_pesos
                                ELSE ((imp.e0621_importh * -1) / 100) 
                        END   as DECIMAL(19,4)) as imp_total_garantia_pesos_remantente  
        , 0 as stage
        , gmg.tip_instrumentacion as tip_instrumentacion
        , gmg.pignoracion as pignoracion
        , lpad(gmg.cod_estado,3,0) as gar_cod_estado
        , imp.e0621_coddiv as gar_cod_divisa
        , lpad(gmg.tip_aval,2,0) as tip_aval
        , lpad(gmg.tip_gara_bdr,3,0) as tip_gara_bdr
        , lpad(gmg.cod_garantia_bdr,5,0) as cod_garantia_bdr
        , ngm.id_cto_bdr as id_cto_bdr
        , '23100' as cod_entidad_bdr    
from 
    (
        select *
        from bi_corp_bdr.jm_posic_contr 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and e0621_coddiv = 'ARS'
    ) imp 
inner join 
    (
        select * 
        from bi_corp_bdr.normalizacion_id_contratos 
        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        and source = 'corresponsales-tactico'
    ) ngm
on imp.e0621_contra1 =  ngm.id_cto_bdr
inner join 
    (
        
    select gmg.*,
            imp_limite * cmb.imp_cambio_fijo_vigente as Imp_Pesos
    from 
        (select *
            from bi_corp_bdr.garant_miga gmg 
            where gmg.fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
                and gmg.fec_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
        ) gmg
        left join 
        (
          SELECT cod_divisa
                        , data_date_part
                        , (imp_cambio_fijo/100) as imp_cambio_fijo_vigente
                        , concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
                FROM santander_business_risk_arda.cotizacion
                WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
                        and ind_divisa = 'D' 
                        and ind_cotizacion = 'S' 
                        and cod_segmento = ''
                        and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
        ) cmb  
            on trim(gmg.cod_divisa) = trim(cmb.cod_divisa)    
    ) gmg
        on trim(gmg.num_persona) = SUBSTRING(trim(id_cto_source),1,8) 
left join
    gar_miga_jpy gru  
    on gru.num_persona = trim(gmg.num_persona) 
where gru.porc_reparto < 100  ;
     


INSERT INTO TABLE bi_corp_bdr.aux_garant_garantias_contratos_div
PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT *
FROM 
    (
    select * from gar_miga_usd
    union all 
    select * from gar_miga_eur
    union all
    select * from gar_miga_jpy
    union all
    select * from gar_miga_arg
    ) gmm  

