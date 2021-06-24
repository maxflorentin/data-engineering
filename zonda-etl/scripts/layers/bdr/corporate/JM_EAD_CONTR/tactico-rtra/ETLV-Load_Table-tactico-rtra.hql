set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with tactico_rtra as (
                       select dealstamp,
                              origen,
                              regexp_replace(format_number(cast(nvl(regexp_replace(a.contribution,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") contribution,
                              regexp_replace(format_number(cast(nvl(regexp_replace(a.market_value,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") market_value,
                              regexp_replace(format_number(cast(nvl(regexp_replace(a.nominal_value,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") nominal_value,
                              regexp_replace(format_number(cast(nvl(regexp_replace(a.mtm_net,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") mtm_net
                         from
                             (select dealstamp,
                                     lpad('1',5,'0') origen,
                                     contribution ,
                                     market_value,
                                     nominal_value,
                                     mtm_net
                                from bi_corp_staging.rtra_regulatorio_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                              select dealstamp,
                                     lpad('2',5,'0') origen,
                                     rec_equivalente contribution,
                                     market_value,
                                     nominal_value,
                                     mtm_net
                                from bi_corp_staging.rtra_economico_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA') a
                         left join santander_business_risk_arda.cotizaciones_bcra c
                           on c.data_date_part  = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cotizaciones_bcra', dag_id='BDR_LOAD_perimetro-rtra') }}'
                          and c.cod_especie  = 'USD'
                       )
insert overwrite table bi_corp_bdr.test_jm_ead_contr
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select distinct
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as G5519_FEOPERAC,
       '23100' as G5519_S1EMP,
       lpad(nc.id_cto_bdr,9,'0') as G5519_CONTRA1,
       rtra_reg_eco.origen as G5519_METAPLIC,
       lpad('2',5,'0') as G5519_FASECALC,
       case
         when rtra_reg_eco.market_value  >= 0 then -- cambiamos el signo
           concat("+", lpad(rtra_reg_eco.market_value,16,'0'))
         else
           concat("-",lpad(regexp_replace(rtra_reg_eco.market_value,"-",""),16,'0'))
       end as G5519_MTM,
       case
         when nvl(rtra_reg_eco.contribution , 0) >= 0 then
           concat("+", lpad(rtra_reg_eco.contribution,16,'0'))
         else
           concat("-",lpad(regexp_replace(rtra_reg_eco.contribution,"-",""),16,'0'))
       end as G5519_EAD,
       lpad('0',17,'0') as G5519_ESPEPROV,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as G5519_FECULTMO,
       case
         when nvl(rtra_reg_eco.nominal_value , 0) >= 0 then
           concat("+", lpad(rtra_reg_eco.nominal_value,16,'0'))
         else
           concat("-",lpad(regexp_replace(rtra_reg_eco.nominal_value,"-",""),16,'0'))
       end as G5519_IMPNOMCT,
       lpad('0',17,'0') as G5519_ADDONBRU,
       lpad('0',17,'0') as G5519_ADDONNET,
       lpad('0',9,'0') as G5519_COEFREGU,
       lpad('0',5,'0') as G5519_METLIQUI,
       case
         when nvl(rtra_reg_eco.mtm_net , 0) >= 0 then
           concat("+", lpad(rtra_reg_eco.mtm_net,16,'0'))
         else
           concat("-",lpad(regexp_replace(rtra_reg_eco.mtm_net,"-",""),16,'0'))
       end as G5519_MTM_BRUT
  from bi_corp_staging.cargabal_rtra  cargabal
 inner join tactico_rtra rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
 --tabla test normalizacion
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source = 'tactico-rtra'
       and nc.id_cto_source = concat_ws('_', trim(cargabal.cuenta),trim(cargabal.dealstamp))
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
 UNION ALL
  select g5519_feoperac,
        g5519_s1emp,
        g5519_contra1,
        g5519_metaplic,
        g5519_fasecalc,
        g5519_mtm,
        g5519_ead,
        g5519_espeprov,
        g5519_fecultmo,
        g5519_impnomct,
        g5519_addonbru,
        g5519_addonnet,
        g5519_coefregu,
        g5519_metliqui,
        g5519_mtm_brut
   from bi_corp_bdr.jm_ead_contr ead
  inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source != 'tactico-rtra'
       and nc.id_cto_bdr = ead.g5519_contra1
 where ead.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 ;