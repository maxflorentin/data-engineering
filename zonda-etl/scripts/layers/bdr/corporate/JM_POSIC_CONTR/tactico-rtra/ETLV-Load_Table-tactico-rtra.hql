set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with tactico_rtra as (
                       select cpty,
                              dealstamp,
                              currency,
                              regexp_replace(format_number(cast(nvl(regexp_replace(market_value,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") market_value,
                              regexp_replace(format_number(cast(nvl(regexp_replace(nominal_value,"\\,","."),0) as double)  *
                                                           cast(nvl(regexp_replace(c.imp_cotizacion_ars,"\\,","."),0) as double), 2),"\\,|\\.","") nominal_value
                         from bi_corp_staging.rtra_regulatorio_test
                         left join santander_business_risk_arda.cotizaciones_bcra c
                           on c.data_date_part  = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cotizaciones_bcra', dag_id='BDR_LOAD_perimetro-rtra') }}'
                          and c.cod_especie  = 'USD'
                        where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                          and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                          and cpty != 'CLIENTE FICTICIO ARGENTINA'
                       )
INSERT overwrite table bi_corp_bdr.test_jm_posic_contr
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select distinct
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS E0621_FEOPERAC
      ,'23100' AS E0621_S1EMP
      ,lpad(nc.id_cto_bdr,9,'0')  AS E0621_CONTRA1 --Normalización de Contrato
      ,rpad(cargabal.bsr , 40, ' ') AS E0621_CTA_CONT
      ,'00001' AS e0621_tip_impt
      ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS e0621_fec_mes
      ,rpad(cargabal.bcra , 40, ' ') AS E0621_AGRCTACB
      ,case
         when substr (trim(cargabal.cuenta),1,2) = '32' then
           case
             when rtra_reg_eco.nominal_value  >= 0 then -- cambiamos el signo
               concat("-", lpad(rtra_reg_eco.nominal_value,16,'0'))
             else
               concat("+",lpad(regexp_replace(rtra_reg_eco.nominal_value,"-",""),16,'0'))
           end
         else
           case
             when rtra_reg_eco.market_value >= 0 then
               concat("-", lpad(rtra_reg_eco.market_value,16,'0'))
             else
               concat("+",lpad(regexp_replace(rtra_reg_eco.market_value,"-",""),16,'0'))
           end
       end as E0621_IMPORTH
      ,case
         when trim(upper(rtra_reg_eco.currency)) = 'ARD' THEN
           'USD'
         when trim(upper(rtra_reg_eco.currency)) = 'ARE' THEN
           'EUR'
         else
           lpad(substr(trim(rtra_reg_eco.currency),1 ,3 ), 3, ' ')
       end as E0621_CODDIV
      ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as e0621_fecultmo
      ,lpad(' ', 40, ' ') as E0621_CENTCTBL
      ,rpad(cargabal.cuenta , 40, ' ') as E0621_CTACGBAL
 from bi_corp_staging.cargabal_rtra  cargabal
 inner join tactico_rtra rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
 --tabla test normalizacion
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source = 'tactico-rtra'
       and nc.id_cto_source = concat_ws('_', trim(cargabal.cuenta),trim(cargabal.dealstamp))
  where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
UNION ALL
select pc.e0621_feoperac,
       pc.e0621_s1emp,
       pc.e0621_contra1,
       pc.e0621_cta_cont,
       pc.e0621_tip_impt,
       pc.e0621_fec_mes,
       pc.e0621_agrctacb,
       pc.e0621_importh,
       pc.e0621_coddiv,
       pc.e0621_fecultmo,
       pc.e0621_centctbl,
       pc.e0621_ctacgbal
  from bi_corp_bdr.jm_posic_contr pc
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source != 'tactico-rtra'
       and nc.id_cto_bdr = pc.e0621_contra1
 where pc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
;