set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_bdr.jm_ead_contr
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}')
select t.G5519_FEOPERAC,
       t.G5519_S1EMP,
       t.G5519_CONTRA1,
       t.G5519_METAPLIC,
       t.G5519_FASECALC,
       case
         when nvl(t.G5519_MTM, 0) >= 0 then
           concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
         else
           concat('-', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
       end as G5519_MTM,
       case
         when nvl(t.G5519_MTM, 0) >= 0 then
           concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
         else
           concat('-', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
       end as G5519_EAD,
       lpad('0',17,'0') as G5519_ESPEPROV,
       G5519_FEOPERAC as G5519_FECULTMO,
       case
         when nvl(t.G5519_MTM, 0) >= 0 then
           concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_IMPNOMCT ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
         else
           concat('-', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_IMPNOMCT ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
       end as G5519_IMPNOMCT ,
       --lpad(regexp_replace(format_number(cast(regexp_replace(nvl(cast(regexp_replace(regexp_replace(t.G5519_IMPNOMCT,'\\.',''),'\\,','.') as double) ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),17,'0') as G5519_IMPNOMCT,
       lpad('0',17,'0') as G5519_ADDONBRU,
       lpad('0',17,'0') as G5519_ADDONNET,
       lpad('0',9,'0') as G5519_COEFREGU,
       lpad('0',5,'0') as G5519_METLIQUI,
       case
         when nvl(t.G5519_MTM_BRUT ,0) >= 0 then
           concat('+', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM_BRUT ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
         else
           concat('-', lpad(regexp_replace(format_number(cast(regexp_replace(nvl(t.G5519_MTM_BRUT ,0),'\\,','.') as double), 2) ,'\\,|\\.|\\-|\\+',''),16,'0'))
       end as G5519_MTM_BRUT
  from (select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Monthly') }}'  as G5519_FEOPERAC,
               '23100' as G5519_S1EMP,
               lpad(nc.id_cto_bdr,9,'0') as G5519_CONTRA1,
               case
                 when rtra.origen = 'regulatorio' then
                   lpad('1',5,'0')
                 else
                   lpad('2',5,'0')
               end G5519_METAPLIC,
               lpad('2',5,'0') G5519_FASECALC,
               cast(rtra.mtm_net as double) * cast(c.imp_cotizacion_ars as double) * esp.porcentaje as G5519_MTM,
               esp.suma_nominales as G5519_IMPNOMCT,
               cast(rtra.market_value as double) * cast(c.imp_cotizacion_ars as double) * esp.porcentaje as G5519_MTM_BRUT
          from (select cast(esp.suma_nominales as double) / (sum(cast(esp.suma_nominales as double) ) over (partition by esp.especie)) porcentaje,
                       esp.*
                  from bi_corp_staging.mmff_tactico_especie esp
                 where esp.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly') }}'
               ) esp
         inner join bi_corp_bdr.normalizacion_id_contratos nc
            on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Monthly') }}'
           and nc.id_cto_source = concat_ws('_',  trim(esp.cuenta), trim(esp.isin))
           and nc.`source` = 'mmff-tactico'
          left join (select * , 'regulatorio' as origen
                       from bi_corp_staging.rtra_regulatorio reg
                      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
                       union all
                     select * ,'economico' as origen
                       from bi_corp_staging.rtra_economico rte
                      where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
                    ) rtra
            on rtra.cpty_name != 'CLIENTE FICTICIO ARGENTINA'
           and rtra.dealstamp = concat(esp.especie,'-I')
          left join santander_business_risk_arda.cotizaciones_bcra c
            on c.data_date_part  = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cotizaciones_bcra', dag_id='BDR_LOAD_Tables-Monthly') }}'
           and c.cod_especie  = 'USD'
         where esp.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='bdr_max_partition_mmff_tactico_especie', dag_id='BDR_LOAD_Tables-Monthly') }}'
        ) t;