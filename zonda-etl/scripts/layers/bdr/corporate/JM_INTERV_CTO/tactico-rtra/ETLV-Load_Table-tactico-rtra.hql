set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with tactico_rtra as (
                       select *
                         from
                             (select dealstamp,
                                     cpty,
                                     lpad('1',5,'0') origen,
                                     contribution
                                from bi_corp_staging.rtra_regulatorio_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                              select dealstamp,
                                     cpty,
                                     lpad('2',5,'0') origen,
                                     contribution
                                from bi_corp_staging.rtra_economico_test
                               where partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA') a
                       )
insert OVERWRITE TABLE bi_corp_bdr.test_jm_interv_cto
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
SELECT DISTINCT
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS G4128_FEOPERAC,
       '23100' AS G4128_S1EMP,
       lpad(nc.id_cto_bdr,9,'0') AS G4128_CONTRA1,
       '00001' AS G4128_TIPINTEV,
       '00015' AS G4128_TIPINTV2,
       '00000000001000000' AS G4128_NUMORDIN,
       lpad(mdr.nup,9,'0') AS G4128_IDNUMCLI,
       lpad('0', 5, '0') AS G4128_FORMINTV,
       lpad('0', 9, '0') AS G4128_PORPARTN,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS G4128_FECULTMO
  from bi_corp_staging.cargabal_rtra  cargabal
 inner join tactico_rtra rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
 inner join bi_corp_bdr.perim_mdr_contraparte  mdr on trim(mdr.shortname) = trim(rtra_reg_eco.cpty) and mdr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 --tabla test normalizacion
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source = 'tactico-rtra'
       and nc.id_cto_source = concat_ws('_', trim(cargabal.cuenta),trim(cargabal.dealstamp))
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
 UNION ALL
  select g4128_feoperac,
        g4128_s1emp,
        g4128_contra1,
        g4128_tipintev,
        g4128_tipintv2,
        g4128_numordin,
        g4128_idnumcli,
        g4128_formintv,
        g4128_porpartn,
        g4128_fecultmo
   from bi_corp_bdr.jm_interv_cto ic
  inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source != 'tactico-rtra'
       and nc.id_cto_bdr = ic.g4128_contra1
 where ic.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 ;