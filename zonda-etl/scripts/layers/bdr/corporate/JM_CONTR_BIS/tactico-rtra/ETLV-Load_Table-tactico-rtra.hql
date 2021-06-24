set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

with tactico_rtra as (
                       select distinct *
                         from
                             (select cpty,
                                     industry,
                                     dealstamp,
                                     maturity_date,
                                     instrument ,
                                     trade_date,
                                     currency
                                from bi_corp_staging.rtra_regulatorio_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                              select cpty,
                                     industry,
                                     dealstamp,
                                     maturity_date,
                                     instrument,
                                     trade_date,
                                     currency
                                from bi_corp_staging.rtra_economico_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA') a
                       )
insert overwrite table bi_corp_bdr.test_jm_contr_bis --tabla test
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
SELECT  distinct '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}'  AS G4095_FEOPERAC,
                '23100' AS G4095_S1EMP,
                lpad(nc.id_cto_bdr,9,'0') AS G4095_CONTRA1,
                rpad('', 40, ' ') AS G4095_IDSUCUR,
                lpad('0', 5, '0') AS G4095_ID_PAIS,
                case 
                  when rtra_reg_eco.instrument ='FIBREP' then 
                    lpad('193', 5, '0')
                  when rtra_reg_eco.instrument ='FXCS' then
                    lpad('31', 5, '0')
                  when rtra_reg_eco.instrument ='FXF' then
                    lpad('162', 5, '0')
                  else
                    lpad('0', 5, '0')
                end AS G4095_ID_PROD,
                case 
                  when rtra_reg_eco.instrument ='FIBREP' then 
                    '61026'
                  when rtra_reg_eco.instrument ='FXCS' then
                    '61025'
                  when rtra_reg_eco.instrument ='FXF' then
                    '61021'
                  else
                    lpad('0', 5, '0')
                end AS G4095_IDPRO_LC,
                nvl(rtra_reg_eco.maturity_date,'0001-01-01') AS G4095_FECVENTO,
                nvl(rtra_reg_eco.maturity_date,'0001-01-01') AS G4095_FECVE2,
                nvl(rtra_reg_eco.trade_date,'0001-01-01') AS G4095_FECHAPER, --duda
                '9999-12-31' as G4095_FECCANCE,
                '0001-01-01' AS G4095_FECREES,
                '0001-01-01' AS G4095_FECREFI,
                '0001-01-01' AS G4095_FECNOVAC,
                lpad('0', 5, '0') AS G4095_IDFINALI,
                lpad('0', 5, '0') AS G4095_IDFINALD,
                lpad('0', 9, '0') AS G4095_CONTRA2,
                case
                  when trim(upper(rtra_reg_eco.currency)) = 'ARD' THEN
                    'USD'
                  when trim(upper(rtra_reg_eco.currency)) = 'ARE' THEN
                    'EUR'
                  else
                    lpad(substr(trim(rtra_reg_eco.currency),1 ,3 ), 3, ' ')
                end AS G4095_CODDIV,
                lpad('0', 5, '0') AS G4095_ID_CANAL,
                lpad('0', 5, '0') AS G4095_ID_CANA2,
                lpad('0', 5, '0') AS G4095_ID_NATUR,
                lpad('0', 5, '0') AS G4095_ID_NSUBY,
                ' ' AS G4095_INDLIM,
                ' ' AS G4095_INDAVA,
                ' ' AS G4095_IND_TITU,
                ' ' AS G4095_INDDEUD,
                ' ' AS G4095_INDDEUD2,
                lpad('0', 5, '0') as G4095_TIP_INTE,
                lpad('9', 5, '9') AS G4095_IDEMIS,
                lpad(' ', 40, ' ') AS G4095_IDEMISI,
                lpad('0', 9, '0') AS G4095_IDNETING,
                lpad('0', 9, '0') AS G4095_IDCOLATE,
                '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS G4095_FECULTMO,
                lpad('0', 9, '0') AS G4095_VENCORIG,
                lpad('0', 5, '0') AS G4095_DEUDPREL,
                '9999-12-31' AS G4095_FECENTVI,
                lpad('9', 5, '9') AS G4095_IDPROLC2
 from bi_corp_staging.cargabal_rtra  cargabal     
 inner join tactico_rtra rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp 
 --tabla test normalizacion
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source = 'tactico-rtra'
       and nc.id_cto_source = concat_ws('_', trim(cargabal.cuenta),trim(cargabal.dealstamp))
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
 UNION ALL
 select g4095_feoperac,
       g4095_s1emp,
       g4095_contra1,
       g4095_idsucur,
       g4095_id_pais,
       g4095_id_prod,
       g4095_idpro_lc,
       g4095_fecvento,
       g4095_fecve2,
       g4095_fechaper,
       g4095_feccance,
       g4095_fecrees,
       g4095_fecrefi,
       g4095_fecnovac,
       g4095_idfinali,
       g4095_idfinald,
       g4095_contra2,
       g4095_coddiv,
       g4095_id_canal,
       g4095_id_cana2,
       g4095_id_natur,
       g4095_id_nsuby,
       g4095_indlim,
       g4095_indava,
       g4095_ind_titu,
       g4095_inddeud,
       g4095_inddeud2,
       g4095_tip_inte,
       g4095_idemis,
       g4095_idemisi,
       g4095_idneting,
       g4095_idcolate,
       g4095_fecultmo,
       g4095_vencorig,
       g4095_deudprel,
       g4095_fecentvi,
       g4095_idprolc2
  from bi_corp_bdr.jm_contr_bis cb
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source != 'tactico-rtra'
       and nc.id_cto_bdr = cb.g4095_contra1
 where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'

;