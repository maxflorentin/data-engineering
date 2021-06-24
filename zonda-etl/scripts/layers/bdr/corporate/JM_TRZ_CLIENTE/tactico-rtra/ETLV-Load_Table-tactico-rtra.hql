set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--drop de tabla temporal
DROP TABLE trz_client_cream;

-- cargamos en una temporal los clientes cream que no estan en la traza
CREATE TEMPORARY TABLE trz_client_cream AS
select distinct
        '23100' as G7015_S1EMP
       ,lpad(mdr.nup, 9, '0')  as G7015_IDNUMCLI
       ,nvl(cb.g4093_fchini ,'0001-01-01') as  G7015_FECDESDE
       ,lpad('10000', 9, '0')  as G7015_IDSISTE
       ,rpad(' ', 1, ' ') as G7015_TIP_PER
       ,lpad('0', 9, '0') G7015_CDG_PERS
       ,rpad(mdr.shortname, 50, ' ') as G7015_CODSISTE
       ,nvl(cb.g4093_fchfin,'0001-01-01')  as G7015_FEC_HAS
       ,'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}'  as G7015_FEC_MOD
  from bi_corp_staging.cargabal_rtra  cargabal
 inner join (select distinct t.*
                from (select cpty,
                             dealstamp
                        from bi_corp_staging.rtra_regulatorio_test
                       where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                         and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                       union all
                      select cpty,
                             dealstamp
                        from bi_corp_staging.rtra_economico_test
                       where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                         and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                      ) t
              )rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
 inner join bi_corp_bdr.perim_mdr_contraparte  mdr on trim(mdr.shortname) = trim(rtra_reg_eco.cpty)
                                                  and mdr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 inner join bi_corp_bdr.jm_client_bii cb on cb.g4093_idnumcli = lpad(mdr.nup,9,'0')
                                        and cb.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 left join bi_corp_bdr.jm_trz_cliente tz on tz.g7015_idnumcli = lpad(mdr.nup,9,'0')
                                        and tz.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
   and tz.g7015_idnumcli is null;

-- inserto todos los clientes de cream + el perimetro productivo sin los clientes de cream de la corrida anterior (en caso de reproceso)
insert overwrite table bi_corp_bdr.test_jm_trz_cliente
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select *
  from trz_client_cream
union all
select g7015_s1emp,
       g7015_idnumcli,
       g7015_fecdesde,
       g7015_idsiste,
       g7015_tip_per,
       g7015_cdg_pers,
       g7015_codsiste,
       g7015_fec_has,
       g7015_fec_mod
  from bi_corp_bdr.jm_trz_cliente tc
 where partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
   and not exists (select 1
                     from bi_corp_bdr.trz_cliente_control cc
                    where cc.nup = tc.G7015_IDNUMCLI
                      and cc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}' );

-- actualizo la tabla de control en caso de un reproceso
insert overwrite table bi_corp_bdr.trz_cliente_control
partition(partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select G7015_IDNUMCLI from trz_client_cream ;
