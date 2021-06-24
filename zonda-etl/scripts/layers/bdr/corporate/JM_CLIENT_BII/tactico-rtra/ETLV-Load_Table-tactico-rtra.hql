set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
set hive.strict.checks.cartesian.product=false;

insert overwrite table bi_corp_bdr.test_jm_client_bii
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS G4093_FEOPERAC,
	   '23100' AS G4093_S1EMP,
	   lpad(cl.num_persona, 9, '0') AS G4093_IDNUMCLI,
	   '00002' AS G4093_TIP_PERS,
	   lpad(' ', 80, ' ') AS G4093_APNOMPER,
	   lpad(' ', 40, ' ') AS G4093_DATEXTO1,
	   lpad(' ', 40, ' ') AS G4093_DATEXTO2,
	   rpad(nvl(cl.cod_tip_documento, ''), 40, ' ') AS G4093_IDENTPER,
	   lpad(nvl(' ', ' '), 40, ' ') AS G4093_CODIDPER,
	   lpad('0', 9, '0') AS G4093_CLIE_GLO,
	   lpad(' ', 40, ' ') AS G4093_IDSUCUR,
	   '00001' AS G4093_CARTER,
	   lpad(nvl(mb2.cod_baremo_global, '0') , 5, '0') AS G4093_ID_PAIS,
	   lpad(nvl(mb9.cod_baremo_global, '0'), 5, '0') AS G4093_COD_SECT,
	   lpad(nvl(mb9.cod_baremo_local, '0'), 5, '0') AS G4093_COD_SEC2,
	   lpad( '0', 5, '0') AS G4093_COD_SEC3,
	   lpad(nvl(mb24.cod_baremo_global, '0'), 5, '0') AS G4093_CLISEGM,
	   lpad(nvl(mb24.cod_baremo_local, '0'), 5, '0') AS G4093_CLISEGL1,
	   rpad(nvl(cl.cod_segmento, ''), 40, ' ') AS G4093_TIPSEGL1,
	   lpad(nvl(cast (msc.cod_segmento_local_2 as string), '0'), 5, '0') AS G4093_CLISEGL2,
	   rpad(nvl(cast (msc.tipo_segmento_local_2 as string), ''), 40, ' ') AS G4093_TIPSEGL2,
	   '0001-01-01' AS G4093_FCHINI,
	   '9999-12-31' AS G4093_FCHFIN,
	   '0001-01-01' AS G4093_FECULTMO,
	   lpad('0', 5, '0') AS G4093_INDUSTRY,
	   lpad('0', 5, '0') AS G4093_EXCLCLI,
	   ' 'AS G4093_INDBCART,
	   '0001-01-01' AS G4093_FECNACIM,
	   '00032' AS G4093_PAISNEG,
	   lpad(' ', 40, ' ') AS G4093_CDPOSTAL,
	   lpad('0', 5, '0') AS G4093_TTO_ESPE, --queda pendiente!!!
	   '99999'  As G4093_GRA_VINC,
	   '99999' AS G4093_UTP_CLI,
	   '0001-01-01' AS G4093_FINIUTCL,
	   '9999-12-31' AS G4093_FFINUTCL
  from bi_corp_staging.cargabal_rtra  cargabal
 inner join (select distinct t.*
                from (select cpty,
                             industry,
                             dealstamp,
                             upper(cpty_countryname) cpty_countryname
                        from bi_corp_staging.rtra_regulatorio_test
                       where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                         and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                       union all
                      select cpty,
                             industry,
                             dealstamp,
                             upper(cpty_countryname) cpty_countryname
                        from bi_corp_staging.rtra_economico_test
                       where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                         and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                      ) t
              )rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
inner join bi_corp_bdr.perim_mdr_contraparte  mdr on trim(mdr.shortname) = trim(rtra_reg_eco.cpty) and mdr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
left join bi_corp_bdr.jm_client_bii cb on cb.g4093_idnumcli = lpad(mdr.nup,9,'0') and cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
inner join santander_business_risk_arda.clientes cl on mdr.nup = cl.num_persona and cl.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_clientes', dag_id='BDR_LOAD_perimetro-rtra') }}'
--baremos
LEFT JOIN bi_corp_bdr.baremos_local bl2 on bl2.desc_baremo_local = upper(rtra_reg_eco.cpty_countryname) and bl2.cod_negocio_local = '2'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb2 ON mb2.cod_baremo_local = bl2.cod_baremo_local and mb2.cod_negocio = 2
LEFT JOIN bi_corp_bdr.baremos_local bl9 on bl9.cod_baremo_alfanumerico_local = cast(cast(cl.cod_afip as int) as string) and bl9.cod_negocio_local = '9'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb9 on mb9.cod_baremo_local = bl9.cod_baremo_local and mb9.cod_negocio = 9
LEFT JOIN bi_corp_bdr.baremos_local bl24 on bl24.cod_baremo_alfanumerico_local = cl.cod_segmento and bl24.cod_negocio_local = '24'
LEFT JOIN bi_corp_bdr.map_baremos_global_local mb24 on mb24.cod_baremo_local = bl24.cod_baremo_local and mb24.cod_negocio = 24
LEFT JOIN bi_corp_bdr.map_sector_contable msc on msc.tipo_segmento_local_1 = bl24.cod_baremo_alfanumerico_local
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
   and cb.g4093_idnumcli is null
union all
select g4093_feoperac,
       g4093_s1emp,
       g4093_idnumcli,
       g4093_tip_pers,
       g4093_apnomper,
       g4093_datexto1,
       g4093_datexto2,
       g4093_identper,
       g4093_codidper,
       g4093_clie_glo,
       g4093_idsucur,
       g4093_carter,
       g4093_id_pais,
       g4093_cod_sect,
       g4093_cod_sec2,
       g4093_cod_sec3,
       g4093_clisegm,
       g4093_clisegl1,
       g4093_tipsegl1,
       g4093_clisegl2,
       g4093_tipsegl2,
       g4093_fchini,
       g4093_fchfin,
       g4093_fecultmo,
       g4093_industry,
       g4093_exclcli,
       g4093_indbcart,
       g4093_fecnacim,
       g4093_paisneg,
       g4093_cdpostal,
       g4093_tto_espe,
       g4093_gra_vinc,
       g4093_utp_cli,
       g4093_finiutcl,
       g4093_ffinutcl
  from bi_corp_bdr.jm_client_bii cb
 where cb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
;
