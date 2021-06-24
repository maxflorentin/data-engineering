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
                                     cpty_countryname,
                                     Buysell,
                                     maturity_date
                                from bi_corp_staging.rtra_regulatorio_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA'
                              union all
                              select cpty,
                                     industry,
                                     dealstamp,
                                     cpty_countryname,
                                     Buysell,
                                     maturity_date
                                from bi_corp_staging.rtra_economico_test
                               where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='next_month_first_day', dag_id='BDR_LOAD_perimetro-rtra') }}'
                                 and (instrument in ('FXF') or (instrument = 'FXCS' and upper(trim(deal_table)) ='MX3_LT') or (instrument = 'FIBREP' and upper(trim(deal_table)) ='SAM'))
                                 and cpty != 'CLIENTE FICTICIO ARGENTINA') a
                       )
insert overwrite table bi_corp_bdr.test_jm_contr_otros --tabla test
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}')
select DISTINCT
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' AS E0623_FEOPERAC,
       lpad('23100',5,'0') as E0623_S1EMP,
       lpad(nc.id_cto_bdr,9,'0') as E0623_CONTRA1,
       '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as E0623_FEC_MES,
       case when nvl(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(rtra_reg_eco.maturity_date, '9999-12-31') as varchar(10)),'yyyy-MM-dd'))),
                              to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as varchar(10)),'yyyy-MM-dd')))
                             )
                    ,0) <  0
            then
            	'+0000000000000000'
            else
              	lpad(concat(datediff(to_date(from_unixtime(UNIX_TIMESTAMP(cast(nvl(rtra_reg_eco.maturity_date, '9999-12-31') as varchar(10)),'yyyy-MM-dd'))),
                              to_date(from_unixtime(UNIX_TIMESTAMP(cast( '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_perimetro-rtra') }}' as varchar(10)),'yyyy-MM-dd')))
                             ),
                    '000000'),17,'0')
              end as E0623_VCTO_RES,
       lpad('0', 17, '0')  as E0623_VCTO_PON,
       lpad('0', 5,'0')  as E0623_IDSUBPRD,
       lpad('0', 5, '0') AS  E0623_TIP_LIQU,
       lpad('0', 17, '0') AS E0623_LIQ_PZO,
       lpad('0', 5, '0') AS  E0623_TIP_AMOR,
       lpad('0', 17, '0') AS E0623_AMOR_PZO,
       lpad('0', 5, '0') AS  E0623_AMOR_SIS,
       lpad('0', 5, '0') AS  E0623_CTB_SITU,
       lpad('1', 5, '0') AS  E0623_GEST_SIT,
       lpad('1', 5, '0') AS  E0623_GES2_SIT,
       lpad('0', 9, '0') AS  E0623_ATAEMAX,
       lpad('0', 9, '0') AS  E0623_TIP_INT,
       concat('+', lpad('0', 16, '0')) AS E0623_IMP1LIMI,
       concat('+', lpad('0', 16, '0')) AS E0623_ALIMACT,
       concat('+', lpad('0', 16, '0')) AS E0623_IMPORTH,
       lpad('2', 5, '0') AS E0623_INV_NEGO,
       concat('+', lpad('0', 16, '0')) AS    E0623_IPROVISI,
       concat('+', lpad('0', 16, '0')) AS    E0623_IPROVIS1,
       '0001-01-01' AS E0623_FECULTMO,
       lpad('0', 5, '0') AS E0623_ESTADTRJ,
       lpad('0', 17, '0') AS E0623_INACTRJ,
       lpad('0', 5, '0') AS E0623_UNNT,
       lpad('0', 5, '0') AS E0623_UNNTS,
       lpad('0', 5, '0') AS E0623_UNNV,
       lpad('0', 5, '0') AS E0623_UNNVS,
       ' ' AS  E0623_RGOSUB,
       '0001-01-01' AS E0623_FECINCAR,
       '0001-01-01' AS E0623_FECFICAR,
       ' ' as E0623_INTNEG,
       lpad('0', 5, '0') AS E0623_MTVALTA,
       ' ' AS  E0623_INDSUBRO,
       lpad('0', 5, '0') AS  E0623_TIP_INTE,
       lpad('0', 9, '0') AS  E0623_DIFERNCI,
       concat('+', lpad('0', 16, '0')) AS E0623_IMPRTCUO,
       ' ' AS E0623_INDSEGUR,
       ' ' AS E0623_AMORTPAR,
       '9999-12-31' AS E0623_FECIMPAG,
       concat('-', lpad('0', 16, '0')) AS    E0623_IMPPRIMP,
       '9999-12-31' AS E0623_FHPRIMPG,
       concat('-', lpad('0', 16, '0')) AS    E0623_IMPIMPNR,
       lpad('0', 5, '0') AS E0623_ESTPRINM,
       lpad('0', 5, '0') AS  E0623_EXCLCTO,
       lpad('0', 9, '0') AS  E0623_CUOTIMPA,
       lpad('0', 17, '0') AS E0623_LIMOCULT,
       lpad('0', 5, '0') AS E0623_CODIMPHI,
       ' ' AS  E0623_INDEUCON,
       lpad('0', 5, '0') AS E0623_TIPCAREN,
       concat('+', lpad('0', 16, '0')) AS E0623_CUOTPRES,
       nvl(rtra_reg_eco.Buysell,' ')  as E0623_IBUYSELL,
       lpad('0', 9, '0') AS  E0623_SUTIPINT,
       lpad('0', 9, '0') AS  E0623_TETIPINT,
       lpad('0', 5, '0') AS  E0623_TIPSUELO,
       lpad('0', 5, '0') AS  E0623_TIPTECHO,
       lpad('0', 5, '0') AS  E0623_PLREVTIN,
       '9999-12-31' AS E0623_FECCUOTA,
       lpad('0', 9, '0') AS E0623_NUDIAATR,
       lpad('0', 5, '0') AS  E0623_SEGPLLIM,
       concat('+', lpad('0', 16, '0')) AS    E0623_VOLTRANS,
       ' ' AS  E0623_MARCAUTI,
       lpad('0', 5, '0') AS  E0623_TOPDEALE,
       '9999-12-31' AS E0623_FECREPRE,
       '23100' as E0623_EMPREPOR,
       concat('+', lpad('0', 16, '0')) AS    E0623_IMPCUIMP,
       lpad('0', 9, '0') AS E0623_TIPINEFE,
       lpad('0', 5, '0') AS E0623_FORPGACT,
       '0001-01-01' AS E0623_FINIUTCT,
       '0001-01-01' AS E0623_FFINUTCT
  from bi_corp_staging.cargabal_rtra  cargabal
 inner join tactico_rtra rtra_reg_eco on rtra_reg_eco.dealstamp = cargabal.dealstamp
 --tabla test normalizacion
 inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source = 'tactico-rtra'
       and nc.id_cto_source = concat_ws('_', trim(cargabal.cuenta),trim(cargabal.dealstamp))
 where cargabal.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_cargabal_rtra', dag_id='BDR_LOAD_perimetro-rtra') }}'
 UNION ALL
 select e0623_feoperac,
        e0623_s1emp,
        e0623_contra1,
        e0623_fec_mes,
        e0623_vcto_res,
        e0623_vcto_pon,
        e0623_idsubprd,
        e0623_tip_liqu,
        e0623_liq_pzo,
        e0623_tip_amor,
        e0623_amor_pzo,
        e0623_amor_sis,
        e0623_ctb_situ,
        e0623_gest_sit,
        e0623_ges2_sit,
        e0623_ataemax,
        e0623_tip_int,
        e0623_imp1limi,
        e0623_alimact,
        e0623_importh,
        e0623_inv_nego,
        e0623_iprovisi,
        e0623_iprovis1,
        e0623_fecultmo,
        e0623_estadtrj,
        e0623_inactrj,
        e0623_unnt,
        e0623_unnts,
        e0623_unnv,
        e0623_unnvs,
        e0623_rgosub,
        e0623_fecincar,
        e0623_fecficar,
        e0623_intneg,
        e0623_mtvalta,
        e0623_indsubro,
        e0623_tip_inte,
        e0623_difernci,
        e0623_imprtcuo,
        e0623_indsegur,
        e0623_amortpar,
        e0623_fecimpag,
        e0623_impprimp,
        e0623_fhprimpg,
        e0623_impimpnr,
        e0623_estprinm,
        e0623_exclcto,
        e0623_cuotimpa,
        e0623_limocult,
        e0623_codimphi,
        e0623_indeucon,
        e0623_tipcaren,
        e0623_cuotpres,
        e0623_ibuysell,
        e0623_sutipint,
        e0623_tetipint,
        e0623_tipsuelo,
        e0623_tiptecho,
        e0623_plrevtin,
        e0623_feccuota,
        e0623_nudiaatr,
        e0623_segpllim,
        e0623_voltrans,
        e0623_marcauti,
        e0623_topdeale,
        e0623_fecrepre,
        e0623_emprepor,
        e0623_impcuimp,
        e0623_tipinefe,
        e0623_forpgact,
        e0623_finiutct,
        e0623_ffinutct
   from bi_corp_bdr.jm_contr_otros co
  inner join bi_corp_bdr.test_normalizacion_id_contratos nc on nc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
	   and nc.source != 'tactico-rtra'
       and nc.id_cto_bdr = co.e0623_contra1
 where co.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_perimetro-rtra') }}'
;