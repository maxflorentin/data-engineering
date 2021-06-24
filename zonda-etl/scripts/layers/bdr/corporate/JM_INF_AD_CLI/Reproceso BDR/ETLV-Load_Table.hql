set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE bi_corp_bdr.acreditacion_haberes AS
select DISTINCT lpad(`311_numper`, 9, '0') as num_persona
  from bi_corp_staging.malbgc_acreditaciones
 where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_acreditaciones', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
 ;

insert overwrite table bi_corp_bdr.jm_inf_ad_cli
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as H0780_FEOPERAC,
23100 H0780_S1EMP,
lpad(c.num_persona,9,'0') H0780_IDNUMCLI,
'00000' H0780_TIPINFRL,
'00000' H0780_TIPINFRG,
'+0000000000000000' H0780_IMPORTH,
'   ' H0780_CODDIV,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as H0780_FECHAACT,
'{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' as H0780_FECULTMO,
concat('+', lpad("0", 16, '0')) H0780_CUOTPRES,
'+0000000000000000' H0780_INGCLIEN,
'000000000' H0780_NUM_MTOS,
'0001-01-01' H0780_FECINGRE,
'000000000' H0780_RDEUDING,
'00000' H0780_TIP_EMPR,
'+0000000000000000' H0780_TOT_INGR,
'+0000000000000000' H0780_TOT_DEUF,
case when ah.num_persona is null then '0' else '1' end as h0780_flgacsld
  FROM santander_business_risk_arda.clientes c
LEFT JOIN bi_corp_bdr.acreditacion_haberes ah
      on lpad(c.num_persona,9,'0') = ah.num_persona
 WHERE c.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_clientes', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
   AND c.condicion_del_cliente = 'CLI';