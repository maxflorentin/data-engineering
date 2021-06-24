set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtpco AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtpco
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtpco_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtpco c
   INNER JOIN max_part_bgdtpco m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtpco_stock s
   INNER JOIN max_part_bgdtpco ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtpco_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtpco_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtpco PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
    COALESCE(b.pco_plan,a.pco_plan)  			as pco_plan,
    COALESCE(b.pco_concepto,a.pco_concepto)  			as pco_concepto,
    COALESCE(b.pco_fecha_desde,a.pco_fecha_desde)  			as pco_fecha_desde,
    COALESCE(b.pco_fecha_hasta,a.pco_fecha_hasta)  			as pco_fecha_hasta,
    COALESCE(b.pco_mvtos_libr_riom,a.pco_mvtos_libr_riom)  			as pco_mvtos_libr_riom,
    COALESCE(b.pco_period_com,a.pco_period_com)  			as pco_period_com,
    COALESCE(b.pco_period_cobr,a.pco_period_cobr)  			as pco_period_cobr,
    COALESCE(b.pco_dia_nat_cobr,a.pco_dia_nat_cobr)  			as pco_dia_nat_cobr,
    COALESCE(b.pco_entidad_umo,a.pco_entidad_umo)  			as pco_entidad_umo,
    COALESCE(b.pco_centro_umo,a.pco_centro_umo)  			as pco_centro_umo,
    COALESCE(b.pco_userid_umo,a.pco_userid_umo)  			as pco_userid_umo,
    COALESCE(b.pco_netname_umo,a.pco_netname_umo)  			as pco_netname_umo,
    COALESCE(b.pco_timest_umo,a.pco_timest_umo)  			as pco_timest_umo
from bgdtpco_stock_tmp a
full outer join bgdtpco_update_tmp b on
(a.pco_plan = b.pco_plan AND
 a.pco_concepto = b.pco_concepto AND
  a.pco_fecha_desde = b.pco_fecha_desde );
