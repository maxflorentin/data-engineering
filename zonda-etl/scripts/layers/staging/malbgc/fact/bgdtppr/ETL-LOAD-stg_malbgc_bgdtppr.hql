set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtppr AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtppr
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtppr_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtppr c
   INNER JOIN max_part_bgdtppr m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtppr_stock s
   INNER JOIN max_part_bgdtppr ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtppr_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtppr_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtppr PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.ppr_plan,a.ppr_plan)  			as ppr_plan,
COALESCE(b.ppr_concepto,a.ppr_concepto)  			as ppr_concepto,
COALESCE(b.ppr_pco_ecu_per,a.ppr_pco_ecu_per)  			as ppr_pco_ecu_per,
COALESCE(b.ppr_pco_ecu_sop,a.ppr_pco_ecu_sop)  			as ppr_pco_ecu_sop,
COALESCE(b.ppr_fecha_desde,a.ppr_fecha_desde)  			as ppr_fecha_desde,
COALESCE(b.ppr_fecha_hasta,a.ppr_fecha_hasta)  			as ppr_fecha_hasta,
COALESCE(b.ppr_comision,a.ppr_comision)  			as ppr_comision,
COALESCE(b.ppr_entidad_umo,a.ppr_entidad_umo)  			as ppr_entidad_umo,
COALESCE(b.ppr_centro_umo,a.ppr_centro_umo)  			as ppr_centro_umo,
COALESCE(b.ppr_userid_umo,a.ppr_userid_umo)  			as ppr_userid_umo,
COALESCE(b.ppr_netname_umo,a.ppr_netname_umo)  			as ppr_netname_umo,
COALESCE(b.ppr_timest_umo,a.ppr_timest_umo)  			as ppr_timest_umo
from bgdtppr_stock_tmp a
full outer join bgdtppr_update_tmp b on
(a.ppr_plan = b.ppr_plan AND
a.ppr_concepto = b.ppr_concepto AND
a.ppr_pco_ecu_per = b.ppr_pco_ecu_per AND
a.ppr_pco_ecu_sop = b.ppr_pco_ecu_sop AND
a.ppr_fecha_desde = b.ppr_fecha_desde );