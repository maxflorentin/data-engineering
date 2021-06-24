set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtcpc AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtcpc
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtcpc_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcpc c
   INNER JOIN max_part_bgdtcpc m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtcpc_stock s
   INNER JOIN max_part_bgdtcpc ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtcpc_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcpc_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtcpc PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.cpc_concepto,a.cpc_concepto)  			as cpc_concepto,
COALESCE(b.cpc_comision,a.cpc_comision)  			as cpc_comision,
COALESCE(b.cpc_canal,a.cpc_canal)  			as cpc_canal,
COALESCE(b.cpc_fecha_desde,a.cpc_fecha_desde)  			as cpc_fecha_desde,
COALESCE(b.cpc_fecha_hasta,a.cpc_fecha_hasta)  			as cpc_fecha_hasta,
COALESCE(b.cpc_entidad_umo,a.cpc_entidad_umo)  			as cpc_entidad_umo,
COALESCE(b.cpc_centro_umo,a.cpc_centro_umo)  			as cpc_centro_umo,
COALESCE(b.cpc_userid_umo,a.cpc_userid_umo)  			as cpc_userid_umo,
COALESCE(b.cpc_netname_umo,a.cpc_netname_umo)  			as cpc_netname_umo,
COALESCE(b.cpc_timest_umo,a.cpc_timest_umo)  			as cpc_timest_umo
from bgdtcpc_stock_tmp a
full outer join bgdtcpc_update_tmp b on (
a.cpc_concepto = b.cpc_concepto AND
a.cpc_comision = b.cpc_comision AND
a.cpc_canal = b.cpc_canal AND
a.cpc_fecha_desde = b.cpc_fecha_desde  );