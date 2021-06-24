set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtplc AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtplc
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtplc_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtplc c
   INNER JOIN max_part_bgdtplc m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtplc_stock s
   INNER JOIN max_part_bgdtplc ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtplc_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtplc_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtplc PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.bgtcplc_plan,a.bgtcplc_plan)  			as bgtcplc_plan,
COALESCE(b.bgtcplc_concepto,a.bgtcplc_concepto)  			as bgtcplc_concepto,
COALESCE(b.bgtcplc_canal,a.bgtcplc_canal)  			as bgtcplc_canal,
COALESCE(b.bgtcplc_fecha_desde,a.bgtcplc_fecha_desde)  			as bgtcplc_fecha_desde,
COALESCE(b.bgtcplc_comision,a.bgtcplc_comision)  			as bgtcplc_comision,
COALESCE(b.bgtcplc_fecha_hasta,a.bgtcplc_fecha_hasta)  			as bgtcplc_fecha_hasta,
COALESCE(b.bgtcplc_entidad_umo,a.bgtcplc_entidad_umo)  			as bgtcplc_entidad_umo,
COALESCE(b.bgtcplc_centro_umo,a.bgtcplc_centro_umo)  			as bgtcplc_centro_umo,
COALESCE(b.bgtcplc_userid_umo,a.bgtcplc_userid_umo)  			as bgtcplc_userid_umo,
COALESCE(b.bgtcplc_netname_umo,a.bgtcplc_netname_umo)  			as bgtcplc_netname_umo,
COALESCE(b.bgtcplc_timest_umo,a.bgtcplc_timest_umo)  			as bgtcplc_timest_umo
from bgdtplc_stock_tmp a
full outer join bgdtplc_update_tmp b on
(a.bgtcplc_plan = b.bgtcplc_plan AND
a.bgtcplc_concepto = b.bgtcplc_concepto AND
a.bgtcplc_canal = b.bgtcplc_canal AND
a.bgtcplc_fecha_desde = b.bgtcplc_fecha_desde );
