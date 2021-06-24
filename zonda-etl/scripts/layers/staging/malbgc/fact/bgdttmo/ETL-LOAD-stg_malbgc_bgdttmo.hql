set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdttmo AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdttmo
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdttmo_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdttmo c
   INNER JOIN max_part_bgdttmo m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdttmo_stock s
   INNER JOIN max_part_bgdttmo ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdttmo_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdttmo_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdttmo PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.tmo_concepto,a.tmo_concepto)  			as tmo_concepto,
COALESCE(b.tmo_comision,a.tmo_comision)  			as tmo_comision,
COALESCE(b.tmo_fecha_desde,a.tmo_fecha_desde)  			as tmo_fecha_desde,
COALESCE(b.tmo_codigo,a.tmo_codigo)  			as tmo_codigo,
COALESCE(b.tmo_fecha_hasta,a.tmo_fecha_hasta)  			as tmo_fecha_hasta,
COALESCE(b.tmo_entidad_umo,a.tmo_entidad_umo)  			as tmo_entidad_umo,
COALESCE(b.tmo_centro_umo,a.tmo_centro_umo)  			as tmo_centro_umo,
COALESCE(b.tmo_userid_umo,a.tmo_userid_umo)  			as tmo_userid_umo,
COALESCE(b.tmo_netname_umo,a.tmo_netname_umo)  			as tmo_netname_umo,
COALESCE(b.tmo_timest_umo,a.tmo_timest_umo)  			as tmo_timest_umo
from bgdttmo_stock_tmp a
full outer join bgdttmo_update_tmp b on
 (a.tmo_concepto = b.tmo_concepto AND
a.tmo_comision = b.tmo_comision AND
a.tmo_fecha_desde = b.tmo_fecha_desde AND
a.tmo_codigo = b.tmo_codigo );
