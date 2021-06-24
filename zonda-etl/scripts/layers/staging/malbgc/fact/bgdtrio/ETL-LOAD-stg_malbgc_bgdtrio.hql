set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


WITH max_part_bgdtrio AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtrio
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtrio_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtrio c
   INNER JOIN max_part_bgdtrio m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtrio_stock s
   INNER JOIN max_part_bgdtrio ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtrio_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtrio_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtrio PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.rio_plan,a.rio_plan)  			as rio_plan,
COALESCE(b.rio_concepto,a.rio_concepto)  			as rio_concepto,
COALESCE(b.rio_tip_operacion,a.rio_tip_operacion)  			as rio_tip_operacion,
COALESCE(b.rio_zona,a.rio_zona)  			as rio_zona,
COALESCE(b.rio_divisa,a.rio_divisa)  			as rio_divisa,
COALESCE(b.rio_fecha_desde,a.rio_fecha_desde)  			as rio_fecha_desde,
COALESCE(b.rio_fecha_hasta,a.rio_fecha_hasta)  			as rio_fecha_hasta,
COALESCE(b.rio_comision,a.rio_comision)  			as rio_comision,
COALESCE(b.rio_entidad_umo,a.rio_entidad_umo)  			as rio_entidad_umo,
COALESCE(b.rio_centro_umo,a.rio_centro_umo)  			as rio_centro_umo,
COALESCE(b.rio_userid_umo,a.rio_userid_umo)  			as rio_userid_umo,
COALESCE(b.rio_netname_umo,a.rio_netname_umo)  			as rio_netname_umo,
COALESCE(b.rio_timest_umo,a.rio_timest_umo)  			as rio_timest_umo
from bgdtrio_stock_tmp a
full outer join bgdtrio_update_tmp b on
(a.rio_plan = b.rio_plan AND
a.rio_concepto = b.rio_concepto AND
a.rio_tip_operacion = b.rio_tip_operacion AND
a.rio_zona = b.rio_zona AND
a.rio_divisa = b.rio_divisa AND
a.rio_fecha_desde = b.rio_fecha_desde );