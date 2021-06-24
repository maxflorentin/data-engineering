set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtban AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtban
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtban_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtban c
   INNER JOIN max_part_bgdtban m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtban_stock s
   INNER JOIN max_part_bgdtban ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtban_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtban_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtban PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT COALESCE(b.ban_plan,a.ban_plan) AS ban_plan,
       COALESCE(b.ban_concepto,a.ban_concepto) AS ban_concepto,
       COALESCE(b.ban_titularidad,a.ban_titularidad) AS ban_titularidad,
       COALESCE(b.ban_red,a.ban_red) AS ban_red,
       COALESCE(b.ban_fecha_desde,a.ban_fecha_desde) AS ban_fecha_desde,
       COALESCE(b.ban_fecha_hasta,a.ban_fecha_hasta) AS ban_fecha_hasta,
       COALESCE(b.ban_comision,a.ban_comision) AS ban_comision,
       COALESCE(b.ban_entidad_umo,a.ban_entidad_umo) AS ban_entidad_umo,
       COALESCE(b.ban_centro_umo,a.ban_centro_umo) AS ban_centro_umo,
       COALESCE(b.ban_userid_umo,a.ban_userid_umo) AS ban_userid_umo,
       COALESCE(b.ban_netname_umo,a.ban_netname_umo) AS ban_netname_umo,
       COALESCE(b.ban_timest_umo,a.ban_timest_umo) AS ban_timest_umo
FROM bgdtban_stock_tmp a
FULL OUTER JOIN bgdtban_update_tmp b ON (a.ban_plan = b.ban_plan
                                         AND a.ban_concepto = b.ban_concepto
                                         AND a.ban_titularidad = b.ban_titularidad
                                         AND a.ban_red = b.ban_red
                                         AND a.ban_fecha_desde = b.ban_fecha_desde);