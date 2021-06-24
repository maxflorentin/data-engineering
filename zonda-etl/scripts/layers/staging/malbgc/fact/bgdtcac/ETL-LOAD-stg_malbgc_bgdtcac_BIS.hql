set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtcac AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtcac
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}' ),
     bgdtcac_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcac c
   INNER JOIN max_part_bgdtcac m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtcac_stock s
   INNER JOIN max_part_bgdtcac ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtcac_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcac_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtcac PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates_BIS') }}')
SELECT COALESCE(b.cac_entidad,a.cac_entidad) AS cac_entidad,
       COALESCE(b.cac_centro_alta,a.cac_centro_alta) AS cac_centro_alta,
       COALESCE(b.cac_cuenta,a.cac_cuenta) AS cac_cuenta,
       COALESCE(b.cac_divisa,a.cac_divisa) AS cac_divisa,
       COALESCE(b.cac_campania,a.cac_campania) AS cac_campania,
       COALESCE(b.cac_fecha_desde,a.cac_fecha_desde) AS cac_fecha_desde,
       COALESCE(b.cac_fecha_hasta,a.cac_fecha_hasta) AS cac_fecha_hasta,
       COALESCE(b.cac_entidad_umo,a.cac_entidad_umo) AS cac_entidad_umo,
       COALESCE(b.cac_centro_umo,a.cac_centro_umo) AS cac_centro_umo,
       COALESCE(b.cac_userid_umo,a.cac_userid_umo) AS cac_userid_umo,
       COALESCE(b.cac_netname_umo,a.cac_netname_umo) AS cac_netname_umo,
       COALESCE(b.cac_timest_umo,a.cac_timest_umo) AS cac_timest_umo
FROM bgdtcac_stock_tmp a
FULL OUTER JOIN bgdtcac_update_tmp b ON (a.cac_entidad = b.cac_entidad
                                         AND a.cac_centro_alta = b.cac_centro_alta
                                         AND a.cac_cuenta = b.cac_cuenta
                                         AND a.cac_divisa = b.cac_divisa
                                         AND a.cac_campania = b.cac_campania
                                         AND a.cac_fecha_desde = b.cac_fecha_desde);
