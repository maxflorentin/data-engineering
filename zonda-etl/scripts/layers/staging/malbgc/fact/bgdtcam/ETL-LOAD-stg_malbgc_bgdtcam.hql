set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


WITH max_part_bgdtmax AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtcam
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtcam_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcam c
   INNER JOIN max_part_bgdtmax m ON c.partition_date = m.previos_date_from
    WHERE c.partition_date = m.previos_date_from
  UNION ALL
SELECT *
   FROM bi_corp_staging.malbgc_bgdtcam_stock s
   INNER JOIN max_part_bgdtmax ms ON s.partition_date = ms.previos_date_from
    WHERE s.partition_date = ms.previos_date_from),
     bgdtcam_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcam_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtcam PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT COALESCE(b.cam_campania,a.cam_campania) AS cam_campania,
       COALESCE(b.cam_fecha_desde,a.cam_fecha_desde) AS cam_fecha_desde,
       COALESCE(b.cam_producto,a.cam_producto) AS cam_producto,
       COALESCE(b.cam_subprodu,a.cam_subprodu) AS cam_subprodu,
       COALESCE(b.cam_fecha_hasta,a.cam_fecha_hasta) AS cam_fecha_hasta,
       COALESCE(b.cam_plazo,a.cam_plazo) AS cam_plazo,
       COALESCE(b.cam_fecha_vto,a.cam_fecha_vto) AS cam_fecha_vto,
       COALESCE(b.cam_tarifa,a.cam_tarifa) AS cam_tarifa,
       COALESCE(b.cam_period_liq,a.cam_period_liq) AS cam_period_liq,
       COALESCE(b.cam_divisa,a.cam_divisa) AS cam_divisa,
       COALESCE(b.cam_plan,a.cam_plan) AS cam_plan,
       COALESCE(b.cam_descripcion,a.cam_descripcion) AS cam_descripcion,
       COALESCE(b.cam_clase_liq,a.cam_clase_liq) AS cam_clase_liq,
       COALESCE(b.cam_clase_taf,a.cam_clase_taf) AS cam_clase_taf,
       COALESCE(b.cam_periodo_tar,a.cam_periodo_tar) AS cam_periodo_tar,
       COALESCE(b.cam_indesta,a.cam_indesta) AS cam_indesta,
       COALESCE(b.cam_fecha_estado,a.cam_fecha_estado) AS cam_fecha_estado,
       COALESCE(b.cam_entidad_umo,a.cam_entidad_umo) AS cam_entidad_umo,
       COALESCE(b.cam_centro_umo,a.cam_centro_umo) AS cam_centro_umo,
       COALESCE(b.cam_userid_umo,a.cam_userid_umo) AS cam_userid_umo,
       COALESCE(b.cam_netname_umo,a.cam_netname_umo) AS cam_netname_umo,
       COALESCE(b.cam_timest_umo,a.cam_timest_umo) AS cam_timest_umo
FROM bgdtcam_stock_tmp a
FULL OUTER JOIN bgdtcam_update_tmp b ON (a.cam_campania = b.cam_campania
                                         AND a.cam_fecha_desde = b.cam_fecha_desde
                                         AND a.cam_producto = b.cam_producto
                                         AND a.cam_subprodu = b.cam_subprodu);
