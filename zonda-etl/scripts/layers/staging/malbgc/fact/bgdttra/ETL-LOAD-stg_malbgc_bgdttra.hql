set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdttra AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdttra
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdttra_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdttra c
   INNER JOIN max_part_bgdttra m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdttra_stock s
   INNER JOIN max_part_bgdttra ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdttra_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdttra_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdttra PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.tra_concepto,a.tra_concepto)  			as tra_concepto,
COALESCE(b.tra_comision,a.tra_comision)  			as tra_comision,
COALESCE(b.tra_fecha_desde,a.tra_fecha_desde)  			as tra_fecha_desde,
COALESCE(b.tra_saldo_hasta,a.tra_saldo_hasta)  			as tra_saldo_hasta,
COALESCE(b.tra_fecha_hasta,a.tra_fecha_hasta)  			as tra_fecha_hasta,
COALESCE(b.tra_comi_im,a.tra_comi_im)  			as tra_comi_im,
COALESCE(b.tra_comi_po,a.tra_comi_po)  			as tra_comi_po,
COALESCE(b.tra_comi_max,a.tra_comi_max)  			as tra_comi_max,
COALESCE(b.tra_comi_min,a.tra_comi_min)  			as tra_comi_min,
COALESCE(b.tra_entidad_umo,a.tra_entidad_umo)  			as tra_entidad_umo,
COALESCE(b.tra_centro_umo,a.tra_centro_umo)  			as tra_centro_umo,
COALESCE(b.tra_userid_umo,a.tra_userid_umo)  			as tra_userid_umo,
COALESCE(b.tra_netname_umo,a.tra_netname_umo)  			as tra_netname_umo,
COALESCE(b.tra_timest_umo,a.tra_timest_umo)  			as tra_timest_umo
from bgdttra_stock_tmp a
full outer join bgdttra_update_tmp b on
(a.tra_concepto = b.tra_concepto AND
a.tra_comision = b.tra_comision AND
a.tra_fecha_desde = b.tra_fecha_desde AND
a.tra_saldo_hasta = b.tra_saldo_hasta );