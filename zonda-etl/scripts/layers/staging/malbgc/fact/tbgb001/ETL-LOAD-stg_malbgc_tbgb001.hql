set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_tbgb001 AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_tbgb001
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     tbgb001_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_tbgb001 c
   INNER JOIN max_part_tbgb001 m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_tbgb001_stock s
   INNER JOIN max_part_tbgb001 ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     tbgb001_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_tbgb001_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_tbgb001 PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.001_suscriptor,a.001_suscriptor)  			as 001_suscriptor,
COALESCE(b.001_des_suscriptor,a.001_des_suscriptor)  			as 001_des_suscriptor,
COALESCE(b.001_tpo_suscriptor,a.001_tpo_suscriptor)  			as 001_tpo_suscriptor,
COALESCE(b.001_entidad,a.001_entidad)  			as 001_entidad,
COALESCE(b.001_centro_alta,a.001_centro_alta)  			as 001_centro_alta,
COALESCE(b.001_cuenta,a.001_cuenta)  			as 001_cuenta,
COALESCE(b.001_divisa,a.001_divisa)  			as 001_divisa,
COALESCE(b.001_entidad_umo,a.001_entidad_umo)  			as 001_entidad_umo,
COALESCE(b.001_centro_umo,a.001_centro_umo)  			as 001_centro_umo,
COALESCE(b.001_userid_umo,a.001_userid_umo)  			as 001_userid_umo,
COALESCE(b.001_netname_umo,a.001_netname_umo)  			as 001_netname_umo,
COALESCE(b.001_timest_umo,a.001_timest_umo)  			as 001_timest_umo
from tbgb001_stock_tmp a
full outer join tbgb001_update_tmp b on (a.001_suscriptor = b.001_suscriptor );