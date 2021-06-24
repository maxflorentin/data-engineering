set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtpab AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtpab
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtpab_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtpab c
   INNER JOIN max_part_bgdtpab m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtpab_stock s
   INNER JOIN max_part_bgdtpab ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtpab_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtpab_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtpab PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.pab_num_convenio,a.pab_num_convenio)  			as pab_num_convenio,
COALESCE(b.pab_concepto,a.pab_concepto)  			as pab_concepto,
COALESCE(b.pab_porc_suscriptor,a.pab_porc_suscriptor)  			as pab_porc_suscriptor,
COALESCE(b.pab_porc_entidad,a.pab_porc_entidad)  			as pab_porc_entidad,
COALESCE(b.pab_porc_cliente,a.pab_porc_cliente)  			as pab_porc_cliente,
COALESCE(b.pab_entidad,a.pab_entidad)  			as pab_entidad,
COALESCE(b.pab_centro_alta,a.pab_centro_alta)  			as pab_centro_alta,
COALESCE(b.pab_cuenta,a.pab_cuenta)  			as pab_cuenta,
COALESCE(b.pab_divisa,a.pab_divisa)  			as pab_divisa,
COALESCE(b.pab_indesta,a.pab_indesta)  			as pab_indesta,
COALESCE(b.pab_fecha_est,a.pab_fecha_est)  			as pab_fecha_est,
COALESCE(b.pab_entidad_umo,a.pab_entidad_umo)  			as pab_entidad_umo,
COALESCE(b.pab_centro_umo,a.pab_centro_umo)  			as pab_centro_umo,
COALESCE(b.pab_userid_umo,a.pab_userid_umo)  			as pab_userid_umo,
COALESCE(b.pab_netname_umo,a.pab_netname_umo)  			as pab_netname_umo,
COALESCE(b.pab_timest_umo,a.pab_timest_umo)  			as pab_timest_umo
from bgdtpab_stock_tmp a
full outer join bgdtpab_update_tmp b on
(
a.pab_num_convenio = b.pab_num_convenio AND
a.pab_concepto = b.pab_concepto );
