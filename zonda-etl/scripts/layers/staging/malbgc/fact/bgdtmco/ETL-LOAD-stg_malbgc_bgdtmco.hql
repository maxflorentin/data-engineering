set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtmco AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtmco
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtmco_stock_tmp AS
  (SELECT c.*,
       row_number()
       over(partition BY c.mco_num_convenio, c.mco_convenio, c.mco_producto, c.mco_subprodu,
       c.mco_suscriptor
ORDER BY c.mco_divisa) AS rownum
   FROM bi_corp_staging.malbgc_bgdtmco c
   INNER JOIN max_part_bgdtmco m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT s.*,
       row_number()
       over(partition BY s.mco_num_convenio, s.mco_convenio, s.mco_producto, s.mco_subprodu,
       s.mco_suscriptor
ORDER BY s.mco_divisa) AS rownum
   FROM bi_corp_staging.malbgc_bgdtmco_stock s
   INNER JOIN max_part_bgdtmco ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtmco_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtmco_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtmco PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
        COALESCE(b.mco_num_convenio,a.mco_num_convenio)  			as mco_num_convenio,
        COALESCE(b.mco_convenio,a.mco_convenio)  			as mco_convenio,
        COALESCE(b.mco_producto,a.mco_producto)  			as mco_producto,
        COALESCE(b.mco_subprodu,a.mco_subprodu)  			as mco_subprodu,
        COALESCE(b.mco_suscriptor,a.mco_suscriptor)  			as mco_suscriptor,
        COALESCE(b.mco_entidad,a.mco_entidad)  			as mco_entidad,
        COALESCE(b.mco_centro_alta,a.mco_centro_alta)  			as mco_centro_alta,
        COALESCE(b.mco_cuenta,a.mco_cuenta)  			as mco_cuenta,
        COALESCE(b.mco_divisa,a.mco_divisa)  			as mco_divisa,
        COALESCE(b.mco_indesta,a.mco_indesta)  			as mco_indesta,
        COALESCE(b.mco_fecha_est,a.mco_fecha_est)  			as mco_fecha_est,
        COALESCE(b.mco_num_mes,a.mco_num_mes)  			as mco_num_mes,
        COALESCE(b.mco_saldo_medio,a.mco_saldo_medio)  			as mco_saldo_medio,
        COALESCE(b.mco_saldo_med_cv,a.mco_saldo_med_cv)  			as mco_saldo_med_cv,
        COALESCE(b.mco_tipo_convenio,a.mco_tipo_convenio)  			as mco_tipo_convenio,
        COALESCE(b.mco_num_asociados,a.mco_num_asociados)  			as mco_num_asociados,
        COALESCE(b.mco_dias_vigencia,a.mco_dias_vigencia)  			as mco_dias_vigencia,
        COALESCE(b.mco_entidad_umo,a.mco_entidad_umo)  			as mco_entidad_umo,
        COALESCE(b.mco_centro_umo,a.mco_centro_umo)  			as mco_centro_umo,
        COALESCE(b.mco_userid_umo,a.mco_userid_umo)  			as mco_userid_umo,
        COALESCE(b.mco_netname_umo,a.mco_netname_umo)  			as mco_netname_umo,
        COALESCE(b.mco_timest_umo,a.mco_timest_umo)  			as mco_timest_umo
from bgdtmco_stock_tmp a
full outer join bgdtmco_update_tmp b on
(a.mco_num_convenio = b.mco_num_convenio);