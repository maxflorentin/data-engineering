set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtcom AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtcom
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtcom_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcom c
   INNER JOIN max_part_bgdtcom m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtcom_stock s
   INNER JOIN max_part_bgdtcom ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtcom_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtcom_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtcom PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.com_concepto, a.com_concepto)  			as com_concepto,
COALESCE(b.com_comision, a.com_comision)  			as com_comision,
COALESCE(b.com_fecha_desde, a.com_fecha_desde)  			as com_fecha_desde,
COALESCE(b.com_fecha_hasta, a.com_fecha_hasta)  			as com_fecha_hasta,
COALESCE(b.com_estado, a.com_estado)  			as com_estado,
COALESCE(b.com_fecha_estado, a.com_fecha_estado)  			as com_fecha_estado,
COALESCE(b.com_period_com, a.com_period_com)  			as com_period_com,
COALESCE(b.com_divisa, a.com_divisa)  			as com_divisa,
COALESCE(b.com_period_cobr, a.com_period_cobr)  			as com_period_cobr,
COALESCE(b.com_cpo_libre, a.com_cpo_libre)  			as com_cpo_libre,
COALESCE(b.com_comi_im, a.com_comi_im)  			as com_comi_im,
COALESCE(b.com_comi_po, a.com_comi_po)  			as com_comi_po,
COALESCE(b.com_comi_min, a.com_comi_min)  			as com_comi_min,
COALESCE(b.com_comi_max, a.com_comi_max)  			as com_comi_max,
COALESCE(b.com_dia_nat_cobr, a.com_dia_nat_cobr)  			as com_dia_nat_cobr,
COALESCE(b.com_ind_tramos, a.com_ind_tramos)  			as com_ind_tramos,
COALESCE(b.com_ind_bof, a.com_ind_bof)  			as com_ind_bof,
COALESCE(b.com_dias_calc_prop, a.com_dias_calc_prop)  			as com_dias_calc_prop,
COALESCE(b.com_entidad_umo, a.com_entidad_umo)  			as com_entidad_umo,
COALESCE(b.com_centro_umo, a.com_centro_umo)  			as com_centro_umo,
COALESCE(b.com_userid_umo, a.com_userid_umo)  			as com_userid_umo,
COALESCE(b.com_netname_umo, a.com_netname_umo)  			as com_netname_umo,
COALESCE(b.com_timest_umo, a.com_timest_umo)  			as com_timest_umo
from bgdtcom_stock_tmp a
full outer join bgdtcom_update_tmp b on
(a.com_concepto = b.com_concepto AND
a.com_comision = b.com_comision AND
a.com_fecha_desde = b.com_fecha_desde );