set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

WITH max_part_bgdtimp AS
  (SELECT max(partition_date) AS previos_date_from
   FROM bi_corp_staging.malbgc_bgdtimp
   WHERE partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}', 7)
     AND partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' ),
     bgdtimp_stock_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtimp c
   INNER JOIN max_part_bgdtimp m ON c.partition_date = m.previos_date_from
   WHERE c.partition_date = m.previos_date_from
   UNION ALL SELECT *
   FROM bi_corp_staging.malbgc_bgdtimp_stock s
   INNER JOIN max_part_bgdtimp ms ON s.partition_date = ms.previos_date_from
   WHERE s.partition_date = ms.previos_date_from),
     bgdtimp_update_tmp AS
  (SELECT *
   FROM bi_corp_staging.malbgc_bgdtimp_updates
   WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}' )
INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtimp PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.imp_entidad,a.imp_entidad)  			as imp_entidad,
COALESCE(b.imp_centro_alta,a.imp_centro_alta)  			as imp_centro_alta,
COALESCE(b.imp_cuenta,a.imp_cuenta)  			as imp_cuenta,
COALESCE(b.imp_divisa,a.imp_divisa)  			as imp_divisa,
COALESCE(b.imp_secuimp,a.imp_secuimp)  			as imp_secuimp,
COALESCE(b.imp_secuimp_adici,a.imp_secuimp_adici)  			as imp_secuimp_adici,
COALESCE(b.imp_concepto,a.imp_concepto)  			as imp_concepto,
COALESCE(b.imp_fecha_imp,a.imp_fecha_imp)  			as imp_fecha_imp,
COALESCE(b.imp_prioridad,a.imp_prioridad)  			as imp_prioridad,
COALESCE(b.imp_importe_impago,a.imp_importe_impago)  			as imp_importe_impago,
COALESCE(b.imp_codoper_impago,a.imp_codoper_impago)  			as imp_codoper_impago,
COALESCE(b.imp_base_impuesto,a.imp_base_impuesto)  			as imp_base_impuesto,
COALESCE(b.imp_impuesto,a.imp_impuesto)  			as imp_impuesto,
COALESCE(b.imp_codope_impuest,a.imp_codope_impuest)  			as imp_codope_impuest,
COALESCE(b.imp_cpto_impuesto,a.imp_cpto_impuesto)  			as imp_cpto_impuesto,
COALESCE(b.imp_ind_dec_div,a.imp_ind_dec_div)  			as imp_ind_dec_div,
COALESCE(b.imp_fecha_cobro,a.imp_fecha_cobro)  			as imp_fecha_cobro,
COALESCE(b.imp_import_totcob,a.imp_import_totcob)  			as imp_import_totcob,
COALESCE(b.imp_impago_cobro,a.imp_impago_cobro)  			as imp_impago_cobro,
COALESCE(b.imp_impu_cobro,a.imp_impu_cobro)  			as imp_impu_cobro,
COALESCE(b.imp_estado,a.imp_estado)  			as imp_estado,
COALESCE(b.imp_cod_oper_ppal,a.imp_cod_oper_ppal)  			as imp_cod_oper_ppal,
COALESCE(b.imp_entidad_umo,a.imp_entidad_umo)  			as imp_entidad_umo,
COALESCE(b.imp_centro_umo,a.imp_centro_umo)  			as imp_centro_umo,
COALESCE(b.imp_userid_umo,a.imp_userid_umo)  			as imp_userid_umo,
COALESCE(b.imp_netname_umo,a.imp_netname_umo)  			as imp_netname_umo,
COALESCE(b.imp_timest_umo,a.imp_timest_umo)  			as imp_timest_umo
from bgdtimp_stock_tmp a
full outer join bgdtimp_update_tmp b on
(a.imp_entidad = b.imp_entidad AND
a.imp_centro_alta = b.imp_centro_alta AND
a.imp_cuenta = b.imp_cuenta AND
a.imp_divisa = b.imp_divisa AND
a.imp_secuimp = b.imp_secuimp AND
a.imp_secuimp_adici = b.imp_secuimp_adici);