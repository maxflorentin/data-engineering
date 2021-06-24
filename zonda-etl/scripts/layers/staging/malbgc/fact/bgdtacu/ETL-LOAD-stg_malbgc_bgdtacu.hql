set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS bgdtacu_stock_tmp;
create temporary table bgdtacu_stock_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtacu where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates') }}'
union all
SELECT * FROM bi_corp_staging.malbgc_bgdtacu_stock where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_date_from', dag_id='LOAD_STG_Malbgc_Updates') }}';

DROP TABLE IF EXISTS bgdtacu_update_tmp;
create TEMPORARY table bgdtacu_update_tmp as
SELECT * FROM bi_corp_staging.malbgc_bgdtacu_updates where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}';

INSERT OVERWRITE TABLE bi_corp_staging.malbgc_bgdtacu PARTITION(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='date_from', dag_id='LOAD_STG_Malbgc_Updates') }}')
SELECT
COALESCE(b.acu_entidad,a.acu_entidad)  			as acu_entidad,
COALESCE(b.acu_centro_alta,a.acu_centro_alta) 	as acu_centro_alta,
COALESCE(b.acu_cuenta,a.acu_cuenta) 			as acu_cuenta,
COALESCE(b.acu_divisa,a.acu_divisa) 			as acu_divisa,
COALESCE(b.acu_secuencia,a.acu_secuencia) 		as acu_secuencia,
COALESCE(b.acu_tipo_acuerdo,a.acu_tipo_acuerdo) as acu_tipo_acuerdo,
COALESCE(b.acu_estado,a.acu_estado) 			as acu_estado,
COALESCE(b.acu_limite,a.acu_limite) 			as acu_limite,
COALESCE(b.acu_limite_renov,a.acu_limite_renov) as acu_limite_renov,
COALESCE(b.acu_period_liq,a.acu_period_liq) 	as acu_period_liq,
COALESCE(b.acu_tipo_liq,a.acu_tipo_liq) 		as acu_tipo_liq,
COALESCE(b.acu_clase_liq,a.acu_clase_liq) 		as acu_clase_liq,
COALESCE(b.acu_dia_liq,a.acu_dia_liq) 			as acu_dia_liq,
COALESCE(b.acu_prioridad,a.acu_prioridad) 		as acu_prioridad,
COALESCE(b.acu_tarifa,a.acu_tarifa) 			as acu_tarifa,
COALESCE(b.acu_clase_taf,a.acu_clase_taf) 		as acu_clase_taf,
COALESCE(b.acu_tipo_interes,a.acu_tipo_interes) as acu_tipo_interes,
COALESCE(b.acu_coefici,a.acu_coefici) 			as acu_coefici,
COALESCE(b.acu_puntos,a.acu_puntos) 			as acu_puntos,
COALESCE(b.acu_tipoint_inc_sbrg,a.acu_tipoint_inc_sbrg) as acu_tipoint_inc_sbrg,
COALESCE(b.acu_tasa_max_conv,a.acu_tasa_max_conv) 		as acu_tasa_max_conv,
COALESCE(b.acu_importe_cambio,a.acu_importe_cambio) 	as acu_importe_cambio,
COALESCE(b.acu_limite_ori,a.acu_limite_ori) 			as acu_limite_ori,
COALESCE(b.acu_inte_devdeud,a.acu_inte_devdeud) 		as acu_inte_devdeud,
COALESCE(b.acu_refer_liq,a.acu_refer_liq) 				as acu_refer_liq,
COALESCE(b.acu_ind_renauto,a.acu_refer_liq)				as acu_refer_liq,
COALESCE(b.acu_ind_intv_sal_dia,a.acu_ind_intv_sal_dia) as acu_ind_intv_sal_dia,
COALESCE(b.acu_ind_pet_liq,a.acu_ind_intv_sal_dia) 		as acu_ind_intv_sal_dia,
COALESCE(b.acu_tipo_vto,a.acu_tipo_vto) 				as acu_tipo_vto,
COALESCE(b.acu_num_vto,a.acu_num_vto)					as acu_num_vto,
COALESCE(b.acu_dias_uso,a.acu_dias_uso)					as acu_dias_uso,
COALESCE(b.acu_dias_desp_pro,a.acu_dias_desp_pro)		as acu_dias_desp_pro,
COALESCE(b.acu_fecha_ultren,a.acu_fecha_ultren)			as acu_fecha_ultren,
COALESCE(b.acu_fecha_inicio,a.acu_fecha_inicio)			as acu_fecha_inicio,
COALESCE(b.acu_fecha_vcto,a.acu_fecha_vcto)				as acu_fecha_vcto,
COALESCE(b.acu_fecha_ultliq,a.acu_fecha_ultliq)			as acu_fecha_ultliq,
COALESCE(b.acu_fecha_upro_liq,a.acu_fecha_upro_liq)		as acu_fecha_upro_liq,
COALESCE(b.acu_fecha_proliq,a.acu_fecha_proliq)			as acu_fecha_proliq,
COALESCE(b.acu_fecha_proc_liq,a.acu_fecha_proc_liq)		as acu_fecha_proc_liq,
COALESCE(b.acu_entidad_umo,a.acu_entidad_umo)			as acu_entidad_umo,
COALESCE(b.acu_centro_umo,a.acu_centro_umo)				as acu_centro_umo,
COALESCE(b.acu_userid_umo,a.acu_userid_umo)				as acu_userid_umo,
COALESCE(b.acu_netname_umo,a.acu_netname_umo)			as acu_netname_umo,
COALESCE(b.acu_timest_umo,a.acu_timest_umo)				as acu_timest_umo
from bgdtacu_stock_tmp a
full outer join bgdtacu_update_tmp b on (a.acu_entidad = b.acu_entidad AND a.acu_centro_alta = b.acu_centro_alta AND a.acu_cuenta = b.acu_cuenta AND a.acu_divisa = b.acu_divisa AND a.acu_secuencia = b.acu_secuencia);