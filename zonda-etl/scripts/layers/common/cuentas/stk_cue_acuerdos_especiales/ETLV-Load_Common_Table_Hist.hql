set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_common.stk_cue_acuerdos_especiales
partition(partition_date)
SELECT
	acu_centro_alta as cod_suc_sucursal
	,acu_cuenta as cod_cue_cuenta
	,acu_divisa as cod_cue_divisa
	,acu_secuencia as cod_cue_secuencia
	,acu_tipo_acuerdo as cod_cue_tipo_acuerdo
	,acu_estado as cod_cue_estado
	,acu_limite as fc_cue_limite
	,acu_limite_renov as fc_cue_limite_renovacion
	,acu_period_liq as cod_cue_period_liquidacion
	,acu_tipo_liq as cod_cue_tipo_liquidacion
	,acu_clase_liq as cod_cue_clase_liquidacion
	,acu_dia_liq as cod_cue_dia_liquidacion
	,acu_prioridad as cod_cue_prioridad
	,case when length(trim(acu_tarifa))=0 then null else trim(acu_tarifa) end as cod_cue_tarifa
	,case when trim(acu_clase_taf)='N' then 0 else 1 end as flag_cue_clase_taf
	,acu_tipo_interes as fc_cue_tipo_interes
	,case when length(trim(acu_coefici))=0 then null else trim(acu_coefici) end as fc_cue_coeficiente
	,acu_puntos as fc_cue_puntos
	,acu_tipoint_inc_sbrg as cod_cue_tipoint_inc_sbrg
	,acu_tasa_max_conv as fc_cue_tasa_max_conv
	,acu_importe_cambio as fc_cue_importe_cambio
	,acu_limite_ori as fc_cue_limite_original
	,acu_inte_devdeud as fc_cue_intereses_devengados_deuda
	,acu_refer_liq as cod_cue_refer_liq
	,acu_ind_renauto as flag_cue_renauto
	,case when trim(acu_ind_intv_sal_dia)='N' then 0 else 1 end  as flag_cue_ind_intv_sal_dia
	,case when trim(acu_ind_pet_liq)='N' then 0 else 1 end as flag_cue_ind_pet_liq
	,case when length(trim(acu_tipo_vto))=0 then null else trim(acu_tipo_vto) end  as cod_cue_tipo_vencimiento
	,acu_num_vto as cod_cue_num_vencimiento
	,acu_dias_uso as fc_cue_dias_uso
	,acu_dias_desp_pro as fc_cue_dias_desp_pro
	,acu_fecha_ultren as dt_cue_ultren
	,acu_fecha_inicio as dt_cue_inicio
	,acu_fecha_vcto as dt_cue_vcto
	,acu_fecha_ultliq as dt_cue_ultima_liquidacion
	,partition_date
FROM
	bi_corp_staging.malbgc_bgdtacu
WHERE
	partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malbgc_bgdtacu', dag_id='LOAD_CMN_Cuentas_Acuerdos') }}'
	and acu_fecha_inicio <='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Acuerdos') }}'
	and acu_fecha_vcto >='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Acuerdos') }}'
;