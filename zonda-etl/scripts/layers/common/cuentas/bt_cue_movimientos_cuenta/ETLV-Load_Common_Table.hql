set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente

DROP TABLE IF EXISTS pedt008_temp;
CREATE TEMPORARY TABLE pedt008_temp AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Cuentas') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where p.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Cuentas') }}'
AND p.pecalpar = 'TI';

insert overwrite table bi_corp_common.bt_cue_movimientos_cuenta
partition(partition_date)
SELECT
	trim(mo1.mo1_ENTIDAD) AS cod_cue_entidad,
	trim(mo1.mo1_CENTRO_ALTA) AS cod_suc_sucursal_alta,
	trim(mo1.mo1_CUENTA) AS cod_cue_cuenta,
	trim(mo1.mo1_DIVISA) AS cod_cue_divisa,
	trim(mo1.mo1_PRODUCTO) AS cod_cue_producto,
	trim(mo1.mo1_SUBPRODU) AS cod_cue_subproducto,
	CASE WHEN LENGTH(trim(mo1.mo1_COD_OPER_PPAL))=0 THEN NULL ELSE trim(mo1.mo1_COD_OPER_PPAL) END  AS cod_cue_operacion,
	mo1.mo1_NUMER_MOV AS cod_cue_comprobante,
	CASE WHEN LENGTH(trim(mo1.mo1_CANAL))=0 THEN NULL ELSE trim(mo1.mo1_CANAL) END  AS cod_cue_canal,
	trim(mo1_fecha_conta) AS dt_cue_proceso,
	trim(mo1_fecha_valor) AS dt_cue_movimiento,
	CASE WHEN trim(mo1.mo1_codigo)='2811' THEN 0 ELSE mo1.mo1_IMPORTE END AS fc_cue_importe, --saco los haberes de los empleados
	trim(mo1.mo1_IND_AOM) AS cod_cue_automatico,
	trim(mae.producto_contab) AS cod_cue_producto_contable,
	trim(mae.subprodu_contab) AS cod_cue_subproducto_contable,
	trim(aux.ind_paquete) AS flag_cue_paquete,
	trim(pedt008.penumper) as cod_per_nup,
	case when LENGTH(trim(mo1.mo1_TRANSACCION))=0 then null else trim(mo1.mo1_TRANSACCION) end AS cod_cue_transaccion,
	trim(mo1.mo1_IND_CAR_ABO) AS cod_cue_cargo_abono,
	CASE WHEN (tipo_movimiento.flag_cue_cliente=1 and mo1.mo1_codigo not in ('0005')) or mo1.mo1_codigo in ('0999','1037','1033')  THEN 1 ELSE 0 end AS flag_cue_genuino,
	trim(mo1.mo1_CENTRO_UMO) AS cod_suc_sucursal_ultimo_movimiento,
	trim(mo1.mo1_CENTRO_ORIGEN) AS cod_suc_sucursal_origen,
	trim(mo1.mo1_FECHA_OPER) AS dt_cue_operacion,
	trim(mo1.mo1_HORA_OPER) AS ts_cue_operacion,
	CASE WHEN LENGTH(trim(mo1.mo1_CONCEPTO))=0 THEN NULL ELSE trim(mo1.mo1_CONCEPTO) END  AS desc_cue_concepto,
	mo1.mo1_cheque cod_cue_cheque,
	mo1.mo1_num_serie cod_cue_numero_serie,
	case when LENGTH(trim(mo1.mo1_ref_interna))=0 then null else trim(mo1.mo1_ref_interna) end as cod_cue_ref_interna,
	case when trim(mo1.mo1_ind_desdobla)='S' then 1 else 0 end as flag_cue_desdoblamiento,
	mo1.mo1_ind_observa as cod_cue_observa,
	case when LENGTH(trim(mo1.mo1_ind_giro_dep))=0 then null else trim(mo1.mo1_ind_giro_dep) end as cod_cue_giro_dep,
	case when LENGTH(trim(mo1.mo1_codigo_ur))=0 then null else trim(mo1.mo1_codigo_ur) end as cod_cue_ur,
	mo1.mo1_tipo_cambio as cod_cue_tipo_cambio,
	case when trim(mo1.mo1_ind_contab)='S' then 1 else 0 end as flag_cue_contable,
	case when trim(mo1.mo1_ind_impto_mir)='S' then 1 else 0 end as flag_cue_impto_mir,
	case when trim(mo1.mo1_ind_ppal)='S' then 1 else 0 end as flag_cue_principal,
	mo1.mo1_cod_destino_mov as cod_cue_destino,
	case when trim(mo1.mo1_cod_tributa)='S' then 1 else 0 end as flag_cue_tributa,
	case when trim(mo1.mo1_ind_acred_sueldo)='S' then 1 else 0 end as flag_cue_acred_sueldo,
	mo1.mo1_numer_ret as cod_cue_numero_ret,
	case when trim(mo1.mo1_ind_anulable)='S' then 1 else 0 end as flag_cue_anulable,
	mo1.mo1_codigo as  cod_cue_codigo,
	tipo_movimiento.ds_cue_alfa_grande,
	mo1.mo1_base_impuesto as fc_cue_base_impuesto,
	case when LENGTH(trim(mo1.mo1_cod_impuesto))=0 then null else trim(mo1_cod_impuesto) end as cod_cue_impuesto,
	mo1.mo1_impor_impuesto as fc_cue_impor_impuesto,
	case when LENGTH(trim(mo1.mo1_cpto_impuesto))=0 then null else trim(mo1_cpto_impuesto) end as cod_cue_cpto_impuesto,
	mo1.partition_date
FROM
	bi_corp_staging.malbgc_bgdtmo1 mo1
LEFT JOIN bi_corp_staging.bgdtmae mae
	ON  mae.entidad = mo1.mo1_entidad
	AND mae.centro_alta = mo1.mo1_centro_alta
	AND mae.cuenta = mo1.mo1_cuenta
	AND mae.divisa = mo1.mo1_divisa
	AND mae.producto = mo1.mo1_producto
	AND mae.subprodu = mo1.mo1_subprodu
	AND mae.partition_date = mo1.partition_date
LEFT JOIN bi_corp_staging.bgdtaux aux
	ON	mo1.mo1_entidad = aux.entidad
	and mo1.mo1_centro_alta = aux.centro_alta
	and mo1.mo1_cuenta = aux.cuenta
	and mo1.mo1_divisa = aux.divisa
	and aux.partition_date = mo1.partition_date
LEFT JOIN pedt008_temp pedt008
ON mo1.mo1_CENTRO_ALTA = pedt008.pecodofi
AND mo1.mo1_CUENTA = pedt008.cuenta
LEFT JOIN bi_corp_common.dim_cue_tipo_movimiento tipo_movimiento
ON mo1.mo1_codigo= tipo_movimiento.cod_cue_codigo

WHERE
	mo1.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'
	AND (mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  mae.partition_date IS NULL)
	AND (aux.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  aux.partition_date IS NULL)
    AND mo1.mo1_ind_anul!='S'

UNION ALL

SELECT
	trim(mo2.mo2_ENTIDAD) AS cod_cue_entidad,
	trim(mo2.mo2_CENTRO_ALTA) AS cod_suc_sucursal_alta,
	trim(mo2.mo2_CUENTA) AS cod_cue_cuenta,
	trim(mo2.mo2_DIVISA) AS cod_cue_divisa,
	trim(mo2.mo2_PRODUCTO) AS cod_cue_producto,
	trim(mo2.mo2_SUBPRODU) AS cod_cue_subproducto,
	CASE WHEN LENGTH(trim(mo2.mo2_COD_OPER_PPAL))=0 THEN NULL ELSE trim(mo2.mo2_COD_OPER_PPAL) END  AS cod_cue_operacion,
	mo2.mo2_NUMER_MOV AS cod_cue_comprobante,
	CASE WHEN LENGTH(trim(mo2.mo2_CANAL))=0 THEN NULL ELSE trim(mo2.mo2_CANAL) END  AS cod_cue_canal,
	trim(mo2_fecha_conta) AS dt_cue_proceso,
	trim(mo2_fecha_valor) AS dt_cue_movimiento,
	CASE WHEN trim(mo2.mo2_codigo)='2811' THEN 0 ELSE mo2.mo2_IMPORTE END AS fc_cue_importe, --saco los haberes de los empleados
	trim(mo2.mo2_IND_AOM) AS cod_cue_automatico,
	trim(mae.producto_contab) AS cod_cue_producto_contable,
	trim(mae.subprodu_contab) AS cod_cue_subproducto_contable,
	trim(aux.ind_paquete) AS flag_cue_paquete,
	trim(pedt008.penumper) as cod_per_nup,
	case when LENGTH(trim(mo2.mo2_TRANSACCION))=0 then null else trim(mo2.mo2_TRANSACCION) end AS cod_cue_transaccion,
	trim(mo2.mo2_IND_CAR_ABO) AS cod_cue_cargo_abono,
	CASE WHEN (tipo_movimiento.flag_cue_cliente=1 and mo2.mo2_codigo not in ('0005')) or mo2.mo2_codigo in ('0999','1037','1033')  THEN 1 ELSE 0 end AS flag_cue_genuino,
	trim(mo2.mo2_CENTRO_UMO) AS cod_suc_sucursal_ultimo_movimiento,
	trim(mo2.mo2_CENTRO_ORIGEN) AS cod_suc_sucursal_origen,
	trim(mo2.mo2_FECHA_OPER) AS dt_cue_operacion,
	trim(mo2.mo2_HORA_OPER) AS ts_cue_operacion,
	CASE WHEN LENGTH(trim(mo2.mo2_CONCEPTO))=0 THEN NULL ELSE trim(mo2.mo2_CONCEPTO) END  AS desc_cue_concepto,
	mo2.mo2_cheque cod_cue_cheque,
	mo2.mo2_num_serie cod_cue_numero_serie,
	case when LENGTH(trim(mo2.mo2_ref_interna))=0 then null else trim(mo2.mo2_ref_interna) end as cod_cue_ref_interna,
	case when trim(mo2.mo2_ind_desdobla)='S' then 1 else 0 end as flag_cue_desdoblamiento,
	mo2.mo2_ind_observa as cod_cue_observa,
	case when LENGTH(trim(mo2.mo2_ind_giro_dep))=0 then null else trim(mo2.mo2_ind_giro_dep) end as cod_cue_giro_dep,
	case when LENGTH(trim(mo2.mo2_codigo_ur))=0 then null else trim(mo2.mo2_codigo_ur) end as cod_cue_ur,
	mo2.mo2_tipo_cambio as cod_cue_tipo_cambio,
	case when trim(mo2.mo2_ind_contab)='S' then 1 else 0 end as flag_cue_contable,
	case when trim(mo2.mo2_ind_impto_mir)='S' then 1 else 0 end as flag_cue_impto_mir,
	case when trim(mo2.mo2_ind_ppal)='S' then 1 else 0 end as flag_cue_principal,
	mo2.mo2_cod_destino_mov as cod_cue_destino,
	case when trim(mo2.mo2_cod_tributa)='S' then 1 else 0 end as flag_cue_tributa,
	case when trim(mo2.mo2_ind_acred_sueldo)='S' then 1 else 0 end as flag_cue_acred_sueldo,
	mo2.mo2_numer_ret as cod_cue_numero_ret,
	case when trim(mo2.mo2_ind_anulable)='S' then 1 else 0 end as flag_cue_anulable,
	mo2.mo2_codigo as  cod_cue_codigo,
	tipo_movimiento.ds_cue_alfa_grande,
	mo2.mo2_base_impuesto as fc_cue_base_impuesto,
	case when LENGTH(trim(mo2.mo2_cod_impuesto))=0 then null else trim(mo2_cod_impuesto) end as cod_cue_impuesto,
	mo2.mo2_impor_impuesto as fc_cue_impor_impuesto,
	case when LENGTH(trim(mo2.mo2_cpto_impuesto))=0 then null else trim(mo2_cpto_impuesto) end as cod_cue_cpto_impuesto,
	mo2.partition_date
FROM bi_corp_staging.malbgc_bgdtmo2 mo2
LEFT JOIN bi_corp_staging.bgdtmae mae
  ON  mae.entidad = mo2.mo2_entidad
  AND mae.centro_alta = mo2.mo2_centro_alta
  AND mae.cuenta = mo2.mo2_cuenta
  AND mae.divisa = mo2.mo2_divisa
  AND mae.producto = mo2.mo2_producto
  AND mae.subprodu = mo2.mo2_subprodu
  AND mae.partition_date =mo2.partition_date
LEFT JOIN bi_corp_staging.bgdtaux aux
  ON mo2.mo2_entidad = aux.entidad
  AND mo2.mo2_centro_alta = aux.centro_alta
  AND mo2.mo2_cuenta = aux.cuenta
  AND mo2.mo2_divisa = aux.divisa
  AND aux.partition_date=mo2.partition_date
LEFT JOIN pedt008_temp pedt008
  ON mo2.mo2_CENTRO_ALTA = pedt008.pecodofi
  AND mo2.mo2_CUENTA = pedt008.cuenta
LEFT JOIN bi_corp_common.dim_cue_tipo_movimiento tipo_movimiento
ON mo2.mo2_codigo= tipo_movimiento.cod_cue_codigo

WHERE
	mo2.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'
	AND (mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  mae.partition_date IS NULL)
	AND (aux.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  aux.partition_date IS NULL)
    AND mo2.mo2_ind_anul!='S'

UNION ALL

SELECT
	trim(mob.mob_ENTIDAD) AS cod_cue_entidad,
	trim(mob.mob_CENTRO_ALTA) AS cod_suc_sucursal_alta,
	trim(mob.mob_CUENTA) AS cod_cue_cuenta,
	trim(mob.mob_DIVISA) AS cod_cue_divisa,
	trim(mob.mob_PRODUCTO) AS cod_cue_producto,
	trim(mob.mob_SUBPRODU) AS cod_cue_subproducto,
	CASE WHEN LENGTH(trim(mob.mob_COD_OPER_PPAL))=0 THEN NULL ELSE trim(mob.mob_COD_OPER_PPAL) END  AS cod_cue_operacion,
	mob.mob_NUMER_MOV AS cod_cue_comprobante,
	CASE WHEN LENGTH(trim(mob.mob_CANAL))=0 THEN NULL ELSE trim(mob.mob_CANAL) END  AS cod_cue_canal,
	trim(mob_fecha_conta) AS dt_cue_proceso,
	trim(mob_fecha_valor) AS dt_cue_movimiento,
	CASE WHEN trim(mob.mob_codigo)='2811' THEN 0 ELSE mob.mob_IMPORTE END AS fc_cue_importe, --saco los haberes de los empleados
	trim(mob.mob_IND_AOM) AS cod_cue_automatico,
	trim(mae.producto_contab) AS cod_cue_producto_contable,
	trim(mae.subprodu_contab) AS cod_cue_subproducto_contable,
	trim(aux.ind_paquete) AS flag_cue_paquete,
	trim(pedt008.penumper) as cod_per_nup,
	case when LENGTH(trim(mob.mob_TRANSACCION))=0 then null else trim(mob.mob_TRANSACCION) end AS cod_cue_transaccion,
	trim(mob.mob_IND_CAR_ABO) AS cod_cue_cargo_abono,
	CASE WHEN (tipo_movimiento.flag_cue_cliente=1 and mob.mob_codigo not in ('0005')) or mob.mob_codigo in ('0999','1037','1033')  THEN 1 ELSE 0 end AS flag_cue_genuino,
	trim(mob.mob_CENTRO_UMO) AS cod_suc_sucursal_ultimo_movimiento,
	trim(mob.mob_CENTRO_ORIGEN) AS cod_suc_sucursal_origen,
	trim(mob.mob_FECHA_OPER) AS dt_cue_operacion,
	trim(mob.mob_HORA_OPER) AS ts_cue_operacion,
	CASE WHEN LENGTH(trim(mob.mob_CONCEPTO))=0 THEN NULL ELSE trim(mob.mob_CONCEPTO) END  AS desc_cue_concepto,
	mob.mob_cheque cod_cue_cheque,
	mob.mob_num_serie cod_cue_numero_serie,
	case when LENGTH(trim(mob.mob_ref_interna))=0 then null else trim(mob.mob_ref_interna) end as cod_cue_ref_interna,
	case when trim(mob.mob_ind_desdobla)='S' then 1 else 0 end as flag_cue_desdoblamiento,
	mob.mob_ind_observa as cod_cue_observa,
	case when LENGTH(trim(mob.mob_ind_giro_dep))=0 then null else trim(mob.mob_ind_giro_dep) end as cod_cue_giro_dep,
	case when LENGTH(trim(mob.mob_codigo_ur))=0 then null else trim(mob.mob_codigo_ur) end as cod_cue_ur,
	mob.mob_tipo_cambio as cod_cue_tipo_cambio,
	case when trim(mob.mob_ind_contab)='S' then 1 else 0 end as flag_cue_contable,
	case when trim(mob.mob_ind_impto_mir)='S' then 1 else 0 end as flag_cue_impto_mir,
	case when trim(mob.mob_ind_ppal)='S' then 1 else 0 end as flag_cue_principal,
	mob.mob_cod_destino_mov as cod_cue_destino,
	case when trim(mob.mob_cod_tributa)='S' then 1 else 0 end as flag_cue_tributa,
	case when trim(mob.mob_ind_acred_sueldo)='S' then 1 else 0 end as flag_cue_acred_sueldo,
	mob.mob_numer_ret as cod_cue_numero_ret,
	case when trim(mob.mob_ind_anulable)='S' then 1 else 0 end as flag_cue_anulable,
	mob.mob_codigo as  cod_cue_codigo,
	tipo_movimiento.ds_cue_alfa_grande,
	mob.mob_base_impuesto as fc_cue_base_impuesto,
	case when LENGTH(trim(mob.mob_cod_impuesto))=0 then null else trim(mob_cod_impuesto) end as cod_cue_impuesto,
	mob.mob_impor_impuesto as fc_cue_impor_impuesto,
	case when LENGTH(trim(mob.mob_cpto_impuesto))=0 then null else trim(mob_cpto_impuesto) end as cod_cue_cpto_impuesto,
	mob.partition_date
FROM bi_corp_staging.malbgc_bgdtmob mob
LEFT JOIN bi_corp_staging.bgdtmae mae
  ON  mae.entidad = mob.mob_entidad
  AND mae.centro_alta = mob.mob_centro_alta
  AND mae.cuenta = mob.mob_cuenta
  AND mae.divisa = mob.mob_divisa
  AND mae.producto = mob.mob_producto
  AND mae.subprodu = mob.mob_subprodu
  AND mae.partition_date =mob.partition_date
LEFT JOIN bi_corp_staging.bgdtaux aux
  ON mob.mob_entidad = aux.entidad
  AND mob.mob_centro_alta = aux.centro_alta
  AND mob.mob_cuenta = aux.cuenta
  AND mob.mob_divisa = aux.divisa
  AND aux.partition_date=mob.partition_date
LEFT JOIN pedt008_temp pedt008
  ON mob.mob_CENTRO_ALTA = pedt008.pecodofi
  AND mob.mob_CUENTA = pedt008.cuenta
LEFT JOIN bi_corp_common.dim_cue_tipo_movimiento tipo_movimiento
ON mob.mob_codigo= tipo_movimiento.cod_cue_codigo

WHERE
	mob.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'
	AND (mae.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  mae.partition_date IS NULL)
	AND (aux.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas') }}'  OR  aux.partition_date IS NULL)
	AND mob.mob_ind_anul!='S'
;
