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
where SUBSTRING(partition_date,1,7)=SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}',1,7)
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where SUBSTRING(p.partition_date,1,7)= SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}',1,7)
AND p.pecalpar = 'TI';

insert overwrite table bi_corp_common.bt_cue_movimientos_cuenta
partition(partition_date)
SELECT
	trim(moh.moh_ENTIDAD) AS cod_cue_entidad,
	trim(moh.moh_CENTRO_ALTA) AS cod_suc_sucursal_alta,
	trim(moh.moh_CUENTA) AS cod_cue_cuenta,
	trim(moh.moh_DIVISA) AS cod_cue_divisa,
	trim(moh.moh_PRODUCTO) AS cod_cue_producto,
	trim(moh.moh_SUBPRODU) AS cod_cue_subproducto,
	CASE WHEN LENGTH(trim(moh.moh_COD_OPER_PPAL))=0 THEN NULL ELSE trim(moh.moh_COD_OPER_PPAL) END  AS cod_cue_operacion,
	moh.moh_NUMER_MOV AS cod_cue_comprobante,
	CASE WHEN LENGTH(trim(moh.moh_CANAL))=0 THEN NULL ELSE trim(moh.moh_CANAL) END  AS cod_cue_canal,
	trim(moh_fecha_conta) AS dt_cue_proceso,
	trim(moh_fecha_valor) AS dt_cue_movimiento,
	CASE WHEN mco.mco_num_convenio is not null and trim(moh.moh_codigo) in ('2811','2761') THEN 0 ELSE moh.moh_IMPORTE END AS fc_cue_importe, --saco los haberes de los empleados
	trim(moh.moh_IND_AOM) AS cod_cue_automatico,
	trim(mae.producto_contab) AS cod_cue_producto_contable,
	trim(mae.subprodu_contab) AS cod_cue_subproducto_contable,
	trim(aux.ind_paquete) AS flag_cue_paquete,
	trim(pedt008.penumper) as cod_per_nup,
	case when LENGTH(trim(moh.moh_TRANSACCION))=0 then null else trim(moh.moh_TRANSACCION) end AS cod_cue_transaccion,
	trim(moh.moh_IND_CAR_ABO) AS cod_cue_cargo_abono,
	CASE WHEN (tipo_movimiento.flag_cue_cliente=1 and moh.moh_codigo not in ('0005')) or moh.moh_codigo in ('0999','1037','1033')  THEN 1 ELSE 0 end AS flag_cue_genuino,
	trim(moh.moh_CENTRO_UMO) AS cod_suc_sucursal_ultimo_movimiento,
	trim(moh.moh_CENTRO_ORIGEN) AS cod_suc_sucursal_origen,
	trim(moh.moh_FECHA_OPER) AS dt_cue_operacion,
	trim(moh.moh_HORA_OPER) AS ts_cue_operacion,
	CASE WHEN LENGTH(trim(moh.moh_CONCEPTO))=0 THEN NULL ELSE trim(moh.moh_CONCEPTO) END  AS desc_cue_concepto,
	moh.moh_cheque cod_cue_cheque,
	moh.moh_num_serie cod_cue_numero_serie,
	case when LENGTH(trim(moh.moh_ref_interna))=0 then null else trim(moh.moh_ref_interna) end as cod_cue_ref_interna,
	case when trim(moh.moh_ind_desdobla)='S' then 1 else 0 end as flag_cue_desdoblamiento,
	moh.moh_ind_observa as cod_cue_observa,
	case when LENGTH(trim(moh.moh_ind_giro_dep))=0 then null else trim(moh.moh_ind_giro_dep) end as cod_cue_giro_dep,
	case when LENGTH(trim(moh.moh_codigo_ur))=0 then null else trim(moh.moh_codigo_ur) end as cod_cue_ur,
	moh.moh_tipo_cambio as cod_cue_tipo_cambio,
	case when trim(moh.moh_ind_contab)='S' then 1 else 0 end as flag_cue_contable,
	case when trim(moh.moh_ind_impto_mir)='S' then 1 else 0 end as flag_cue_impto_mir,
	case when trim(moh.moh_ind_ppal)='S' then 1 else 0 end as flag_cue_principal,
	moh.moh_cod_destino_mov as cod_cue_destino,
	case when trim(moh.moh_cod_tributa)='S' then 1 else 0 end as flag_cue_tributa,
	case when trim(moh.moh_ind_acred_sueldo)='S' then 1 else 0 end as flag_cue_acred_sueldo,
	moh.moh_numer_ret as cod_cue_numero_ret,
	case when trim(moh.moh_ind_anulable)='S' then 1 else 0 end as flag_cue_anulable,
	moh.moh_codigo as  cod_cue_codigo,
	tipo_movimiento.ds_cue_alfa_grande,
	moh.moh_base_impuesto as fc_cue_base_impuesto,
	case when LENGTH(trim(moh.moh_cod_impuesto))=0 then null else trim(moh_cod_impuesto) end as cod_cue_impuesto,
	moh.moh_impor_impuesto as fc_cue_impor_impuesto,
	case when LENGTH(trim(moh.moh_cpto_impuesto))=0 then null else trim(moh_cpto_impuesto) end as cod_cue_cpto_impuesto,
	moh.moh_fecha_conta as partition_date
FROM
	bi_corp_staging.malbgc_bgdtmoh moh
LEFT JOIN bi_corp_staging.bgdtmae mae
	ON  mae.entidad = moh.moh_entidad
	AND mae.centro_alta = moh.moh_centro_alta
	AND mae.cuenta = moh.moh_cuenta
	AND mae.divisa = moh.moh_divisa
	AND mae.producto = moh.moh_producto
	AND mae.subprodu = moh.moh_subprodu
	AND mae.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}' <= '2019-06-19' then '2019-06-19' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtmae', dag_id='LOAD_CMN_Mov_Cuentas') }}' end 
LEFT JOIN bi_corp_staging.bgdtaux aux
	ON	moh.moh_entidad = aux.entidad
	and moh.moh_centro_alta = aux.centro_alta
	and moh.moh_cuenta = aux.cuenta
	and moh.moh_divisa = aux.divisa
	and aux.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}' <= '2019-06-19' then '2019-06-19' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtaux', dag_id='LOAD_CMN_Mov_Cuentas') }}' end
LEFT JOIN pedt008_temp pedt008
ON moh.moh_CENTRO_ALTA = pedt008.pecodofi
AND moh.moh_CUENTA = pedt008.cuenta
LEFT JOIN bi_corp_common.dim_cue_tipo_movimiento tipo_movimiento
ON moh.moh_codigo= tipo_movimiento.cod_cue_codigo
LEFT JOIN bi_corp_staging.malbgc_bgdtmco mco
  ON cast(mae.num_convenio as int) = mco.mco_num_convenio
  AND mco.partition_date = case when '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}' <= '2020-11-26' then '2020-11-26' else '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_bgdtmae', dag_id='LOAD_CMN_Mov_Cuentas') }}' end 
  AND mco.mco_suscriptor in ('00000060000','00000208782')
  AND mco.mco_indesta ='A'
WHERE
	moh.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_Mov_Cuentas') }}'
	AND moh.moh_fecha_conta='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Mov_Cuentas') }}'
    AND moh.moh_ind_anul!='S'
;
