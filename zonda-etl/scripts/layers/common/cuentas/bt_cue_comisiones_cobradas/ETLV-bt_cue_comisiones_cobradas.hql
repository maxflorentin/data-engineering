SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS temp_mantenimiento;
----- Las comisiones de mantenimiento se cobran en pesos
CREATE TEMPORARY TABLE temp_mantenimiento
AS
--- Convierto a pesos el importe
SELECT
    lco.LCO_ENTIDAD as cod_cue_entidad,
	lco.LCO_CENTRO_ALTA as cod_suc_sucursal,
	lco.LCO_CUENTA as cod_cue_cuenta,
	lco.LCO_DIVISA as cod_cue_divisa,
	'ARS' as cod_cue_div_importe,
	lco.lco_entidad_cargo as cod_cue_entidad_cargo,
    lco.lco_centro_cargo as cod_suc_sucursal_cargo,
    lco.lco_cuenta_cargo as cod_cue_cuenta_cargo,
    lco.lco_divisa_cargo as cod_cue_div_cargo,
    lco.lco_producto as cod_cue_producto,
    lco.lco_subprodu as cod_cue_subproducto,
    lco.lco_producto_cargo as cod_cue_producto_cargo,
    lco.lco_subprodu_cargo as cod_cue_subprodu_cargo,
    IF(lco.lco_producto_contab='', cast(NULL as string),lco.lco_producto_contab)  as cod_cue_produ_contab,
    IF(lco.lco_subprodu_contab='', cast(NULL as string),lco.lco_subprodu_contab)  as cod_cue_subprodu_contab,
	lco.LCO_PLAN as cod_cue_plan,
	lco.lco_concepto as cod_cue_concepto,
	IF(lco.lco_comision='', cast(NULL as string),lco.lco_comision) as cod_cue_comision,
	lco.LCO_NUM_CONVENIO as cod_cue_num_convenio,
	IF(lco.lco_operacion='',cast(NULL as string),lco.lco_operacion) as ds_cue_operacion,
	IF(lco.lco_zona='',cast(NULL as string),lco.lco_zona) as ds_cue_zona,
	IF(lco.lco_ind_com_especial = 'S',1,0) as flag_cue_com_esp,
	lco.LCO_IMPORTE_CAP as fc_cue_importe_capitalizado,
	lco.LCO_IMPORTE * c.imp_cotizacion_ars as fc_cue_importe,
	lco.lco_base_calculo as fc_cue_base_calculo,
	lco.lco_comi_po as dec_cue_porc_comision,
	lco.lco_comi_min as fc_cue_min_comision,
	lco.lco_comi_max as fc_cue_max_comision,
	lco.lco_fecha_proliq as dt_cue_fecha_proliq,
	lco.lco_fecha_mov as dt_cue_fecha_mov,
	lco.lco_fecha as dt_cue_fecha,
	lco.partition_date as partition_date
	FROM bi_corp_staging.malbgc_bgeclco lco
	LEFT JOIN santander_business_risk_arda.cotizaciones_bcra c on lco.lco_fecha_mov = c.fec_info and lco.lco_divisa= c.cod_especie
	WHERE lco.LCO_CONCEPTO = 'MANT' AND lco.lco_divisa = 'USD' AND lco.lco_div_importe='USD'
	AND lco.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Comisiones_Cobradas-Daily') }}'

	UNION ALL

 	SELECT
 	lco.LCO_ENTIDAD cod_cue_entidad,
	lco.LCO_CENTRO_ALTA as cod_suc_sucursal,
	lco.LCO_CUENTA as cod_cue_cuenta,
	lco.LCO_DIVISA as cod_cue_divisa,
	lco.LCO_DIV_IMPORTE as cod_cue_div_importe,
	lco.lco_entidad_cargo as cod_cue_entidad_cargo,
    lco.lco_centro_cargo as cod_suc_sucursal_cargo,
    lco.lco_cuenta_cargo as cod_cue_cuenta_cargo,
    lco.lco_divisa_cargo as cod_cue_div_cargo,
    lco.lco_producto as cod_cue_producto,
    lco.lco_subprodu as cod_cue_subproducto,
    lco.lco_producto_cargo as cod_cue_producto_cargo,
    lco.lco_subprodu_cargo as cod_cue_subprodu_cargo,
    IF(lco.lco_producto_contab='',cast(NULL as string),lco.lco_producto_contab)  as cod_cue_produ_contab,
    IF(lco.lco_subprodu_contab='',cast(NULL as string),lco.lco_subprodu_contab)  as cod_cue_subprodu_contab,
	lco.LCO_PLAN as cod_cue_plan,
	lco.lco_concepto as cod_cue_concepto,
	IF(lco.lco_comision='', cast(NULL as string),lco.lco_comision) as cod_cue_comision,
	lco.LCO_NUM_CONVENIO as cod_cue_num_convenio,
	IF(lco.lco_operacion='',cast(NULL as string),lco.lco_operacion) as ds_cue_operacion,
	IF(lco.lco_zona='', cast(NULL as string),lco.lco_zona) as ds_cue_zona,
	IF(lco.lco_ind_com_especial = 'S',1,0) as flag_cue_com_esp,
	lco.LCO_IMPORTE_CAP as fc_cue_importe_capitalizado,
	lco.LCO_IMPORTE as fc_cue_importe,
	lco.lco_base_calculo as fc_cue_base_calculo,
	lco.lco_comi_po as dec_cue_porc_comision,
	lco.lco_comi_min as fc_cue_min_comision,
	lco.lco_comi_max as fc_cue_max_comision,
	lco.lco_fecha_proliq as dt_cue_fecha_proliq,
	lco.lco_fecha_mov as dt_cue_fecha_mov,
	lco.lco_fecha as dt_cue_fecha,
	lco.partition_date as partition_date
	FROM bi_corp_staging.malbgc_bgeclco lco
	WHERE lco.LCO_CONCEPTO = 'MANT' AND (lco.lco_divisa = 'ARS' OR (lco.lco_divisa = 'USD' AND lco.lco_div_importe='ARS'))
    AND lco.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Comisiones_Cobradas-Daily') }}';

-- Resto de los conceptos
    DROP TABLE IF EXISTS temp_resto;
    CREATE TEMPORARY TABLE temp_resto
    AS
    SELECT
    lco.LCO_ENTIDAD cod_cue_entidad,
    lco.LCO_CENTRO_ALTA as cod_suc_sucursal,
    lco.LCO_CUENTA as cod_cue_cuenta,
    lco.LCO_DIVISA as cod_cue_divisa,
    lco.LCO_DIV_IMPORTE as cod_cue_div_importe,
    lco.lco_entidad_cargo as cod_cue_entidad_cargo,
    lco.lco_centro_cargo as cod_suc_sucursal_cargo,
    lco.lco_cuenta_cargo as cod_cue_cuenta_cargo,
    lco.lco_divisa_cargo as cod_cue_div_cargo,
    lco.lco_producto as cod_cue_producto,
    lco.lco_subprodu as cod_cue_subproducto,
    lco.lco_producto_cargo as cod_cue_producto_cargo,
    lco.lco_subprodu_cargo as cod_cue_subprodu_cargo,
    IF(lco.lco_producto_contab='',cast(NULL as string),lco.lco_producto_contab)  as cod_cue_produ_contab,
    IF(lco.lco_subprodu_contab='',cast(NULL as string),lco.lco_subprodu_contab)  as cod_cue_subprodu_contab,
	lco.LCO_PLAN as cod_cue_plan,
	CASE WHEN lco.lco_concepto in ('EXTR', 'EXTD') THEN 'EXT' ELSE lco_concepto END AS cod_cue_concepto,
	IF(lco.lco_comision='', cast(NULL as string),lco.lco_comision) as cod_cue_comision,
	lco.LCO_NUM_CONVENIO as cod_cue_num_convenio,
	IF(lco.lco_operacion='',cast(NULL as string),lco.lco_operacion) as ds_cue_operacion,
	IF(lco.lco_zona='', cast(NULL as string),lco.lco_zona) as ds_cue_zona,
	IF(lco.lco_ind_com_especial = 'S',1,0) as flag_cue_com_esp,
	lco.LCO_IMPORTE_CAP as fc_cue_importe_capitalizado,
	lco.lco_importe as fc_cue_importe,
	lco.lco_base_calculo as fc_cue_base_calculo,
	lco.lco_comi_po as dec_cue_porc_comision,
	lco.lco_comi_min as fc_cue_min_comision,
	lco.lco_comi_max as fc_cue_max_comision,
    lco.lco_fecha_proliq as dt_cue_fecha_proliq,
    lco.lco_fecha_mov as dt_cue_fecha_mov,
    lco.lco_fecha as dt_cue_fecha,
    lco.partition_date as partition_date
    FROM bi_corp_staging.malbgc_bgeclco lco
    WHERE LCO_CONCEPTO <> 'MANT' and (LCO_DIVISA ='ARS' OR (LCO_DIVISA='USD' and lco_div_importe = 'USD'))
    AND lco.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Comisiones_Cobradas-Daily') }}'

    UNION ALL

    --- Convierto a dólar el importe del producto 06 de depe
    SELECT
    lco.LCO_ENTIDAD cod_cue_entidad,
    lco.LCO_CENTRO_ALTA as cod_suc_sucursal,
    lco.LCO_CUENTA as cod_cue_cuenta,
    lco.LCO_DIVISA as cod_cue_divisa,
    'USD' as cod_cue_div_importe,
    lco.lco_entidad_cargo as cod_cue_entidad_cargo,
    lco.lco_centro_cargo as cod_suc_sucursal_cargo,
    lco.lco_cuenta_cargo as cod_cue_cuenta_cargo,
    lco.lco_divisa_cargo as cod_cue_div_cargo,
    lco.lco_producto as cod_cue_producto,
    lco.lco_subprodu as cod_cue_subproducto,
    lco.lco_producto_cargo as cod_cue_producto_cargo,
    lco.lco_subprodu_cargo as cod_cue_subprodu_cargo,
    IF(lco.lco_producto_contab='', cast(NULL as string),lco.lco_producto_contab)  as cod_cue_produ_contab,
    IF(lco.lco_subprodu_contab='', cast(NULL as string),lco.lco_subprodu_contab)  as cod_cue_subprodu_contab,
	lco.LCO_PLAN as cod_cue_plan,
	CASE WHEN lco.lco_concepto in ('EXTR', 'EXTD') THEN 'EXT' ELSE lco_concepto END AS cod_cue_concepto,
	IF(lco.lco_comision='', cast(NULL as string),lco.lco_comision) as cod_cue_comision,
	lco.LCO_NUM_CONVENIO as cod_cue_num_convenio,
	IF(lco.lco_operacion='', cast(NULL as string),lco.lco_operacion) as ds_cue_operacion,
	IF(lco.lco_zona='', cast(NULL as string),lco.lco_zona) as ds_cue_zona,
	IF(lco.lco_ind_com_especial = 'S',1,0) as flag_cue_com_esp,
	lco.LCO_IMPORTE_CAP as fc_cue_importe_capitalizado,
	lco.lco_importe / c.imp_cotizacion_ars as fc_cue_importe,
	lco.lco_base_calculo as fc_cue_base_calculo,
	lco.lco_comi_po as dec_cue_porc_comision,
	lco.lco_comi_min as fc_cue_min_comision,
	lco.lco_comi_max as fc_cue_max_comision,
    lco.lco_fecha_proliq as dt_cue_fecha_proliq,
    lco.lco_fecha_mov as dt_cue_fecha_mov,
    lco.lco_fecha as dt_cue_fecha,
    lco.partition_date as partition_date
    FROM bi_corp_staging.malbgc_bgeclco lco
    LEFT JOIN santander_business_risk_arda.cotizaciones_bcra c on c.fec_info = lco.lco_fecha_mov and c.cod_especie='USD'
    WHERE LCO_CONCEPTO <> 'MANT' and lco_divisa ='USD' and lco_div_importe ='ARS'
    AND lco.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Comisiones_Cobradas-Daily') }}';

INSERT OVERWRITE TABLE bi_corp_common.bt_cue_comisiones_cobradas
PARTITION(partition_date)
SELECT * FROM temp_mantenimiento
UNION ALL
SELECT * FROM temp_resto