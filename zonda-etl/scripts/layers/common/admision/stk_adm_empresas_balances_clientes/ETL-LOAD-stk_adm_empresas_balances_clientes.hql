set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS balances_supervision;
CREATE TEMPORARY TABLE balances_supervision AS
    SELECT
        id_blc as id_balance,
        'SUPERVISION' as origen,
        cast(null as string) AS numero_propuesta
    FROM bi_corp_staging.sge_blc_act
    WHERE ((pefecalt < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1))
        OR (pefecalt IS NULL AND fec_blc < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1)))
        AND partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';

DROP TABLE IF EXISTS balances_estandarizadas;
CREATE TEMPORARY TABLE balances_estandarizadas AS
    SELECT
        max(id_balance) as id_balance,
        'ESTANDARIZADAS' as origen,
        max(nro_prop) as numero_propuesta
    from bi_corp_staging.sge_stnd_balances
    WHERE ((pefecalt < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1))
        OR (pefecalt IS NULL AND fec_balance < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1)) )
        AND partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    group by id_balance;

-- Inserto los registros de estandarizadas
INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_balances_clientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(BLC_STND.ID_BALANCE as bigint) AS id_adm_balance,
    BLC_STND.FEC_BALANCE AS dt_adm_fecha_balance,
    cast(BLC_STND.PENUMPER as bigint) AS cod_adm_numero_persona_sge,
    'N' AS flag_adm_consolidado,
    cast(BLC_STND.MESES_ABARCA as int) AS int_adm_duracion,
    'ESTANDARIZADAS' AS ds_adm_segmento,
    cast(BLC_STND.DISPONIBILIDADES as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_caja_bancos,
    NULL AS fc_adm_activo_cp_disponibles_valores_depositar,
    NULL AS fc_adm_activo_cp_disponibles_inversiones_corto_plazo,
    NULL AS fc_adm_activo_cp_disponibles_total,
    NULL AS fc_adm_activo_cp_creditos_ventas_dsventas_doccobrar,
    NULL AS fc_adm_activo_cp_creditos_ventas_deudores_gestion_cheques_rechazados,
    NULL AS fc_adm_activo_cp_creditos_ventas_sociedades_vinculadas,
    NULL AS fc_adm_activo_cp_creditos_ventas_prevision_ds_incobrables,
    cast(BLC_STND.IMP_ACT_CREDITO_COM as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_total,
    NULL AS fc_adm_activo_cp_otras_cuentas_creditos_fiscales,
    NULL AS fc_adm_activo_cp_otras_cuentas_gastos_pagados_adelantado,
    NULL AS fc_adm_activo_cp_otras_cuentas_sociedades_vinculadas,
    BLC_STND.IMP_ANTIC_ACCIONISTAS AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_anticipados,
    BLC_STND.IMP_ANTIC_ACCIONISTAS AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_aprobados_anticipados,
    NULL AS fc_adm_activo_cp_otras_cuentas_aportes_pendientes_integracion_cuentas_socios,
    NULL AS fc_adm_activo_cp_otras_cuentas_dividendos_cobrar,
    NULL AS fc_adm_activo_cp_otras_cuentas_diversos,
    NULL AS fc_adm_activo_cp_otras_cuentas_prevision_otros_creditos,
    NULL AS fc_adm_activo_cp_otras_cuentas_total,
    NULL AS fc_adm_activo_cp_bienes_cambio_productos_terminados_merc_reventa,
    NULL AS fc_adm_activo_cp_bienes_cambio_productos_proceso,
    NULL AS fc_adm_activo_cp_bienes_cambio_materias_primas,
    NULL AS fc_adm_activo_cp_bienes_cambio_otros_insumos,
    NULL AS fc_adm_activo_cp_bienes_cambio_anticipos_proveedores,
    NULL AS fc_adm_activo_cp_bienes_cambio_prevision_desvalorizaciones,
    cast(BLC_STND.IMP_ACT_BIENES_CAMBIO as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_total,
    NULL AS fc_adm_activo_cp_otros,
    cast(BLC_STND.IMP_ACT_CORRIENTE as decimal(20, 2)) AS fc_adm_activo_cp_total,
    NULL AS fc_adm_activo_lp_creditos_ventas_dsventas_doccobrar,
    NULL AS fc_adm_activo_lp_creditos_ventas_deudores_gestion_cheques_rechazados,
    NULL AS fc_adm_activo_lp_creditos_ventas_sociedades_vinculadas,
    NULL AS fc_adm_activo_lp_creditos_ventas_prevision_ds_incobrables,
    NULL AS fc_adm_activo_lp_creditos_venta_total,
    NULL AS fc_adm_activo_lp_otros_creditos_fiscales,
    NULL AS fc_adm_activo_lp_otros_creditos_sociales_vinculadas,
    NULL AS fc_adm_activo_lp_otros_creditos_gastos_pagados_adelantado,
    NULL AS fc_adm_activo_lp_otros_creditos_cuentas_particulares_accionistas,
    NULL AS fc_adm_activo_lp_otros_creditos_diversos,
    NULL AS fc_adm_activo_lp_otros_creditos_otros,
    NULL AS fc_adm_activo_lp_otros_creditos_total,
    NULL AS fc_adm_activo_lp_inversiones_permanentes_diversas,
    NULL AS fc_adm_activo_lp_inversiones_permanentes_soc,
    NULL AS fc_adm_activo_lp_inversiones_permanentes_total,
    NULL AS fc_adm_activo_lp_bienes_uso_terrenos,
    NULL AS fc_adm_activo_lp_bienes_uso_edificios_mejoras,
    NULL AS fc_adm_activo_lp_bienes_uso_maquinas_equipos_herramientas,
    NULL AS fc_adm_activo_lp_bienes_uso_m_utiles_instalaciones_rodados,
    NULL AS fc_adm_activo_lp_bienes_uso_otros,
    NULL AS fc_adm_activo_lp_bienes_uso_obras_curso,
    NULL AS fc_adm_activo_lp_bienes_uso_anticipo_proveedores,
    NULL AS fc_adm_activo_lp_bienes_uso_amortizacion_acumulada,
    cast(BLC_STND.IMP_ACT_BIENES_USO as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_total,
    NULL AS fc_adm_activo_lp_bienes_intangibles_marcas_patentes,
    NULL AS fc_adm_activo_lp_bienes_intangibles_reorganizacion_empresa,
    NULL AS fc_adm_activo_lp_bienes_intangibles_valor_llave,
    NULL AS fc_adm_activo_lp_bienes_intangibles_amort_acumulada,
    NULL AS fc_adm_activo_lp_bienes_intangibles_total,
    NULL AS fc_adm_activo_lp_bienes_cambio,
    NULL AS fc_adm_activo_lp_otros,
    NULL AS fc_adm_activo_lp_total,
    cast(NVL(BLC_STND.IMP_ACT_CORRIENTE, 0) as decimal(20, 2)) AS fc_adm_activo_total,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_costo_credito_importacion,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_deuda,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_obligaciones_negociables,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_porcion_deuda_lp,
    cast(BLC_STND.IMP_PAS_DEUDA_FIN_CP as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_total,
    cast(BLC_STND.IMP_PAS_DEUDA_COM_CP as decimal(20, 2)) AS fc_adm_pasivo_cp_importe_proveedores,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_comercial,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_financiera,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_otras,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_total,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_moratorias,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_diferimiento_impositivo,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_otras_ds,
    cast(BLC_STND.IMP_PAS_DEUDA_FIS_CP as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_fiscales_total,
    NULL AS fc_adm_pasivo_cp_otros_anticipos_clientes,
    NULL AS fc_adm_pasivo_cp_dividendos_honorarios_pagar,
    NULL AS fc_adm_pasivo_cp_previsiones,
    cast(BLC_STND.IMP_PAS_CORRIENTE as decimal(20, 2)) AS fc_adm_pasivo_cp_total,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_credito_importacion,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_deuda_financiera,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_obligaciones_negociables,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_proveedores,
    cast(BLC_STND.IMP_PAS_DEUDA_FIN_LP as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_total,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_comercial,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_financiera,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_otras,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_total,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_moratorias,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_diferimiento_impositivo,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_otros_ds,
    cast(BLC_STND.IMP_PAS_DEUDA_FIS_LP as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_fiscales_total,
    cast(BLC_STND.IMP_PAS_OTROS_LP as decimal(20, 2)) AS fc_adm_pasivo_lp_otros,
    NULL AS fc_adm_pasivo_lp_previsiones,
    NULL AS fc_adm_pasivo_lp_participacion_minoritaria,
    cast((COALESCE(BLC_STND.IMP_PAS_TOTAL, 0) - COALESCE(BLC_STND.IMP_PAS_CORRIENTE, 0)) as decimal(20, 2)) AS fc_adm_pasivo_lp_total,
    NULL AS fc_adm_pasivo_patrimonio_neto_capital,
    NULL AS fc_adm_pasivo_patrimonio_neto_reserva_revaluo_tecnico,
    NULL AS fc_adm_pasivo_patrimonio_neto_reservas_resultados,
    NULL AS fc_adm_pasivo_patrimonio_neto_resultados_no_asignados,
    NULL AS fc_adm_pasivo_patrimonio_neto_resultado_ejercicio,
    cast(BLC_STND.IMP_PAS_PATRIMONIO as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_total,
    cast(BLC_STND.IMP_RES_VENTAS as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_ventas_netas,
    cast(BLC_STND.IMP_RES_COSTOS_VENTAS as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_costo_ventas,
    NULL AS fc_adm_estado_resultado_bruto_total,
    cast(BLC_STND.IMP_RES_GTOS_ADMIN as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_administrativos,
    cast(BLC_STND.IMP_RES_GTOS_COMER as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_comerciales,
    NULL AS fc_adm_estado_resultado_operativo_otros,
    NULL AS fc_adm_estado_resultado_operativo_ingresos_promocion_industrial,
    cast(BLC_STND.IMP_RES_OPERATIVO as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_total,
    NULL AS fc_adm_estado_resultado_inversiones_permanentes,
    NULL AS fc_adm_estado_resultado_reintegros_exportaciones,
    NULL AS fc_adm_estado_resultado_otros_ingresos,
    NULL AS fc_adm_estado_resultado_otros_egresos,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_intereses,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_rei,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_diferencias_cambio,
    cast(NVL(BLC_STND.IMP_RES_GTOS_FINAN,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_tenencia,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_otros,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_total,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_intereses,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_rei,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_diferencias_cambio,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_tenencia,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_otros,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_total,
    cast(BLC_STND.IMP_RES_GTOS_FINAN as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_total,
    NULL AS fc_adm_estado_resultado_operaciones_ordinarias_total,
    if(BLC_STND.IMP_RES_EXTRAORDINARIO >= 0, BLC_STND.IMP_RES_EXTRAORDINARIO, 0) AS fc_adm_estado_resultado_antes_impuestos_ganancias_extraordinarias,
    if(BLC_STND.IMP_RES_EXTRAORDINARIO <= 0, BLC_STND.IMP_RES_EXTRAORDINARIO, 0) AS fc_adm_estado_resultado_antes_impuestos_perdidas_extraordinarias,
    NULL AS fc_adm_estado_resultado_antes_impuestos_participacion_minoritaria,
    NULL AS fc_adm_estado_resultado_antes_impuestos_total,
    cast(BLC_STND.IMP_RES_IMP_GANANCIAS as decimal(20, 2)) AS fc_adm_estado_resultado_impuesto_ganancias,
    cast(BLC_STND.IMP_RES_FINAL as decimal(20, 2)) AS fc_adm_estado_resultado_neto_total,
    NULL AS fc_adm_datos_adicionales_despreciaciones_costo_produccion,
    NULL AS fc_adm_datos_adicionales_despreciaciones_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_gastos_com,
    NULL AS fc_adm_datos_adicionales_despreciaciones_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_otros_egresos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_perdidas_extraordinarias,
    cast(BLC_STND.IMP_AMORTIZACIONES as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_total,
    NULL AS fc_adm_datos_adicionales_desafectacion_costo_produccion,
    NULL AS fc_adm_datos_adicionales_desafectacion_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_desafectacion_gastos_com,
    NULL AS fc_adm_datos_adicionales_desafectacion_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_desafectacion_otros_ingresos,
    NULL AS fc_adm_datos_adicionales_desafectacion_ganancias_extraordinarias,
    NULL AS fc_adm_datos_adicionales_desafectacion_total,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_costo_produccion,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_com,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_egresos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_perdidas_extraordinarias,
    NULL AS fc_adm_datos_adicionales_amortizaciones_inversiones_otros_egresos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_total,
    NULL AS fc_adm_datos_adicionales_previsiones_costo_produccion,
    NULL AS fc_adm_datos_adicionales_previsiones_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_previsiones_gastos_com,
    NULL AS fc_adm_datos_adicionales_previsiones_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_previsiones_otros_egresos,
    NULL AS fc_adm_datos_adicionales_previsiones_perdidas_extraordinarias,
    NULL AS fc_adm_datos_adicionales_previsiones_total,
    NULL AS fc_adm_datos_adicionales_honorarios_directores,
    NULL AS dt_adm_datos_adicionales_distribucion_utilidades_fecha_asamblea_aprobatoria,
    cast(COALESCE(BLC_STND.IMP_DIV_APROB_POST_BAL,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_distribucion_utilidades_dividendos_honorarios_aprobados,
    NULL AS fc_adm_datos_adicionales_distribucion_utilidades_gratificaciones_personal,
    NULL AS fc_adm_datos_adicionales_distribucion_utilidades_provision_impuestos,
    cast(BLC_STND.IMP_COMPRAS as decimal(20, 2)) AS fc_adm_datos_adicionales_compras,
    NULL AS fc_adm_datos_adicionales_gatos_produccion,
    NULL AS fc_adm_datos_adicionales_altas_bienes_uso,
    NULL AS fc_adm_datos_adicionales_bajas_bienes_uso,
    NULL AS fc_adm_datos_adicionales_disminucion_amortizaciones,
    NULL AS fc_adm_datos_adicionales_altas_inversiones_permanentes,
    NULL AS fc_adm_datos_adicionales_bajas_inversiones_permanentes,
    NULL AS fc_adm_datos_adicionales_dividendos_cobrados_soc_vinculadas,
    NULL AS fc_adm_datos_adicionales_altas_intangibles_valor_llave_marcas,
    NULL AS fc_adm_datos_adicionales_altas_intangibles_gastos_reorganizacion_const,
    NULL AS fc_adm_datos_adicionales_bajas_inversiones,
    NULL AS fc_adm_datos_adicionales_variacion_previsiones_pasivo,
    NULL AS fc_adm_datos_adicionales_aportes_capital,
    NULL AS fc_adm_datos_adicionales_constitucion_rrt,
    NULL AS fc_adm_datos_adicionales_desafectacion_rrt,
    NULL AS fc_adm_datos_adicionales_ajustes_resultado_anterior,
    NULL AS fc_adm_datos_adicionales_ajustes_ejercicio_anterior,
    NULL AS fc_adm_datos_adicionales_activos_moneda_extranjera,
    NULL AS fc_adm_datos_adicionales_pasivos_moneda_extranjera,
    NULL AS fc_adm_datos_adicionales_doc_descontados,
    NULL AS fc_adm_datos_adicionales_doc_endosados,
    NULL AS fc_adm_datos_adicionales_ajuste_ejercicio_anteriores_ajuste_inflacion,
    BLC.ORIGEN AS ds_adm_balance_origen,
    from_unixtime(unix_timestamp()) AS dt_adm_fecha_hora_actual,
    date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}','yyyyMM') AS ds_adm_periodo,
    NULL AS dt_adm_fecha_inicio_balance,
    NULL AS cod_adm_tipo_balance,
    RANK() OVER(PARTITION BY BLC_STND.PENUMPER ORDER BY COALESCE(BLC_STND.FEC_BALANCE, date_format('17530101','YYYYMMDD')) DESC, BLC_STND.ID_BALANCE DESC) AS int_adm_orden,
    NULL AS ds_adm_periodo_balance,
    RANK() OVER(PARTITION BY BLC_STND.PENUMPER ORDER BY BLC_STND.PENUMPER DESC, BLC_STND.ID_BALANCE DESC) AS int_adm_orden_periodo_balance
FROM balances_estandarizadas BLC
LEFT JOIN bi_corp_staging.sge_stnd_balances BLC_STND ON BLC_STND.id_balance = BLC.id_balance AND BLC.numero_propuesta = BLC_STND.nro_prop and BLC_STND.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';


-- Inserto en la tabla final los registros de supervision
INSERT INTO TABLE bi_corp_common.stk_adm_empresas_balances_clientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(SUP_BLC_ACT.ID_BLC as bigint) AS id_adm_balance,
    SUP_BLC_ACT.FEC_BLC AS dt_adm_fecha_balance,
    cast(SUP_BLC_ACT.PENUMPER as bigint) AS cod_adm_numero_persona_sge,
    SUP_BLC_ACT.CONSOLIDADO AS flag_adm_consolidado,
    cast(SUP_BLC_ACT.DURACION as int) AS int_adm_duracion,
    'SUPERVISION' AS ds_adm_segmento,
    cast(SUP_BLC_ACT.IMP_CAJABANCOS as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_caja_bancos,
    cast(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_valores_depositar,
    cast(SUP_BLC_ACT.IMP_INVER_TRANS as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_inversiones_corto_plazo,
    cast(COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0) as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_total,
    cast(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_dsventas_doccobrar,
    cast(SUP_BLC_ACT.IMP_DEUGEST_CHRECH as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_deudores_gestion_cheques_rechazados,
    cast(SUP_BLC_ACT.IMP_CTASXCOB as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_sociedades_vinculadas,
    cast(SUP_BLC_ACT.IMP_PREVDS_INC as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_prevision_ds_incobrables,
    cast(COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0) + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0) as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_total,
    cast(SUP_BLC_ACT.IMP_CREFISC as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_creditos_fiscales,
    cast(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_gastos_pagados_adelantado,
    cast(SUP_BLC_ACT.IMP_OTRCRECOV as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_sociedades_vinculadas,
    cast(SUP_BLC_ACT.IMP_HONDIVANT as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_anticipados,
    cast(SUP_BLC_ACT.IMP_HONDIVAPRANT as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_aprobados_anticipados,
    cast(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_aportes_pendientes_integracion_cuentas_socios,
    cast(SUP_BLC_ACT.IMP_DIVCOBRAR as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_dividendos_cobrar,
    cast(SUP_BLC_ACT.IMP_DIVERSOS as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_diversos,
    cast(SUP_BLC_ACT.IMP_PREV_OTR_CRED as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_prevision_otros_creditos,
    cast(COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0)
        + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_total,
    cast(SUP_BLC_ACT.IMP_PROD_TERM_MERC as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_productos_terminados_merc_reventa,
    cast(SUP_BLC_ACT.IMP_PRODENPROS as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_productos_proceso,
    cast(SUP_BLC_ACT.IMP_MATPRIMAS as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_materias_primas,
    cast(SUP_BLC_ACT.IMP_OTROS_INSUMOS as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_otros_insumos,
    cast(SUP_BLC_ACT.IMP_ANTPROVBSCBIO as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_anticipos_proveedores,
    cast(SUP_BLC_ACT.IMP_PREV_DESV as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_prevision_desvalorizaciones,
    cast(COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_total,
    cast(SUP_BLC_ACT.IMP_OTRACTCTES as decimal(20, 2)) AS fc_adm_activo_cp_otros,
    cast(COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0) + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0) + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0)
        + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0) + COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0)
        + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) + COALESCE(IMP_INVER_TRANS, 0)+COALESCE(SUP_BLC_ACT.IMP_OTRACTCTES,0) as decimal(20, 2)) AS fc_adm_activo_cp_total,
    cast(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR as decimal(20, 2)) AS fc_adm_activo_lp_creditos_ventas_dsventas_doccobrar,
    cast(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_creditos_ventas_deudores_gestion_cheques_rechazados,
    cast(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS as decimal(20, 2)) AS fc_adm_activo_lp_creditos_ventas_sociedades_vinculadas,
    cast(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_creditos_ventas_prevision_ds_incobrables,
    cast(COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) as decimal(20, 2)) AS fc_adm_activo_lp_creditos_venta_total,
    cast(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_fiscales,
    cast(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_sociales_vinculadas,
    cast(SUP_BLC_ACT.IMP_GS_PAGXADEL as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_gastos_pagados_adelantado,
    cast(SUP_BLC_ACT.IMP_CTAS_PART_ACC as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_cuentas_particulares_accionistas,
    cast(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_diversos,
    cast(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_otros,
    cast(COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0)
    + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_total,
    cast(SUP_BLC_ACT.IMP_OTRASINV as decimal(20, 2)) AS fc_adm_activo_lp_inversiones_permanentes_diversas,
    cast(SUP_BLC_ACT.IMP_INV_SOCCYV as decimal(20, 2)) AS fc_adm_activo_lp_inversiones_permanentes_soc,
    cast(COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0) + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0) as decimal(20, 2)) AS fc_adm_activo_lp_inversiones_permanentes_total,
    cast(SUP_BLC_ACT.IMP_TERRENOS as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_terrenos,
    cast(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_edificios_mejoras,
    cast(SUP_BLC_ACT.IMP_MAQEQHERR as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_maquinas_equipos_herramientas,
    cast(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_m_utiles_instalaciones_rodados,
    cast(SUP_BLC_ACT.IMP_OTROS as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_otros,
    cast(SUP_BLC_ACT.IMP_OBRAS_CURSO as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_obras_curso,
    cast(SUP_BLC_ACT.IMP_ANTPROVBSUSO as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_anticipo_proveedores,
    cast(SUP_BLC_ACT.IMP_AMORT_ACUM as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_amortizacion_acumulada,
    cast(COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0) + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0) + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0)
        + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0) + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0) as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_total,
    cast(SUP_BLC_ACT.IMP_MARCAS_PATENTES as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_marcas_patentes,
    cast(SUP_BLC_ACT.IMP_GS_ORGYREEST as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_reorganizacion_empresa,
    cast(SUP_BLC_ACT.IMP_VALOR_LLAVE as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_valor_llave,
    cast(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_amort_acumulada,
    cast(COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0) as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_total,
    cast(SUP_BLC_ACT.IMP_BS_CBIO_LP as decimal(20, 2)) AS fc_adm_activo_lp_bienes_cambio,
    cast(SUP_BLC_ACT.IMP_OTRACTNOCTE as decimal(20, 2)) AS fc_adm_activo_lp_otros,
    cast(COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0) + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0)
        + COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0) + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0) + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0) + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0) + COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0)
        + COALESCE(IMP_BS_CBIO_LP, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRACTNOCTE, 0) as decimal(20, 2)) AS fc_adm_activo_lp_total,
    cast(COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0) + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0) + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0)+ COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0)
        + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0) + COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0)
        + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) + COALESCE(IMP_INVER_TRANS, 0)+COALESCE(SUP_BLC_ACT.IMP_OTRACTCTES,0)
        + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR,0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0) + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0)
        + COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0) + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0) + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0)
        + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0) + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0)
        + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0) + COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0)
        + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0)
        + COALESCE(IMP_BS_CBIO_LP, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRACTNOCTE, 0) as decimal(20, 2)) AS fc_adm_activo_total,
    cast(SUP_BLC_PAS.IMP_CCRED_IMP as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_costo_credito_importacion,
    cast(SUP_BLC_PAS.IMP_DEUFINBAN as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_deuda,
    cast(SUP_BLC_PAS.IMP_OTRDEUFINLP as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_obligaciones_negociables,
    cast(SUP_BLC_PAS.IMP_TCTEDEULP as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_porcion_deuda_lp,
    cast(COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINBAN, 0)
        + COALESCE(SUP_BLC_PAS.IMP_OTRDEUFINLP, 0) + COALESCE(SUP_BLC_PAS.IMP_TCTEDEULP, 0) as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_total,
    cast(SUP_BLC_PAS.IMP_PROVEEDORES as decimal(20, 2)) AS fc_adm_pasivo_cp_importe_proveedores,
    cast(SUP_BLC_PAS.IMP_PROVSUBAFIL as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_art33_comercial,
    cast(SUP_BLC_PAS.IMP_DEUFINSOC as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_art33_financiera,
    cast(SUP_BLC_PAS.IMP_OTRAS_DEUDAS as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_art33_otras,
    cast(COALESCE(SUP_BLC_PAS.IMP_PROVSUBAFIL, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS, 0) as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_art33_total,
    cast(SUP_BLC_PAS.IMP_MORAT_CTES as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_fiscales_moratorias,
    cast(SUP_BLC_PAS.IMP_DIFIMP_CTES as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_fiscales_diferimiento_impositivo,
    cast(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_fiscales_otras_ds,
    cast(COALESCE(SUP_BLC_PAS.IMP_MORAT_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES, 0) as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_sociales_fiscales_total,
    cast(SUP_BLC_PAS.IMP_OTRPASCP as decimal(20, 2)) AS fc_adm_pasivo_cp_otros_anticipos_clientes,
    cast(SUP_BLC_PAS.IMP_HONYDIVAPAG as decimal(20, 2)) AS fc_adm_pasivo_cp_dividendos_honorarios_pagar,
    cast(SUP_BLC_PAS.IMP_PREVPASCTE as decimal(20, 2)) AS fc_adm_pasivo_cp_previsiones,
    cast(COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINBAN, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDEUFINLP, 0)
        + COALESCE(SUP_BLC_PAS.IMP_TCTEDEULP, 0) + COALESCE(SUP_BLC_PAS.IMP_PROVEEDORES, 0) + COALESCE(SUP_BLC_PAS.IMP_PROVSUBAFIL, 0)
        + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS, 0) + COALESCE(SUP_BLC_PAS.IMP_MORAT_CTES, 0)
        + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRPASCP, 0)
        + COALESCE(SUP_BLC_PAS.IMP_HONYDIVAPAG, 0) + COALESCE(SUP_BLC_PAS.IMP_PREVPASCTE, 0) as decimal(20, 2)) AS fc_adm_pasivo_cp_total,
    cast(SUP_BLC_PAS.IMP_CCRED_IMP_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_credito_importacion,
    cast(SUP_BLC_PAS.IMP_DEUBANCLP as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_deuda_financiera,
    cast(SUP_BLC_PAS.IMP_OBLIGNEG_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_obligaciones_negociables,
    cast(SUP_BLC_PAS.IMP_CTASAPAGLP as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_proveedores,
    cast(COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUBANCLP, 0) + COALESCE(SUP_BLC_PAS.IMP_OBLIGNEG_NCTES, 0) as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_total,
    cast(SUP_BLC_PAS.IMP_CTASAPAGSUBAFIL as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_articulo33_comercial,
    cast(SUP_BLC_PAS.IMP_DEUFINSOC_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_articulo33_financiera,
    cast(SUP_BLC_PAS.IMP_OTRAS_DEUDAS_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_articulo33_otras,
    cast(COALESCE(SUP_BLC_PAS.IMP_CTASAPAGSUBAFIL, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS_NCTES, 0) as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_articulo33_total,
    cast(SUP_BLC_PAS.IMP_MORAT_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_fiscales_moratorias,
    cast(SUP_BLC_PAS.IMP_DIFIMP_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_fiscales_diferimiento_impositivo,
    cast(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_NCTES as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_fiscales_otros_ds,
    cast(COALESCE(SUP_BLC_PAS.IMP_MORAT_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_NCTES, 0) as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_sociales_fiscales_total,
    cast(SUP_BLC_PAS.IMP_OTRPASLP as decimal(20, 2)) AS fc_adm_pasivo_lp_otros,
    cast(SUP_BLC_PAS.IMP_PREVPASNCTE as decimal(20, 2)) AS fc_adm_pasivo_lp_previsiones,
    cast(SUP_BLC_PAS.IMP_PART3ENSOCC as decimal(20, 2)) AS fc_adm_pasivo_lp_participacion_minoritaria,
    cast(COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_DEUBANCLP,0)
        +COALESCE(SUP_BLC_PAS.IMP_OBLIGNEG_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_CTASAPAGLP,0)
        +COALESCE(SUP_BLC_PAS.IMP_CTASAPAGSUBAFIL,0)
        +COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_MORAT_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_DIFIMP_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_NCTES,0)
        +COALESCE(SUP_BLC_PAS.IMP_OTRPASLP,0)
        +COALESCE(SUP_BLC_PAS.IMP_PREVPASNCTE,0)
        +COALESCE(SUP_BLC_PAS.IMP_PART3ENSOCC,0) as decimal(20, 2)) AS fc_adm_pasivo_lp_total,
    cast(COALESCE(SUP_BLC_PAS.IMP_CSSUSCINT,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_capital,
    cast(COALESCE(SUP_BLC_PAS.IMP_RESVREVBSUSO,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_reserva_revaluo_tecnico,
    cast(COALESCE(SUP_BLC_PAS.IMP_RESVLEGAL,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_reservas_resultados,
    cast(COALESCE(SUP_BLC_PAS.IMP_RESNOASIG,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_resultados_no_asignados,
    cast(COALESCE(SUP_BLC_PAS.IMP_GANNETEJER,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_resultado_ejercicio,
    cast(COALESCE(SUP_BLC_PAS.IMP_CSSUSCINT,0)
        +COALESCE(IMP_RESVREVBSUSO,0)
        +COALESCE(IMP_RESVLEGAL,0)
        +COALESCE(IMP_RESNOASIG,0)
        +COALESCE(IMP_GANNETEJER,0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_total,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_ventas_netas,
    cast(COALESCE(SUP_BLC_ER.IMP_CSTOVTAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_costo_ventas,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)+COALESCE(IMP_CSTOVTAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_total,
    cast(COALESCE(SUP_BLC_ER.IMP_GSADMIN,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_administrativos,
    cast(COALESCE(SUP_BLC_ER.IMP_GSCOMERYOTR,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_comerciales,
    cast(COALESCE(SUP_BLC_ER.TOT_GSOPERAT,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_otros,
    cast(COALESCE(SUP_BLC_ER.IMP_ING_PROM_IND,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_ingresos_promocion_industrial,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)+COALESCE(IMP_CSTOVTAS,0)
        +COALESCE(IMP_GSADMIN,0)
        +COALESCE(IMP_GSCOMERYOTR,0)
        +COALESCE(TOT_GSOPERAT,0)
        +COALESCE(IMP_ING_PROM_IND,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_total,
    cast(COALESCE(SUP_BLC_ER.IMP_RDOINVERS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_inversiones_permanentes,
    cast(COALESCE(SUP_BLC_ER.IMP_REINT_EXP,0) as decimal(20, 2)) AS fc_adm_estado_resultado_reintegros_exportaciones,
    cast(COALESCE(SUP_BLC_ER.IMP_OTROS_INGRESOS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_otros_ingresos,
    cast(COALESCE(SUP_BLC_ER.IMP_OTROS_EGRESOS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_otros_egresos,
    cast(COALESCE(SUP_BLC_ER.IMP_INTERGAN,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_intereses,
    cast(COALESCE(SUP_BLC_ER.IMP_REI_RES_FIN_GEN_ACTIVOS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_rei,
    cast(COALESCE(SUP_BLC_ER.IMP_DIFCBIO_RES_FIN_GEN_ACT,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_diferencias_cambio,
    cast(COALESCE(SUP_BLC_ER.IMP_RES_TEN_RES_FIN_GEN_ACT,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_tenencia,
    cast(COALESCE(SUP_BLC_ER.IMP_OTROS_RES_FIN_GEN_ACT,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_otros,
    cast(COALESCE(SUP_BLC_ER.IMP_INTERGAN,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_activos_total,
    cast(COALESCE(SUP_BLC_ER.IMP_INTERPAG ,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_intereses,
    cast(COALESCE(SUP_BLC_ER.IMP_REI_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_rei,
    cast(COALESCE(SUP_BLC_ER.IMP_DIFCBIO_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_diferencias_cambio,
    cast(COALESCE(SUP_BLC_ER.IMP_RESTEN_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_tenencia,
    cast(COALESCE(SUP_BLC_ER.IMP_OTROS_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_otros,
    cast(COALESCE(SUP_BLC_ER.IMP_INTERPAG,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_generado_pasivos_total,
    cast(COALESCE(SUP_BLC_ER.IMP_INTERGAN,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_INTERPAG,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_total,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)+COALESCE(IMP_CSTOVTAS,0)+COALESCE(IMP_GSADMIN,0)+COALESCE(IMP_GSCOMERYOTR,0)+COALESCE(TOT_GSOPERAT,0)+COALESCE(IMP_ING_PROM_IND,0)
        +COALESCE(IMP_RDOINVERS,0)
        +COALESCE(IMP_REINT_EXP,0)
        +COALESCE(IMP_OTROS_INGRESOS,0)
        +COALESCE(IMP_OTROS_EGRESOS,0)
        +COALESCE(IMP_INTERGAN,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_INTERPAG,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_operaciones_ordinarias_total,
    cast(COALESCE(SUP_BLC_ER.IMP_GCIAEXTRA,0) as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_ganancias_extraordinarias,
    cast(COALESCE(SUP_BLC_ER.IMP_PERDEXTRA,0) as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_perdidas_extraordinarias,
    cast(COALESCE(SUP_BLC_ER.IMP_PART3ENRESSOCC,0) as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_participacion_minoritaria,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)+COALESCE(IMP_CSTOVTAS,0)+COALESCE(IMP_GSADMIN,0)+COALESCE(IMP_GSCOMERYOTR,0)+COALESCE(TOT_GSOPERAT,0)+COALESCE(IMP_ING_PROM_IND,0)
        +COALESCE(IMP_RDOINVERS,0)
        +COALESCE(IMP_REINT_EXP,0)
        +COALESCE(IMP_OTROS_INGRESOS,0)
        +COALESCE(IMP_OTROS_EGRESOS,0)
        +COALESCE(IMP_INTERGAN,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_INTERPAG,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_GCIAEXTRA,0)
        +COALESCE(IMP_PERDEXTRA,0)
        +COALESCE(IMP_PART3ENRESSOCC,0) as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_total,
    cast(COALESCE(SUP_BLC_ER.IMP_IMPUESTOS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_impuesto_ganancias,
    cast(COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)+COALESCE(IMP_CSTOVTAS,0)+COALESCE(IMP_GSADMIN,0)+COALESCE(IMP_GSCOMERYOTR,0)+COALESCE(TOT_GSOPERAT,0)+COALESCE(IMP_ING_PROM_IND,0)
        +COALESCE(IMP_RDOINVERS,0)
        +COALESCE(IMP_REINT_EXP,0)
        +COALESCE(IMP_OTROS_INGRESOS,0)
        +COALESCE(IMP_OTROS_EGRESOS,0)
        +COALESCE(IMP_INTERGAN,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0)
        +COALESCE(IMP_INTERPAG,0)
        +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0)
        +COALESCE(IMP_GCIAEXTRA,0)
        +COALESCE(IMP_PERDEXTRA,0)
        +COALESCE(IMP_PART3ENRESSOCC,0)
        +COALESCE(IMP_IMPUESTOS,0) as decimal(20, 2)) AS fc_adm_estado_resultado_neto_total,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_CPROD,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_costo_produccion,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_GSADM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_gastos_administrativos,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_GSCOM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_gastos_com,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_OTRGSOP,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_otros_gastos_operativos,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_OTREGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_otros_egresos,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_PERDEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_perdidas_extraordinarias,
    cast(COALESCE(SUP_BLC_DA.IMP_DEPREC_CPROD,0)
        +COALESCE(IMP_DEPREC_GSADM,0)
        +COALESCE(IMP_DEPREC_GSCOM,0)
        +COALESCE(IMP_DEPREC_OTRGSOP,0)
        +COALESCE(IMP_DEPREC_OTREGRESOS,0)
        +COALESCE(IMP_DEPREC_PERDEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_total,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_CPROD,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_costo_produccion,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GSADM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_gastos_administrativos,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GSCOM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_gastos_com,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_OTRGSOP,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_otros_gastos_operativos,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_OTRINGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_otros_ingresos,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GANEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_ganancias_extraordinarias,
    cast(COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_CPROD,0)
        +COALESCE(IMP_DESRESRTBSUSO_GSADM,0)
        +COALESCE(IMP_DESRESRTBSUSO_GSCOM,0)
        +COALESCE(IMP_DESRESRTBSUSO_OTRGSOP,0)
        +COALESCE(IMP_DESRESRTBSUSO_OTRINGRESOS,0)
        +COALESCE(IMP_DESRESRTBSUSO_GANEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_total,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_CPROD,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_costo_produccion,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_GSADM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_administrativos,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_GSCOM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_com,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_OTRGSOP,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_gastos_operativos,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_OTREGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_egresos,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_PERDEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_intangibles_perdidas_extraordinarias,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINV_OTREGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_inversiones_otros_egresos,
    cast(COALESCE(SUP_BLC_DA.IMP_AMORTINT_CPROD,0)
        +COALESCE(IMP_AMORTINT_GSADM,0)
        +COALESCE(IMP_AMORTINT_GSCOM,0)
        +COALESCE(IMP_AMORTINT_OTRGSOP,0)
        +COALESCE(IMP_AMORTINT_OTREGRESOS,0)
        +COALESCE(IMP_AMORTINT_PERDEXT,0)
        +COALESCE(IMP_AMORTINV_OTREGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_total,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_CPROD,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_costo_produccion,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_GSADM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_gastos_administrativos,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_GSCOM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_gastos_com,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_OTRGSOP,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_otros_gastos_operativos,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_OTREGRESOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_otros_egresos,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_PERDEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_perdidas_extraordinarias,
    cast(COALESCE(SUP_BLC_DA.IMP_PREV_CPROD,0)
        +COALESCE(IMP_PREV_GSADM,0)
        +COALESCE(IMP_PREV_GSCOM,0)
        +COALESCE(IMP_PREV_OTRGSOP,0)
        +COALESCE(IMP_PREV_OTREGRESOS,0)
        +COALESCE(IMP_PREV_PERDEXT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_previsiones_total,
    cast(COALESCE(SUP_BLC_DA.IMP_HONDIR,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_honorarios_directores,
    FEC_ASAMBLEA AS dt_adm_datos_adicionales_distribucion_utilidades_fecha_asamblea_aprobatoria,
    cast(COALESCE(IMP_DIVHAPXPAG,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_distribucion_utilidades_dividendos_honorarios_aprobados,
    cast(COALESCE(SUP_BLC_DA.IMP_GRATIFPERSONAL,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_distribucion_utilidades_gratificaciones_personal,
    cast(COALESCE(IMP_PROVIMP,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_distribucion_utilidades_provision_impuestos,
    cast(COALESCE(IMP_COMPRAS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_compras,
    cast(COALESCE(SUP_BLC_DA.IMP_GTOSPROD,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_gatos_produccion,
    cast(COALESCE(SUP_BLC_DA.IMP_ALTAS_BIENES_USO,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_altas_bienes_uso,
    cast(COALESCE(SUP_BLC_DA.IMP_BAJAS_BIENES_USO,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_bajas_bienes_uso,
    cast(COALESCE(SUP_BLC_DA.IMP_DISMAMORT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_disminucion_amortizaciones,
    cast(COALESCE(SUP_BLC_DA.IMP_ALTAS_INV_PERM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_altas_inversiones_permanentes,
    cast(COALESCE(SUP_BLC_DA.IMP_BAJAS_INV_PERM,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_bajas_inversiones_permanentes,
    cast(COALESCE(SUP_BLC_DA.IMP_DIVCOB_SOCVINCULADAS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_dividendos_cobrados_soc_vinculadas,
    cast(COALESCE(SUP_BLC_DA.IMP_ALTAS_INT_VAL_LLAVE,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_altas_intangibles_valor_llave_marcas,
    cast(COALESCE(SUP_BLC_DA.IMP_ALTAS_INT_GTOS_REORG,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_altas_intangibles_gastos_reorganizacion_const,
    cast(COALESCE(SUP_BLC_DA.IMP_BAJAS_INT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_bajas_inversiones,
    cast(COALESCE(SUP_BLC_DA.IMP_VAR_PREV_PASIVO,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_variacion_previsiones_pasivo,
    cast(COALESCE(SUP_BLC_DA.IMP_APORTES_CAPITAL,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_aportes_capital,
    cast(COALESCE(SUP_BLC_DA.IMP_CONSTRRT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_constitucion_rrt,
    cast(COALESCE(SUP_BLC_DA.IMP_DESAFRRT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_desafectacion_rrt,
    cast(COALESCE(SUP_BLC_DA.IMP_AJUSTES_RESANTNAF,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_ajustes_resultado_anterior,
    cast(COALESCE(SUP_BLC_DA.IMP_AJUSTE_EJERANT,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_ajustes_ejercicio_anterior,
    cast(COALESCE(SUP_BLC_DA.IMP_ACT_MONEX,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_activos_moneda_extranjera,
    cast(COALESCE(SUP_BLC_DA.IMP_PAS_MONEX,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_pasivos_moneda_extranjera,
    cast(COALESCE(IMP_DOC_DESC,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_doc_descontados,
    cast(COALESCE(IMP_DOC_ENDOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_doc_endosados,
    cast(COALESCE(SUP_BLC_DA.IMP_AJEJERANT_AJINFLA,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_ajuste_ejercicio_anteriores_ajuste_inflacion,
    BLC.ORIGEN AS ds_adm_balance_origen,
    from_unixtime(unix_timestamp()) AS dt_adm_fecha_hora_actual,
    date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}','yyyyMM') AS ds_adm_periodo,
    SUP_BLC_ACT.FEC_INI_BLC AS dt_adm_fecha_inicio_balance,
    SUP_BLC_ACT.TPO_BLC AS cod_adm_tipo_balance,
    RANK() OVER(PARTITION BY SUP_BLC_ACT.PENUMPER ORDER BY COALESCE(SUP_BLC_ACT.FEC_BLC, date_format('17530101','YYYYMMDD')) DESC, SUP_BLC_ACT.ID_BLC DESC) AS int_adm_orden,
    date_format(SUP_BLC_ACT.FEC_INI_BLC, 'yyyyMM') AS ds_adm_periodo_balance,
    RANK() OVER(PARTITION BY SUP_BLC_ACT.PENUMPER, SUP_BLC_ACT.FEC_INI_BLC ORDER BY SUP_BLC_ACT.PENUMPER DESC, SUP_BLC_ACT.FEC_INI_BLC DESC, SUP_BLC_ACT.ID_BLC DESC) AS int_adm_orden_periodo_balance
FROM balances_supervision BLC
LEFT JOIN bi_corp_staging.sge_blc_act SUP_BLC_ACT ON SUP_BLC_ACT.id_blc = BLC.id_balance and SUP_BLC_ACT.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
LEFT JOIN bi_corp_staging.sge_blc_pas SUP_BLC_PAS ON SUP_BLC_PAS.id_blc = BLC.id_balance and SUP_BLC_PAS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
LEFT JOIN bi_corp_staging.sge_blc_eres SUP_BLC_ER ON SUP_BLC_ER.id_blc = BLC.id_balance and SUP_BLC_ER.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
LEFT OUTER JOIN bi_corp_staging.sge_blc_dadic SUP_BLC_DA ON SUP_BLC_DA.id_blc = BLC.id_balance and SUP_BLC_DA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';


-- Inserto en la tabla final los registros de CPP
INSERT INTO TABLE bi_corp_common.stk_adm_empresas_balances_clientes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
SELECT
    cast(CPP_BLC.ID_BALANCE as bigint) AS id_adm_balance,
    CPP_BLC.FEC_BALANCE AS dt_adm_fecha_balance,
    cast(CPP_BLC.PENUMPER as bigint) AS cod_adm_numero_persona_sge,
    'N' AS flag_adm_consolidado,
    cast(CPP_BLC.TPO_BALANCE as int) AS int_adm_duracion,
    'CPP' AS ds_adm_segmento,
    cast(CPP_BLC.DISP_ACT_CP as decimal(20, 2)) AS fc_adm_activo_cp_disponibles_caja_bancos,
    NULL AS fc_adm_activo_cp_disponibles_valores_depositar,
    NULL AS fc_adm_activo_cp_disponibles_inversiones_corto_plazo,
    NULL AS fc_adm_activo_cp_disponibles_total,
    NULL AS fc_adm_activo_cp_creditos_ventas_dsventas_doccobrar,
    NULL AS fc_adm_activo_cp_creditos_ventas_deudores_gestion_cheques_rechazados,
    NULL AS fc_adm_activo_cp_creditos_ventas_sociedades_vinculadas,
    NULL AS fc_adm_activo_cp_creditos_ventas_prevision_ds_incobrables,
    cast(CPP_BLC.CRED_VTAS_ACT_CP as decimal(20, 2)) AS fc_adm_activo_cp_creditos_ventas_total,
    cast(CPP_BLC.CRED_OTROS_ACT_CP as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_creditos_fiscales,
    NULL AS fc_adm_activo_cp_otras_cuentas_gastos_pagados_adelantado,
    NULL AS fc_adm_activo_cp_otras_cuentas_sociedades_vinculadas,
    cast(CPP_BLC.CUEN_PART_SOC_CP as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_anticipados,
    cast(CPP_BLC.CUEN_PART_SOC_CP as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_aprobados_anticipados,
    NULL AS fc_adm_activo_cp_otras_cuentas_aportes_pendientes_integracion_cuentas_socios,
    NULL AS fc_adm_activo_cp_otras_cuentas_dividendos_cobrar,
    cast(CPP_BLC.OTROS_CORR_ACT_CP as decimal(20, 2)) AS fc_adm_activo_cp_otras_cuentas_diversos,
    NULL AS fc_adm_activo_cp_otras_cuentas_prevision_otros_creditos,
    NULL AS fc_adm_activo_cp_otras_cuentas_total,
    NULL AS fc_adm_activo_cp_bienes_cambio_productos_terminados_merc_reventa,
    NULL AS fc_adm_activo_cp_bienes_cambio_productos_proceso,
    NULL AS fc_adm_activo_cp_bienes_cambio_materias_primas,
    NULL AS fc_adm_activo_cp_bienes_cambio_otros_insumos,
    NULL AS fc_adm_activo_cp_bienes_cambio_anticipos_proveedores,
    NULL AS fc_adm_activo_cp_bienes_cambio_prevision_desvalorizaciones,
    cast(CPP_BLC.BIEN_CAMBIO_ACT_CP as decimal(20, 2)) AS fc_adm_activo_cp_bienes_cambio_total,
    NULL AS fc_adm_activo_cp_otros,
    cast(COALESCE(CPP_BLC.DISP_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_VTAS_ACT_CP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_ACT_CP, 0)
        + COALESCE(CPP_BLC.CRED_OTROS_ACT_CP, 0) + COALESCE(CPP_BLC.OTROS_CORR_ACT_CP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_CP, 0) as decimal(20, 2)) AS fc_adm_activo_cp_total,
    NULL AS fc_adm_activo_lp_creditos_ventas_dsventas_doccobrar,
    NULL AS fc_adm_activo_lp_creditos_ventas_deudores_gestion_cheques_rechazados,
    NULL AS fc_adm_activo_lp_creditos_ventas_sociedades_vinculadas,
    NULL AS fc_adm_activo_lp_creditos_ventas_prevision_ds_incobrables,
    cast(CPP_BLC.CTAS_COBRAR_ACT_LP as decimal(20, 2)) AS fc_adm_activo_lp_creditos_venta_total,
    NULL AS fc_adm_activo_lp_otros_creditos_fiscales,
    NULL AS fc_adm_activo_lp_otros_creditos_sociales_vinculadas,
    NULL AS fc_adm_activo_lp_otros_creditos_gastos_pagados_adelantado,
    cast(CPP_BLC.CUEN_PART_SOC_LP as decimal(20, 2)) AS fc_adm_activo_lp_otros_creditos_cuentas_particulares_accionistas,
    NULL AS fc_adm_activo_lp_otros_creditos_diversos,
    NULL AS fc_adm_activo_lp_otros_creditos_otros,
    NULL AS fc_adm_activo_lp_otros_creditos_total,
    cast(CPP_BLC.INV_ACT_LP as decimal(20, 2)) AS fc_adm_activo_lp_inversiones_permanentes_diversas,
    NULL AS fc_adm_activo_lp_inversiones_permanentes_soc,
    NULL AS fc_adm_activo_lp_inversiones_permanentes_total,
    NULL AS fc_adm_activo_lp_bienes_uso_terrenos,
    NULL AS fc_adm_activo_lp_bienes_uso_edificios_mejoras,
    NULL AS fc_adm_activo_lp_bienes_uso_maquinas_equipos_herramientas,
    NULL AS fc_adm_activo_lp_bienes_uso_m_utiles_instalaciones_rodados,
    NULL AS fc_adm_activo_lp_bienes_uso_otros,
    NULL AS fc_adm_activo_lp_bienes_uso_obras_curso,
    NULL AS fc_adm_activo_lp_bienes_uso_anticipo_proveedores,
    NULL AS fc_adm_activo_lp_bienes_uso_amortizacion_acumulada,
    cast(CPP_BLC.BIEN_USO_ACT_LP as decimal(20, 2)) AS fc_adm_activo_lp_bienes_uso_total,
    NULL AS fc_adm_activo_lp_bienes_intangibles_marcas_patentes,
    NULL AS fc_adm_activo_lp_bienes_intangibles_reorganizacion_empresa,
    NULL AS fc_adm_activo_lp_bienes_intangibles_valor_llave,
    NULL AS fc_adm_activo_lp_bienes_intangibles_amort_acumulada,
    cast(CPP_BLC.BIEN_INTANGIBLE_AC as decimal(20, 2)) AS fc_adm_activo_lp_bienes_intangibles_total,
    cast(CPP_BLC.BIEN_CAMBIO_NO_CORR as decimal(20, 2)) AS fc_adm_activo_lp_bienes_cambio,
    cast(CPP_BLC.OTROS_NCORR_ACT_LP as decimal(20, 2)) AS fc_adm_activo_lp_otros,
    cast(COALESCE(CPP_BLC.INV_ACT_LP, 0) + COALESCE(CPP_BLC.CTAS_COBRAR_ACT_LP, 0)
        + COALESCE(CPP_BLC.BIEN_CAMBIO_NO_CORR, 0) + COALESCE(CPP_BLC.OTROS_NCORR_ACT_LP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_LP, 0)
        + COALESCE(CPP_BLC.BIEN_USO_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_INTANGIBLE_AC, 0) as decimal(20, 2)) AS fc_adm_activo_lp_total,
    cast(COALESCE(CPP_BLC.DISP_ACT_CP, 0)
        + COALESCE(CPP_BLC.CRED_VTAS_ACT_CP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_OTROS_ACT_CP, 0)
        + COALESCE(CPP_BLC.OTROS_CORR_ACT_CP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_CP, 0) + COALESCE(CPP_BLC.INV_ACT_LP, 0) + COALESCE(CPP_BLC.CTAS_COBRAR_ACT_LP, 0)
        + COALESCE(CPP_BLC.BIEN_CAMBIO_NO_CORR, 0) + COALESCE(CPP_BLC.OTROS_NCORR_ACT_LP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_LP, 0)
        + COALESCE(CPP_BLC.BIEN_USO_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_INTANGIBLE_AC, 0) as decimal(20, 2)) AS fc_adm_activo_total,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_costo_credito_importacion,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_deuda,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_obligaciones_negociables,
    NULL AS fc_adm_pasivo_cp_deuda_bancaria_financiera_porcion_deuda_lp,
    cast(COALESCE(CPP_BLC.DEU_BAN_PAS_CP,0) as decimal(20, 2)) AS fc_adm_pasivo_cp_deuda_bancaria_financiera_total,
    NULL AS fc_adm_pasivo_cp_importe_proveedores,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_comercial,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_financiera,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_otras,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_art33_total,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_moratorias,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_diferimiento_impositivo,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_otras_ds,
    NULL AS fc_adm_pasivo_cp_deuda_sociales_fiscales_total,
    NULL AS fc_adm_pasivo_cp_otros_anticipos_clientes,
    NULL AS fc_adm_pasivo_cp_dividendos_honorarios_pagar,
    NULL AS fc_adm_pasivo_cp_previsiones,
    cast(COALESCE(CPP_BLC.DEU_BAN_PAS_CP, 0) + COALESCE(CPP_BLC.PROV_PAS_CP, 0)
        + COALESCE(CPP_BLC.DEU_SOC_PAS_CP, 0) + COALESCE(CPP_BLC.OTROS_CORR_PAS_CP, 0) + COALESCE(CPP_BLC.DIV_HON_PAGAR_CP, 0) as decimal(20, 2)) AS fc_adm_pasivo_cp_total,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_credito_importacion,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_deuda_financiera,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_obligaciones_negociables,
    NULL AS fc_adm_pasivo_lp_deuda_bancaria_financiera_proveedores,
    cast(COALESCE(CPP_BLC.DEU_BAN_PAS_LP,0) as decimal(20, 2)) AS fc_adm_pasivo_lp_deuda_bancaria_financiera_total,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_comercial,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_financiera,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_otras,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_articulo33_total,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_moratorias,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_diferimiento_impositivo,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_otros_ds,
    NULL AS fc_adm_pasivo_lp_deuda_sociales_fiscales_total,
    NULL AS fc_adm_pasivo_lp_otros,
    NULL AS fc_adm_pasivo_lp_previsiones,
    NULL AS fc_adm_pasivo_lp_participacion_minoritaria,
    cast(COALESCE(CPP_BLC.DEU_BAN_PAS_LP, 0)
        + COALESCE(CPP_BLC.PROV_PAS_LP, 0) + COALESCE(CPP_BLC.DEU_SOC_PAS_LP, 0) + COALESCE(CPP_BLC.OTROS_NO_CORR_PAS_LP, 0) + COALESCE(CPP_BLC.PREVISIONES_LP, 0) as decimal(20, 2))
        AS fc_adm_pasivo_lp_total,
    NULL AS fc_adm_pasivo_patrimonio_neto_capital,
    NULL AS fc_adm_pasivo_patrimonio_neto_reserva_revaluo_tecnico,
    NULL AS fc_adm_pasivo_patrimonio_neto_reservas_resultados,
    NULL AS fc_adm_pasivo_patrimonio_neto_resultados_no_asignados,
    NULL AS fc_adm_pasivo_patrimonio_neto_resultado_ejercicio,
    cast(COALESCE(CPP_BLC.MON_CAPITAL_SOCIAL, 0) + COALESCE(CPP_BLC.MON_RESERVA_TEC, 0) + COALESCE(CPP_BLC.MON_RESERVA_RES, 0)
        + COALESCE(CPP_BLC.MON_RES_NASIGNADOS, 0) + COALESCE(CPP_BLC.RES_EJERCICIO, 0) as decimal(20, 2)) AS fc_adm_pasivo_patrimonio_neto_total,
    cast(CPP_BLC.ING_VTAS_SERVICIOS as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_ventas_netas,
    cast(CPP_BLC.COST_VENTAS as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_costo_ventas,
    cast(COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) as decimal(20, 2)) AS fc_adm_estado_resultado_bruto_total,
    cast(CPP_BLC.GTOS_ADMIN_RES as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_administrativos,
    cast(CPP_BLC.GTOS_COMERC_RES as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_gastos_comerciales,
    cast(CPP_BLC.OTROS_GTOS_RES as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_otros,
    NULL AS fc_adm_estado_resultado_operativo_ingresos_promocion_industrial,
    cast(COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) as decimal(20, 2)) AS fc_adm_estado_resultado_operativo_total,
    NULL AS fc_adm_estado_resultado_inversiones_permanentes,
    NULL AS fc_adm_estado_resultado_reintegros_exportaciones,
    cast(CPP_BLC.OTROS_ING_RES as decimal(20, 2)) AS fc_adm_estado_resultado_otros_ingresos,
    cast(CPP_BLC.OTROS_EGR_RES as decimal(20, 2)) AS fc_adm_estado_resultado_otros_egresos,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_intereses,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_rei,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_diferencias_cambio,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_tenencia,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_otros,
    NULL AS fc_adm_estado_resultado_financiero_generado_activos_total,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_intereses,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_rei,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_diferencias_cambio,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_tenencia,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_otros,
    NULL AS fc_adm_estado_resultado_financiero_generado_pasivos_total,
    cast(CPP_BLC.RES_FINANCIEROS as decimal(20, 2)) AS fc_adm_estado_resultado_financiero_total,
    cast(COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) + COALESCE(CPP_BLC.RES_FINANCIEROS, 0) as decimal(20, 2)) AS fc_adm_estado_resultado_operaciones_ordinarias_total,
    cast(CPP_BLC.ING_EXTRAORDINARIO as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_ganancias_extraordinarias,
    cast(CPP_BLC.EGR_EXTRAORDINARIO as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_perdidas_extraordinarias,
    NULL AS fc_adm_estado_resultado_antes_impuestos_participacion_minoritaria,
    cast(COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(COST_VENTAS, 0) - COALESCE(GTOS_ADMIN_RES, 0) - COALESCE(GTOS_COMERC_RES, 0) - COALESCE(OTROS_GTOS_RES, 0) + COALESCE(OTROS_ING_RES, 0) - COALESCE(OTROS_EGR_RES, 0) + COALESCE(RES_FINANCIEROS, 0) + COALESCE(ING_EXTRAORDINARIO, 0) - COALESCE(EGR_EXTRAORDINARIO, 0) as decimal(20, 2)) AS fc_adm_estado_resultado_antes_impuestos_total,
    cast(IMPU_RES as decimal(20, 2)) AS fc_adm_estado_resultado_impuesto_ganancias,
    cast(COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) + COALESCE(CPP_BLC.RES_FINANCIEROS, 0) + COALESCE(CPP_BLC.ING_EXTRAORDINARIO, 0) - COALESCE(CPP_BLC.EGR_EXTRAORDINARIO, 0) - COALESCE(CPP_BLC.IMPU_RES, 0) as decimal(20, 2)) AS fc_adm_estado_resultado_neto_total,
    NULL AS fc_adm_datos_adicionales_despreciaciones_costo_produccion,
    NULL AS fc_adm_datos_adicionales_despreciaciones_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_gastos_com,
    NULL AS fc_adm_datos_adicionales_despreciaciones_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_otros_egresos,
    NULL AS fc_adm_datos_adicionales_despreciaciones_perdidas_extraordinarias,
    cast(CPP_BLC.AMORT_BIEN_USO as decimal(20, 2)) AS fc_adm_datos_adicionales_despreciaciones_total,
    NULL AS fc_adm_datos_adicionales_desafectacion_costo_produccion,
    NULL AS fc_adm_datos_adicionales_desafectacion_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_desafectacion_gastos_com,
    NULL AS fc_adm_datos_adicionales_desafectacion_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_desafectacion_otros_ingresos,
    NULL AS fc_adm_datos_adicionales_desafectacion_ganancias_extraordinarias,
    NULL AS fc_adm_datos_adicionales_desafectacion_total,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_costo_produccion,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_com,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_otros_egresos,
    NULL AS fc_adm_datos_adicionales_amortizaciones_intangibles_perdidas_extraordinarias,
    NULL AS fc_adm_datos_adicionales_amortizaciones_inversiones_otros_egresos,
    cast(CPP_BLC.AMORT_BIEN_INTANGIBLE as decimal(20, 2)) AS fc_adm_datos_adicionales_amortizaciones_total,
    NULL AS fc_adm_datos_adicionales_previsiones_costo_produccion,
    NULL AS fc_adm_datos_adicionales_previsiones_gastos_administrativos,
    NULL AS fc_adm_datos_adicionales_previsiones_gastos_com,
    NULL AS fc_adm_datos_adicionales_previsiones_otros_gastos_operativos,
    NULL AS fc_adm_datos_adicionales_previsiones_otros_egresos,
    NULL AS fc_adm_datos_adicionales_previsiones_perdidas_extraordinarias,
    NULL AS fc_adm_datos_adicionales_previsiones_total,
    NULL AS fc_adm_datos_adicionales_honorarios_directores,
    CPP_BLC.FEC_ASAMBLEA AS dt_adm_datos_adicionales_distribucion_utilidades_fecha_asamblea_aprobatoria,
    cast(COALESCE(CPP_BLC.DIV_APROBADOS,0)+COALESCE(CPP_BLC.HON_APROBADOS,0) as decimal(20, 2)) AS fc_adm_datos_adicionales_distribucion_utilidades_dividendos_honorarios_aprobados,
    NULL AS fc_adm_datos_adicionales_distribucion_utilidades_gratificaciones_personal,
    NULL AS fc_adm_datos_adicionales_distribucion_utilidades_provision_impuestos,
    cast(CPP_BLC.MON_COMPRAS as decimal(20, 2)) AS fc_adm_datos_adicionales_compras,
    NULL AS fc_adm_datos_adicionales_gatos_produccion,
    NULL AS fc_adm_datos_adicionales_altas_bienes_uso,
    NULL AS fc_adm_datos_adicionales_bajas_bienes_uso,
    NULL AS fc_adm_datos_adicionales_disminucion_amortizaciones,
    NULL AS fc_adm_datos_adicionales_altas_inversiones_permanentes,
    NULL AS fc_adm_datos_adicionales_bajas_inversiones_permanentes,
    NULL AS fc_adm_datos_adicionales_dividendos_cobrados_soc_vinculadas,
    NULL AS fc_adm_datos_adicionales_altas_intangibles_valor_llave_marcas,
    NULL AS fc_adm_datos_adicionales_altas_intangibles_gastos_reorganizacion_const,
    NULL AS fc_adm_datos_adicionales_bajas_inversiones,
    NULL AS fc_adm_datos_adicionales_variacion_previsiones_pasivo,
    NULL AS fc_adm_datos_adicionales_aportes_capital,
    NULL AS fc_adm_datos_adicionales_constitucion_rrt,
    NULL AS fc_adm_datos_adicionales_desafectacion_rrt,
    NULL AS fc_adm_datos_adicionales_ajustes_resultado_anterior,
    NULL AS fc_adm_datos_adicionales_ajustes_ejercicio_anterior,
    NULL AS fc_adm_datos_adicionales_activos_moneda_extranjera,
    NULL AS fc_adm_datos_adicionales_pasivos_moneda_extranjera,
    NULL AS fc_adm_datos_adicionales_doc_descontados,
    NULL AS fc_adm_datos_adicionales_doc_endosados,
    NULL AS fc_adm_datos_adicionales_ajuste_ejercicio_anteriores_ajuste_inflacion,
    'CPP' AS ds_adm_balance_origen,
    from_unixtime(unix_timestamp()) AS dt_adm_fecha_hora_actual,
    date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}','yyyyMM') AS ds_adm_periodo,
    NULL AS dt_adm_fecha_inicio_balance,
    NULL AS cod_adm_tipo_balance,
    RANK() OVER(PARTITION BY CPP_BLC.PENUMPER ORDER BY COALESCE(CPP_BLC.FEC_BALANCE, date_format('17530101','YYYYMMDD')) DESC, CPP_BLC.ID_BALANCE DESC) AS int_adm_orden,
    NULL AS ds_adm_periodo_balance,
    RANK() OVER(PARTITION BY CPP_BLC.PENUMPER ORDER BY CPP_BLC.PENUMPER DESC, CPP_BLC.ID_BALANCE DESC) AS int_adm_orden_periodo_balance
FROM bi_corp_staging.sge_balances_per CPP_BLC
WHERE ((CPP_BLC.pefecalt < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1))
        OR (CPP_BLC.pefecalt IS NULL AND CPP_BLC.FEC_BALANCE < add_months(date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}', 'yyyy-MM-dd'), 1))
        ) AND CPP_BLC.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';