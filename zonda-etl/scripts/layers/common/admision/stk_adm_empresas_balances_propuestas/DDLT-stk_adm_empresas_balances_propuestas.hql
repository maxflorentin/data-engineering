CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_balances_propuestas (
    id_adm_balance bigint,
    dt_adm_fecha_balance string,
    cod_adm_numero_persona_sge bigint,
    flag_adm_consolidado string,
    int_adm_duracion int,
    ds_adm_segmento string,
    fc_adm_activo_cp_disponibles_caja_bancos decimal(20, 2),
    fc_adm_activo_cp_disponibles_valores_depositar decimal(20, 2),
    fc_adm_activo_cp_disponibles_inversiones_corto_plazo decimal(20, 2),
    fc_adm_activo_cp_disponibles_total decimal(20, 2),
    fc_adm_activo_cp_creditos_ventas_dsventas_doccobrar decimal(20, 2),
    fc_adm_activo_cp_creditos_ventas_deudores_gestion_cheques_rechazados decimal(20, 2),
    fc_adm_activo_cp_creditos_ventas_sociedades_vinculadas decimal(20, 2),
    fc_adm_activo_cp_creditos_ventas_prevision_ds_incobrables decimal(20, 2),
    fc_adm_activo_cp_creditos_ventas_total decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_creditos_fiscales decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_gastos_pagados_adelantado decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_sociedades_vinculadas decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_anticipados decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_aprobados_anticipados decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_aportes_pendientes_integracion_cuentas_socios decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_dividendos_cobrar decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_diversos decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_prevision_otros_creditos decimal(20, 2),
    fc_adm_activo_cp_otras_cuentas_total decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_productos_terminados_merc_reventa decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_productos_proceso decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_materias_primas decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_otros_insumos decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_anticipos_proveedores decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_prevision_desvalorizaciones decimal(20, 2),
    fc_adm_activo_cp_bienes_cambio_total decimal(20, 2),
    fc_adm_activo_cp_otros decimal(20, 2),
    fc_adm_activo_cp_total decimal(20, 2),
    fc_adm_activo_lp_creditos_ventas_dsventas_doccobrar decimal(20, 2),
    fc_adm_activo_lp_creditos_ventas_deudores_gestion_cheques_rechazados decimal(20, 2),
    fc_adm_activo_lp_creditos_ventas_sociedades_vinculadas decimal(20, 2),
    fc_adm_activo_lp_creditos_ventas_prevision_ds_incobrables decimal(20, 2),
    fc_adm_activo_lp_creditos_venta_total decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_fiscales decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_sociales_vinculadas decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_gastos_pagados_adelantado decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_cuentas_particulares_accionistas decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_diversos decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_otros decimal(20, 2),
    fc_adm_activo_lp_otros_creditos_total decimal(20, 2),
    fc_adm_activo_lp_inversiones_permanentes_diversas decimal(20, 2),
    fc_adm_activo_lp_inversiones_permanentes_soc decimal(20, 2),
    fc_adm_activo_lp_inversiones_permanentes_total decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_terrenos decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_edificios_mejoras decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_maquinas_equipos_herramientas decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_m_utiles_instalaciones_rodados decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_otros decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_obras_curso decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_anticipo_proveedores decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_amortizacion_acumulada decimal(20, 2),
    fc_adm_activo_lp_bienes_uso_total decimal(20, 2),
    fc_adm_activo_lp_bienes_intangibles_marcas_patentes decimal(20, 2),
    fc_adm_activo_lp_bienes_intangibles_reorganizacion_empresa decimal(20, 2),
    fc_adm_activo_lp_bienes_intangibles_valor_llave decimal(20, 2),
    fc_adm_activo_lp_bienes_intangibles_amort_acumulada decimal(20, 2),
    fc_adm_activo_lp_bienes_intangibles_total decimal(20, 2),
    fc_adm_activo_lp_bienes_cambio decimal(20, 2),
    fc_adm_activo_lp_otros decimal(20, 2),
    fc_adm_activo_lp_total decimal(20, 2),
    fc_adm_activo_total decimal(20, 2),
    fc_adm_pasivo_cp_deuda_bancaria_financiera_costo_credito_importacion decimal(20, 2),
    fc_adm_pasivo_cp_deuda_bancaria_financiera_deuda decimal(20, 2),
    fc_adm_pasivo_cp_deuda_bancaria_financiera_obligaciones_negociables decimal(20, 2),
    fc_adm_pasivo_cp_deuda_bancaria_financiera_porcion_deuda_lp decimal(20, 2),
    fc_adm_pasivo_cp_deuda_bancaria_financiera_total decimal(20, 2),
    fc_adm_pasivo_cp_importe_proveedores decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_art33_comercial decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_art33_financiera decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_art33_otras decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_art33_total decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_fiscales_moratorias decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_fiscales_diferimiento_impositivo decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_fiscales_otras_ds decimal(20, 2),
    fc_adm_pasivo_cp_deuda_sociales_fiscales_total decimal(20, 2),
    fc_adm_pasivo_cp_otros_anticipos_clientes decimal(20, 2),
    fc_adm_pasivo_cp_dividendos_honorarios_pagar decimal(20, 2),
    fc_adm_pasivo_cp_previsiones decimal(20, 2),
    fc_adm_pasivo_cp_total decimal(20, 2),
    fc_adm_pasivo_lp_deuda_bancaria_financiera_credito_importacion decimal(20, 2),
    fc_adm_pasivo_lp_deuda_bancaria_financiera_deuda_financiera decimal(20, 2),
    fc_adm_pasivo_lp_deuda_bancaria_financiera_obligaciones_negociables decimal(20, 2),
    fc_adm_pasivo_lp_deuda_bancaria_financiera_proveedores decimal(20, 2),
    fc_adm_pasivo_lp_deuda_bancaria_financiera_total decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_articulo33_comercial decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_articulo33_financiera decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_articulo33_otras decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_articulo33_total decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_fiscales_moratorias decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_fiscales_diferimiento_impositivo decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_fiscales_otros_ds decimal(20, 2),
    fc_adm_pasivo_lp_deuda_sociales_fiscales_total decimal(20, 2),
    fc_adm_pasivo_lp_otros decimal(20, 2),
    fc_adm_pasivo_lp_previsiones decimal(20, 2),
    fc_adm_pasivo_lp_participacion_minoritaria decimal(20, 2),
    fc_adm_pasivo_lp_total decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_capital decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_reserva_revaluo_tecnico decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_reservas_resultados decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_resultados_no_asignados decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_resultado_ejercicio decimal(20, 2),
    fc_adm_pasivo_patrimonio_neto_total decimal(20, 2),
    fc_adm_estado_resultado_bruto_ventas_netas decimal(20, 2),
    fc_adm_estado_resultado_bruto_costo_ventas decimal(20, 2),
    fc_adm_estado_resultado_bruto_total decimal(20, 2),
    fc_adm_estado_resultado_operativo_gastos_administrativos decimal(20, 2),
    fc_adm_estado_resultado_operativo_gastos_comerciales decimal(20, 2),
    fc_adm_estado_resultado_operativo_otros decimal(20, 2),
    fc_adm_estado_resultado_operativo_ingresos_promocion_industrial decimal(20, 2),
    fc_adm_estado_resultado_operativo_total decimal(20, 2),
    fc_adm_estado_resultado_inversiones_permanentes decimal(20, 2),
    fc_adm_estado_resultado_reintegros_exportaciones decimal(20, 2),
    fc_adm_estado_resultado_otros_ingresos decimal(20, 2),
    fc_adm_estado_resultado_otros_egresos decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_intereses decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_rei decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_diferencias_cambio decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_tenencia decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_otros decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_activos_total decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_intereses decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_rei decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_diferencias_cambio decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_tenencia decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_otros decimal(20, 2),
    fc_adm_estado_resultado_financiero_generado_pasivos_total decimal(20, 2),
    fc_adm_estado_resultado_financiero_total decimal(20, 2),
    fc_adm_estado_resultado_operaciones_ordinarias_total decimal(20, 2),
    fc_adm_estado_resultado_antes_impuestos_ganancias_extraordinarias decimal(20, 2),
    fc_adm_estado_resultado_antes_impuestos_perdidas_extraordinarias decimal(20, 2),
    fc_adm_estado_resultado_antes_impuestos_participacion_minoritaria decimal(20, 2),
    fc_adm_estado_resultado_antes_impuestos_total decimal(20, 2),
    fc_adm_estado_resultado_impuesto_ganancias decimal(20, 2),
    fc_adm_estado_resultado_neto_total decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_costo_produccion decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_gastos_administrativos decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_gastos_com decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_otros_gastos_operativos decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_otros_egresos decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_perdidas_extraordinarias decimal(20, 2),
    fc_adm_datos_adicionales_despreciaciones_total decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_costo_produccion decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_gastos_administrativos decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_gastos_com decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_otros_gastos_operativos decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_otros_ingresos decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_ganancias_extraordinarias decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_total decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_costo_produccion decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_administrativos decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_com decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_otros_gastos_operativos decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_otros_egresos decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_intangibles_perdidas_extraordinarias decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_inversiones_otros_egresos decimal(20, 2),
    fc_adm_datos_adicionales_amortizaciones_total decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_costo_produccion decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_gastos_administrativos decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_gastos_com decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_otros_gastos_operativos decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_otros_egresos decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_perdidas_extraordinarias decimal(20, 2),
    fc_adm_datos_adicionales_previsiones_total decimal(20, 2),
    fc_adm_datos_adicionales_honorarios_directores decimal(20, 2),
    dt_adm_datos_adicionales_distribucion_utilidades_fecha_asamblea_aprobatoria string,
    fc_adm_datos_adicionales_distribucion_utilidades_dividendos_honorarios_aprobados decimal(20, 2),
    fc_adm_datos_adicionales_distribucion_utilidades_gratificaciones_personal decimal(20, 2),
    fc_adm_datos_adicionales_distribucion_utilidades_provision_impuestos decimal(20, 2),
    fc_adm_datos_adicionales_compras decimal(20, 2),
    fc_adm_datos_adicionales_gatos_produccion decimal(20, 2),
    fc_adm_datos_adicionales_altas_bienes_uso decimal(20, 2),
    fc_adm_datos_adicionales_bajas_bienes_uso decimal(20, 2),
    fc_adm_datos_adicionales_disminucion_amortizaciones decimal(20, 2),
    fc_adm_datos_adicionales_altas_inversiones_permanentes decimal(20, 2),
    fc_adm_datos_adicionales_bajas_inversiones_permanentes decimal(20, 2),
    fc_adm_datos_adicionales_dividendos_cobrados_soc_vinculadas decimal(20, 2),
    fc_adm_datos_adicionales_altas_intangibles_valor_llave_marcas decimal(20, 2),
    fc_adm_datos_adicionales_altas_intangibles_gastos_reorganizacion_const decimal(20, 2),
    fc_adm_datos_adicionales_bajas_inversiones decimal(20, 2),
    fc_adm_datos_adicionales_variacion_previsiones_pasivo decimal(20, 2),
    fc_adm_datos_adicionales_aportes_capital decimal(20, 2),
    fc_adm_datos_adicionales_constitucion_rrt decimal(20, 2),
    fc_adm_datos_adicionales_desafectacion_rrt decimal(20, 2),
    fc_adm_datos_adicionales_ajustes_resultado_anterior decimal(20, 2),
    fc_adm_datos_adicionales_ajustes_ejercicio_anterior decimal(20, 2),
    fc_adm_datos_adicionales_activos_moneda_extranjera decimal(20, 2),
    fc_adm_datos_adicionales_pasivos_moneda_extranjera decimal(20, 2),
    fc_adm_datos_adicionales_doc_descontados decimal(20, 2),
    fc_adm_datos_adicionales_doc_endosados decimal(20, 2),
    fc_adm_datos_adicionales_ajuste_ejercicio_anteriores_ajuste_inflacion decimal(20, 2),
    ds_adm_balance_origen string,
    dt_adm_fecha_hora_actual string,
    ds_adm_periodo string,
    dt_adm_fecha_inicio_balance string,
    cod_adm_numero_propuesta int,
    cod_adm_tipo_balance string,
    int_adm_orden int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_balances_propuestas';