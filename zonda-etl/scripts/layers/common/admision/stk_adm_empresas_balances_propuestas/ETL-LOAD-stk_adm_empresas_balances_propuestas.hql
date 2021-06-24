set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS balances;
CREATE TEMPORARY TABLE balances AS
    select
        id_blc as id_balance,
        'SUPERVISION' as origen,
        nro_prop as numero_propuesta
    from bi_corp_staging.sge_blc_act_prop
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

    UNION ALL

    select
        id_balance,
        'CPP' as origen,
        nro_prop as numero_propuesta
    from bi_corp_staging.sge_balances_prop
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'

    UNION ALL

    select
        id_balance,
        'ESTANDARIZADAS' as origen,
        nro_prop as numero_propuesta
    from bi_corp_staging.sge_stnd_balances
    where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';


INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_balances_propuestas
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(COLUMNA_0 as bigint) AS id_adm_balance,
    COLUMNA_1 AS dt_adm_fecha_balance,
    cast(COLUMNA_2 as bigint) AS cod_adm_numero_persona_sge,
    COLUMNA_3 AS flag_adm_consolidado,
    cast(COLUMNA_4 as int) AS int_adm_duracion,
    COLUMNA_5 AS ds_adm_segmento,
    cast(COLUMNA_6 as decimal(20, 2)) as fc_adm_activo_cp_disponibles_caja_bancos,
    cast(COLUMNA_7 as decimal(20, 2)) as fc_adm_activo_cp_disponibles_valores_depositar,
    cast(COLUMNA_8 as decimal(20, 2)) as fc_adm_activo_cp_disponibles_inversiones_corto_plazo,
    cast(COLUMNA_9 as decimal(20, 2)) as fc_adm_activo_cp_disponibles_total,
    cast(COLUMNA_10 as decimal(20, 2)) as fc_adm_activo_cp_creditos_ventas_dsventas_doccobrar,
    cast(COLUMNA_11 as decimal(20, 2)) as fc_adm_activo_cp_creditos_ventas_deudores_gestion_cheques_rechazados,
    cast(COLUMNA_12 as decimal(20, 2)) as fc_adm_activo_cp_creditos_ventas_sociedades_vinculadas,
    cast(COLUMNA_13 as decimal(20, 2)) as fc_adm_activo_cp_creditos_ventas_prevision_ds_incobrables,
    cast(COLUMNA_14 as decimal(20, 2)) as fc_adm_activo_cp_creditos_ventas_total,
    cast(COLUMNA_15 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_creditos_fiscales,
    cast(COLUMNA_16 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_gastos_pagados_adelantado,
    cast(COLUMNA_17 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_sociedades_vinculadas,
    cast(COLUMNA_18 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_anticipados,
    cast(COLUMNA_19 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_honorarios_dividendos_aprobados_anticipados,
    cast(COLUMNA_20 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_aportes_pendientes_integracion_cuentas_socios,
    cast(COLUMNA_21 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_dividendos_cobrar,
    cast(COLUMNA_22 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_diversos,
    cast(COLUMNA_23 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_prevision_otros_creditos,
    cast(COLUMNA_24 as decimal(20, 2)) as fc_adm_activo_cp_otras_cuentas_total,
    cast(COLUMNA_25 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_productos_terminados_merc_reventa,
    cast(COLUMNA_26 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_productos_proceso,
    cast(COLUMNA_27 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_materias_primas,
    cast(COLUMNA_28 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_otros_insumos,
    cast(COLUMNA_29 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_anticipos_proveedores,
    cast(COLUMNA_30 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_prevision_desvalorizaciones,
    cast(COLUMNA_31 as decimal(20, 2)) as fc_adm_activo_cp_bienes_cambio_total,
    cast(COLUMNA_32 as decimal(20, 2)) as fc_adm_activo_cp_otros,
    cast(COLUMNA_33 as decimal(20, 2)) as fc_adm_activo_cp_total,
    cast(COLUMNA_34 as decimal(20, 2)) as fc_adm_activo_lp_creditos_ventas_dsventas_doccobrar,
    cast(COLUMNA_35 as decimal(20, 2)) as fc_adm_activo_lp_creditos_ventas_deudores_gestion_cheques_rechazados,
    cast(COLUMNA_36 as decimal(20, 2)) as fc_adm_activo_lp_creditos_ventas_sociedades_vinculadas,
    cast(COLUMNA_37 as decimal(20, 2)) as fc_adm_activo_lp_creditos_ventas_prevision_ds_incobrables,
    cast(COLUMNA_38 as decimal(20, 2)) as fc_adm_activo_lp_creditos_venta_total,
    cast(COLUMNA_39 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_fiscales,
    cast(COLUMNA_40 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_sociales_vinculadas,
    cast(COLUMNA_41 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_gastos_pagados_adelantado,
    cast(COLUMNA_42 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_cuentas_particulares_accionistas,
    cast(COLUMNA_43 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_diversos,
    cast(COLUMNA_44 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_otros,
    cast(COLUMNA_45 as decimal(20, 2)) as fc_adm_activo_lp_otros_creditos_total,
    cast(COLUMNA_46 as decimal(20, 2)) as fc_adm_activo_lp_inversiones_permanentes_diversas,
    cast(COLUMNA_47 as decimal(20, 2)) as fc_adm_activo_lp_inversiones_permanentes_soc,
    cast(COLUMNA_48 as decimal(20, 2)) as fc_adm_activo_lp_inversiones_permanentes_total,
    cast(COLUMNA_49 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_terrenos,
    cast(COLUMNA_50 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_edificios_mejoras,
    cast(COLUMNA_51 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_maquinas_equipos_herramientas,
    cast(COLUMNA_52 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_m_utiles_instalaciones_rodados,
    cast(COLUMNA_53 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_otros,
    cast(COLUMNA_54 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_obras_curso,
    cast(COLUMNA_55 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_anticipo_proveedores,
    cast(COLUMNA_56 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_amortizacion_acumulada,
    cast(COLUMNA_57 as decimal(20, 2)) as fc_adm_activo_lp_bienes_uso_total,
    cast(COLUMNA_58 as decimal(20, 2)) as fc_adm_activo_lp_bienes_intangibles_marcas_patentes,
    cast(COLUMNA_59 as decimal(20, 2)) as fc_adm_activo_lp_bienes_intangibles_reorganizacion_empresa,
    cast(COLUMNA_60 as decimal(20, 2)) as fc_adm_activo_lp_bienes_intangibles_valor_llave,
    cast(COLUMNA_61 as decimal(20, 2)) as fc_adm_activo_lp_bienes_intangibles_amort_acumulada,
    cast(COLUMNA_62 as decimal(20, 2)) as fc_adm_activo_lp_bienes_intangibles_total,
    cast(COLUMNA_63 as decimal(20, 2)) as fc_adm_activo_lp_bienes_cambio,
    cast(COLUMNA_64 as decimal(20, 2)) as fc_adm_activo_lp_otros,
    cast(COLUMNA_65 as decimal(20, 2)) as fc_adm_activo_lp_total,
    cast(COLUMNA_66 as decimal(20, 2)) as fc_adm_activo_total,
    cast(COLUMNA_67 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_bancaria_financiera_costo_credito_importacion,
    cast(COLUMNA_68 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_bancaria_financiera_deuda,
    cast(COLUMNA_69 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_bancaria_financiera_obligaciones_negociables,
    cast(COLUMNA_70 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_bancaria_financiera_porcion_deuda_lp,
    cast(COLUMNA_71 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_bancaria_financiera_total,
    cast(COLUMNA_72 as decimal(20, 2)) as fc_adm_pasivo_cp_importe_proveedores,
    cast(COLUMNA_73 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_art33_comercial,
    cast(COLUMNA_74 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_art33_financiera,
    cast(COLUMNA_75 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_art33_otras,
    cast(COLUMNA_76 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_art33_total,
    cast(COLUMNA_77 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_fiscales_moratorias,
    cast(COLUMNA_78 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_fiscales_diferimiento_impositivo,
    cast(COLUMNA_79 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_fiscales_otras_ds,
    cast(COLUMNA_80 as decimal(20, 2)) as fc_adm_pasivo_cp_deuda_sociales_fiscales_total,
    cast(COLUMNA_81 as decimal(20, 2)) as fc_adm_pasivo_cp_otros_anticipos_clientes,
    cast(COLUMNA_82 as decimal(20, 2)) as fc_adm_pasivo_cp_dividendos_honorarios_pagar,
    cast(COLUMNA_83 as decimal(20, 2)) as fc_adm_pasivo_cp_previsiones,
    cast(COLUMNA_84 as decimal(20, 2)) as fc_adm_pasivo_cp_total,
    cast(COLUMNA_85 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_bancaria_financiera_credito_importacion,
    cast(COLUMNA_86 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_bancaria_financiera_deuda_financiera,
    cast(COLUMNA_87 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_bancaria_financiera_obligaciones_negociables,
    cast(COLUMNA_88 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_bancaria_financiera_proveedores,
    cast(COLUMNA_89 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_bancaria_financiera_total,
    cast(COLUMNA_90 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_articulo33_comercial,
    cast(COLUMNA_91 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_articulo33_financiera,
    cast(COLUMNA_92 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_articulo33_otras,
    cast(COLUMNA_93 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_articulo33_total,
    cast(COLUMNA_94 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_fiscales_moratorias,
    cast(COLUMNA_95 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_fiscales_diferimiento_impositivo,
    cast(COLUMNA_96 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_fiscales_otros_ds,
    cast(COLUMNA_97 as decimal(20, 2)) as fc_adm_pasivo_lp_deuda_sociales_fiscales_total,
    cast(COLUMNA_98 as decimal(20, 2)) as fc_adm_pasivo_lp_otros,
    cast(COLUMNA_99 as decimal(20, 2)) as fc_adm_pasivo_lp_previsiones,
    cast(COLUMNA_100 as decimal(20, 2)) as fc_adm_pasivo_lp_participacion_minoritaria,
    cast(COLUMNA_101 as decimal(20, 2)) as fc_adm_pasivo_lp_total,
    cast(COLUMNA_102 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_capital,
    cast(COLUMNA_103 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_reserva_revaluo_tecnico,
    cast(COLUMNA_104 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_reservas_resultados,
    cast(COLUMNA_105 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_resultados_no_asignados,
    cast(COLUMNA_106 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_resultado_ejercicio,
    cast(COLUMNA_107 as decimal(20, 2)) as fc_adm_pasivo_patrimonio_neto_total,
    cast(COLUMNA_108 as decimal(20, 2)) as fc_adm_estado_resultado_bruto_ventas_netas,
    cast(COLUMNA_109 as decimal(20, 2)) as fc_adm_estado_resultado_bruto_costo_ventas,
    cast(COLUMNA_110 as decimal(20, 2)) as fc_adm_estado_resultado_bruto_total,
    cast(COLUMNA_111 as decimal(20, 2)) as fc_adm_estado_resultado_operativo_gastos_administrativos,
    cast(COLUMNA_112 as decimal(20, 2)) as fc_adm_estado_resultado_operativo_gastos_comerciales,
    cast(COLUMNA_113 as decimal(20, 2)) as fc_adm_estado_resultado_operativo_otros,
    cast(COLUMNA_114 as decimal(20, 2)) as fc_adm_estado_resultado_operativo_ingresos_promocion_industrial,
    cast(COLUMNA_115 as decimal(20, 2)) as fc_adm_estado_resultado_operativo_total,
    cast(COLUMNA_116 as decimal(20, 2)) as fc_adm_estado_resultado_inversiones_permanentes,
    cast(COLUMNA_117 as decimal(20, 2)) as fc_adm_estado_resultado_reintegros_exportaciones,
    cast(COLUMNA_118 as decimal(20, 2)) as fc_adm_estado_resultado_otros_ingresos,
    cast(COLUMNA_119 as decimal(20, 2)) as fc_adm_estado_resultado_otros_egresos,
    cast(COLUMNA_120 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_intereses,
    cast(COLUMNA_121 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_rei,
    cast(COLUMNA_122 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_diferencias_cambio,
    cast(COLUMNA_123 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_tenencia,
    cast(COLUMNA_124 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_otros,
    cast(COLUMNA_125 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_activos_total,
    cast(COLUMNA_126 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_intereses,
    cast(COLUMNA_127 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_rei,
    cast(COLUMNA_128 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_diferencias_cambio,
    cast(COLUMNA_129 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_tenencia,
    cast(COLUMNA_130 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_otros,
    cast(COLUMNA_131 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_generado_pasivos_total,
    cast(COLUMNA_132 as decimal(20, 2)) as fc_adm_estado_resultado_financiero_total,
    cast(COLUMNA_133 as decimal(20, 2)) as fc_adm_estado_resultado_operaciones_ordinarias_total,
    cast(COLUMNA_134 as decimal(20, 2)) as fc_adm_estado_resultado_antes_impuestos_ganancias_extraordinarias,
    cast(COLUMNA_135 as decimal(20, 2)) as fc_adm_estado_resultado_antes_impuestos_perdidas_extraordinarias,
    cast(COLUMNA_136 as decimal(20, 2)) as fc_adm_estado_resultado_antes_impuestos_participacion_minoritaria,
    cast(COLUMNA_137 as decimal(20, 2)) as fc_adm_estado_resultado_antes_impuestos_total,
    cast(COLUMNA_138 as decimal(20, 2)) as fc_adm_estado_resultado_impuesto_ganancias,
    cast(COLUMNA_139 as decimal(20, 2)) as fc_adm_estado_resultado_neto_total,
    cast(COLUMNA_140 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_costo_produccion,
    cast(COLUMNA_141 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_gastos_administrativos,
    cast(COLUMNA_142 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_gastos_com,
    cast(COLUMNA_143 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_otros_gastos_operativos,
    cast(COLUMNA_144 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_otros_egresos,
    cast(COLUMNA_145 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_perdidas_extraordinarias,
    cast(COLUMNA_146 as decimal(20, 2)) as fc_adm_datos_adicionales_despreciaciones_total,
    cast(COLUMNA_147 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_costo_produccion,
    cast(COLUMNA_148 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_gastos_administrativos,
    cast(COLUMNA_149 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_gastos_com,
    cast(COLUMNA_150 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_otros_gastos_operativos,
    cast(COLUMNA_151 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_otros_ingresos,
    cast(COLUMNA_152 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_ganancias_extraordinarias,
    cast(COLUMNA_153 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_total,
    cast(COLUMNA_154 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_costo_produccion,
    cast(COLUMNA_155 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_administrativos,
    cast(COLUMNA_156 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_gastos_com,
    cast(COLUMNA_157 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_otros_gastos_operativos,
    cast(COLUMNA_158 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_otros_egresos,
    cast(COLUMNA_159 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_intangibles_perdidas_extraordinarias,
    cast(COLUMNA_160 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_inversiones_otros_egresos,
    cast(COLUMNA_161 as decimal(20, 2)) as fc_adm_datos_adicionales_amortizaciones_total,
    cast(COLUMNA_162 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_costo_produccion,
    cast(COLUMNA_163 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_gastos_administrativos,
    cast(COLUMNA_164 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_gastos_com,
    cast(COLUMNA_165 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_otros_gastos_operativos,
    cast(COLUMNA_166 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_otros_egresos,
    cast(COLUMNA_167 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_perdidas_extraordinarias,
    cast(COLUMNA_168 as decimal(20, 2)) as fc_adm_datos_adicionales_previsiones_total,
    cast(COLUMNA_169 as decimal(20, 2)) as fc_adm_datos_adicionales_honorarios_directores,
    COLUMNA_170 AS dt_adm_datos_adicionales_distribucion_utilidades_fecha_asamblea_aprobatoria,
    cast(COLUMNA_171 as decimal(20, 2)) as fc_adm_datos_adicionales_distribucion_utilidades_dividendos_honorarios_aprobados,
    cast(COLUMNA_172 as decimal(20, 2)) as fc_adm_datos_adicionales_distribucion_utilidades_gratificaciones_personal,
    cast(COLUMNA_173 as decimal(20, 2)) as fc_adm_datos_adicionales_distribucion_utilidades_provision_impuestos,
    cast(COLUMNA_174 as decimal(20, 2)) as fc_adm_datos_adicionales_compras,
    cast(COLUMNA_175 as decimal(20, 2)) as fc_adm_datos_adicionales_gatos_produccion,
    cast(COLUMNA_176 as decimal(20, 2)) as fc_adm_datos_adicionales_altas_bienes_uso,
    cast(COLUMNA_177 as decimal(20, 2)) as fc_adm_datos_adicionales_bajas_bienes_uso,
    cast(COLUMNA_178 as decimal(20, 2)) as fc_adm_datos_adicionales_disminucion_amortizaciones,
    cast(COLUMNA_179 as decimal(20, 2)) as fc_adm_datos_adicionales_altas_inversiones_permanentes,
    cast(COLUMNA_180 as decimal(20, 2)) as fc_adm_datos_adicionales_bajas_inversiones_permanentes,
    cast(COLUMNA_181 as decimal(20, 2)) as fc_adm_datos_adicionales_dividendos_cobrados_soc_vinculadas,
    cast(COLUMNA_182 as decimal(20, 2)) as fc_adm_datos_adicionales_altas_intangibles_valor_llave_marcas,
    cast(COLUMNA_183 as decimal(20, 2)) as fc_adm_datos_adicionales_altas_intangibles_gastos_reorganizacion_const,
    cast(COLUMNA_184 as decimal(20, 2)) as fc_adm_datos_adicionales_bajas_inversiones,
    cast(COLUMNA_185 as decimal(20, 2)) as fc_adm_datos_adicionales_variacion_previsiones_pasivo,
    cast(COLUMNA_186 as decimal(20, 2)) as fc_adm_datos_adicionales_aportes_capital,
    cast(COLUMNA_187 as decimal(20, 2)) as fc_adm_datos_adicionales_constitucion_rrt,
    cast(COLUMNA_188 as decimal(20, 2)) as fc_adm_datos_adicionales_desafectacion_rrt,
    cast(COLUMNA_189 as decimal(20, 2)) as fc_adm_datos_adicionales_ajustes_resultado_anterior,
    cast(COLUMNA_190 as decimal(20, 2)) as fc_adm_datos_adicionales_ajustes_ejercicio_anterior,
    cast(COLUMNA_191 as decimal(20, 2)) as fc_adm_datos_adicionales_activos_moneda_extranjera,
    cast(COLUMNA_192 as decimal(20, 2)) as fc_adm_datos_adicionales_pasivos_moneda_extranjera,
    cast(COLUMNA_193 as decimal(20, 2)) as fc_adm_datos_adicionales_doc_descontados,
    cast(COLUMNA_194 as decimal(20, 2)) as fc_adm_datos_adicionales_doc_endosados,
    cast(COLUMNA_195 as decimal(20, 2)) as fc_adm_datos_adicionales_ajuste_ejercicio_anteriores_ajuste_inflacion,
    COLUMNA_196 AS ds_adm_balance_origen,
    from_unixtime(unix_timestamp()) AS dt_adm_fecha_hora_actual,
    date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}','yyyyMM') AS ds_adm_periodo,
    COLUMNA_197 AS dt_adm_fecha_inicio_balance,
    cast(COLUMNA_199 as int) AS cod_adm_numero_propuesta,
    COLUMNA_198 AS cod_adm_tipo_balance,
    RANK() OVER(PARTITION BY COLUMNA_199 ORDER BY COLUMNA_1 DESC, COLUMNA_0 DESC) AS int_adm_orden
FROM (
    -- estandarizadas
    SELECT
        BLC_STND.ID_BALANCE AS COLUMNA_0,
        BLC_STND.FEC_BALANCE AS COLUMNA_1,
        BLC_STND.PENUMPER AS COLUMNA_2,
        'N' AS COLUMNA_3,
        BLC_STND.MESES_ABARCA AS COLUMNA_4,
        'ESTANDARIZADAS' AS COLUMNA_5,
        BLC_STND.DISPONIBILIDADES AS COLUMNA_6,
        NULL AS COLUMNA_7,
        NULL AS COLUMNA_8,
        NULL AS COLUMNA_9,
        NULL AS COLUMNA_10,
        NULL AS COLUMNA_11,
        NULL AS COLUMNA_12,
        NULL AS COLUMNA_13,
        BLC_STND.IMP_ACT_CREDITO_COM AS COLUMNA_14,
        NULL AS COLUMNA_15,
        NULL AS COLUMNA_16,
        NULL AS COLUMNA_17,
        BLC_STND.IMP_ANTIC_ACCIONISTAS AS COLUMNA_18,
        NULL AS COLUMNA_19,
        NULL AS COLUMNA_20,
        NULL AS COLUMNA_21,
        NULL AS COLUMNA_22,
        NULL AS COLUMNA_23,
        NULL AS COLUMNA_24,
        NULL AS COLUMNA_25,
        NULL AS COLUMNA_26,
        NULL AS COLUMNA_27,
        NULL AS COLUMNA_28,
        NULL AS COLUMNA_29,
        NULL AS COLUMNA_30,
        BLC_STND.IMP_ACT_BIENES_CAMBIO AS COLUMNA_31,
        NULL AS COLUMNA_32,
        BLC_STND.IMP_ACT_CORRIENTE AS COLUMNA_33,
        NULL AS COLUMNA_34,
        NULL AS COLUMNA_35,
        NULL AS COLUMNA_36,
        NULL AS COLUMNA_37,
        NULL AS COLUMNA_38,
        NULL AS COLUMNA_39,
        NULL AS COLUMNA_40,
        NULL AS COLUMNA_41,
        NULL AS COLUMNA_42,
        NULL AS COLUMNA_43,
        NULL AS COLUMNA_44,
        NULL AS COLUMNA_45,
        NULL AS COLUMNA_46,
        NULL AS COLUMNA_47,
        NULL AS COLUMNA_48,
        NULL AS COLUMNA_49,
        NULL AS COLUMNA_50,
        NULL AS COLUMNA_51,
        NULL AS COLUMNA_52,
        NULL AS COLUMNA_53,
        NULL AS COLUMNA_54,
        NULL AS COLUMNA_55,
        NULL AS COLUMNA_56,
        BLC_STND.IMP_ACT_BIENES_USO AS COLUMNA_57,
        NULL AS COLUMNA_58,
        NULL AS COLUMNA_59,
        NULL AS COLUMNA_60,
        NULL AS COLUMNA_61,
        NULL AS COLUMNA_62,
        NULL AS COLUMNA_63,
        NULL AS COLUMNA_64,
        NULL AS COLUMNA_65,
        COALESCE(BLC_STND.IMP_ACT_CORRIENTE, 0) AS COLUMNA_66,
        NULL AS COLUMNA_67,
        NULL AS COLUMNA_68,
        NULL AS COLUMNA_69,
        NULL AS COLUMNA_70,
        BLC_STND.IMP_PAS_DEUDA_FIN_CP AS COLUMNA_71,
        BLC_STND.IMP_PAS_DEUDA_COM_CP AS COLUMNA_72,
        NULL AS COLUMNA_73,
        NULL AS COLUMNA_74,
        NULL AS COLUMNA_75,
        NULL AS COLUMNA_76,
        NULL AS COLUMNA_77,
        NULL AS COLUMNA_78,
        NULL AS COLUMNA_79,
        BLC_STND.IMP_PAS_DEUDA_FIS_CP AS COLUMNA_80,
        NULL AS COLUMNA_81,
        NULL AS COLUMNA_82,
        NULL AS COLUMNA_83,
        BLC_STND.IMP_PAS_CORRIENTE AS COLUMNA_84,
        NULL AS COLUMNA_85,
        NULL AS COLUMNA_86,
        NULL AS COLUMNA_87,
        NULL AS COLUMNA_88,
        BLC_STND.IMP_PAS_DEUDA_FIN_LP AS COLUMNA_89,
        NULL AS COLUMNA_90,
        NULL AS COLUMNA_91,
        NULL AS COLUMNA_92,
        NULL AS COLUMNA_93,
        NULL AS COLUMNA_94,
        NULL AS COLUMNA_95,
        NULL AS COLUMNA_96,
        BLC_STND.IMP_PAS_DEUDA_FIS_LP AS COLUMNA_97,
        BLC_STND.IMP_PAS_OTROS_LP AS COLUMNA_98,
        NULL AS COLUMNA_99,
        NULL AS COLUMNA_100,
        COALESCE(BLC_STND.IMP_PAS_TOTAL, 0) - COALESCE(BLC_STND.IMP_PAS_CORRIENTE, 0) AS COLUMNA_101,
        NULL AS COLUMNA_102,
        NULL AS COLUMNA_103,
        NULL AS COLUMNA_104,
        NULL AS COLUMNA_105,
        NULL AS COLUMNA_106,
        BLC_STND.IMP_PAS_PATRIMONIO AS COLUMNA_107,
        BLC_STND.IMP_RES_VENTAS AS COLUMNA_108,
        BLC_STND.IMP_RES_COSTOS_VENTAS AS COLUMNA_109,
        NULL AS COLUMNA_110,
        BLC_STND.IMP_RES_GTOS_ADMIN AS COLUMNA_111,
        BLC_STND.IMP_RES_GTOS_COMER AS COLUMNA_112,
        NULL AS COLUMNA_113,
        NULL AS COLUMNA_114,
        BLC_STND.IMP_RES_OPERATIVO AS COLUMNA_115,
        NULL AS COLUMNA_116,
        NULL AS COLUMNA_117,
        NULL AS COLUMNA_118,
        NULL AS COLUMNA_119,
        NULL AS COLUMNA_120,
        NULL AS COLUMNA_121,
        NULL AS COLUMNA_122,
        NULL AS COLUMNA_123,
        NULL AS COLUMNA_124,
        NULL AS COLUMNA_125,
        NULL AS COLUMNA_126,
        NULL AS COLUMNA_127,
        NULL AS COLUMNA_128,
        NULL AS COLUMNA_129,
        NULL AS COLUMNA_130,
        NULL AS COLUMNA_131,
        BLC_STND.IMP_RES_GTOS_FINAN AS COLUMNA_132,
        NULL AS COLUMNA_133,
        if(BLC_STND.IMP_RES_EXTRAORDINARIO >= 0, BLC_STND.IMP_RES_EXTRAORDINARIO, 0) AS COLUMNA_134,
        if(BLC_STND.IMP_RES_EXTRAORDINARIO <= 0, BLC_STND.IMP_RES_EXTRAORDINARIO, 0) AS COLUMNA_135,
        NULL AS COLUMNA_136,
        NULL AS COLUMNA_137,
        BLC_STND.IMP_RES_IMP_GANANCIAS AS COLUMNA_138,
        BLC_STND.IMP_RES_FINAL AS COLUMNA_139,
        NULL AS COLUMNA_140,
        NULL AS COLUMNA_141,
        NULL AS COLUMNA_142,
        NULL AS COLUMNA_143,
        NULL AS COLUMNA_144,
        NULL AS COLUMNA_145,
        BLC_STND.IMP_AMORTIZACIONES AS COLUMNA_146,
        NULL AS COLUMNA_147,
        NULL AS COLUMNA_148,
        NULL AS COLUMNA_149,
        NULL AS COLUMNA_150,
        NULL AS COLUMNA_151,
        NULL AS COLUMNA_152,
        NULL AS COLUMNA_153,
        NULL AS COLUMNA_154,
        NULL AS COLUMNA_155,
        NULL AS COLUMNA_156,
        NULL AS COLUMNA_157,
        NULL AS COLUMNA_158,
        NULL AS COLUMNA_159,
        NULL AS COLUMNA_160,
        NULL AS COLUMNA_161,
        NULL AS COLUMNA_162,
        NULL AS COLUMNA_163,
        NULL AS COLUMNA_164,
        NULL AS COLUMNA_165,
        NULL AS COLUMNA_166,
        NULL AS COLUMNA_167,
        NULL AS COLUMNA_168,
        NULL AS COLUMNA_169,
        NULL AS COLUMNA_170,
        NULL AS COLUMNA_171,
        NULL AS COLUMNA_172,
        NULL AS COLUMNA_173,
        BLC_STND.IMP_COMPRAS AS COLUMNA_174,
        NULL AS COLUMNA_175,
        NULL AS COLUMNA_176,
        NULL AS COLUMNA_177,
        NULL AS COLUMNA_178,
        NULL AS COLUMNA_179,
        NULL AS COLUMNA_180,
        NULL AS COLUMNA_181,
        NULL AS COLUMNA_182,
        NULL AS COLUMNA_183,
        NULL AS COLUMNA_184,
        NULL AS COLUMNA_185,
        NULL AS COLUMNA_186,
        NULL AS COLUMNA_187,
        NULL AS COLUMNA_188,
        NULL AS COLUMNA_189,
        NULL AS COLUMNA_190,
        NULL AS COLUMNA_191,
        NULL AS COLUMNA_192,
        NULL AS COLUMNA_193,
        NULL AS COLUMNA_194,
        NULL AS COLUMNA_195,
        BLC.ORIGEN AS COLUMNA_196,
        NULL AS COLUMNA_197,
        NULL AS COLUMNA_198,
        BLC.NUMERO_PROPUESTA AS COLUMNA_199
    FROM balances BLC
    LEFT JOIN bi_corp_staging.sge_stnd_balances BLC_STND ON BLC_STND.id_balance = BLC.id_balance AND BLC.numero_propuesta = BLC_STND.nro_prop and BLC_STND.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    WHERE BLC.origen = 'ESTANDARIZADAS'

    UNION ALL

    -- supervision
    SELECT
        SUP_BLC_ACT.ID_BLC AS COLUMNA_0,
        SUP_BLC_ACT.FEC_BLC AS COLUMNA_1,
        SUP_BLC_ACT.PENUMPER AS COLUMNA_2,
        SUP_BLC_ACT.CONSOLIDADO AS COLUMNA_3,
        SUP_BLC_ACT.DURACION AS COLUMNA_4,
        'SUPERVISION' AS COLUMNA_5,
        SUP_BLC_ACT.IMP_CAJABANCOS AS COLUMNA_6,
        SUP_BLC_ACT.IMP_VALORES_DEPOSITAR AS COLUMNA_7,
        SUP_BLC_ACT.IMP_INVER_TRANS AS COLUMNA_8,
        COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0) AS COLUMNA_9,
        SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR AS COLUMNA_10,
        SUP_BLC_ACT.IMP_DEUGEST_CHRECH AS COLUMNA_11,
        SUP_BLC_ACT.IMP_CTASXCOB AS COLUMNA_12,
        SUP_BLC_ACT.IMP_PREVDS_INC AS COLUMNA_13,
        COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0) + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0) AS COLUMNA_14,
        SUP_BLC_ACT.IMP_CREFISC AS COLUMNA_15,
        SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR AS COLUMNA_16, SUP_BLC_ACT.IMP_OTRCRECOV AS COLUMNA_17,
        SUP_BLC_ACT.IMP_HONDIVANT AS COLUMNA_18,
        SUP_BLC_ACT.IMP_HONDIVAPRANT AS COLUMNA_19,
        SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC AS COLUMNA_20,
        SUP_BLC_ACT.IMP_DIVCOBRAR AS COLUMNA_21,
        SUP_BLC_ACT.IMP_DIVERSOS AS COLUMNA_22,
        SUP_BLC_ACT.IMP_PREV_OTR_CRED AS COLUMNA_23,
        COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) AS COLUMNA_24,
        SUP_BLC_ACT.IMP_PROD_TERM_MERC AS COLUMNA_25,
        SUP_BLC_ACT.IMP_PRODENPROS AS COLUMNA_26,
        SUP_BLC_ACT.IMP_MATPRIMAS AS COLUMNA_27,
        SUP_BLC_ACT.IMP_OTROS_INSUMOS AS COLUMNA_28,
        SUP_BLC_ACT.IMP_ANTPROVBSCBIO AS COLUMNA_29,
        SUP_BLC_ACT.IMP_PREV_DESV AS COLUMNA_30,
        COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0) + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0) + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) AS COLUMNA_31,
        SUP_BLC_ACT.IMP_OTRACTCTES AS COLUMNA_32,
        COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0) + COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0) + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0) + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0) + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0) + COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) + COALESCE(IMP_INVER_TRANS, 0) AS COLUMNA_33,
        SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR AS COLUMNA_34,
        SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR AS COLUMNA_35,
        SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS AS COLUMNA_36,
        SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR AS COLUMNA_37,
        COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) AS COLUMNA_38,
        SUP_BLC_ACT.IMP_CRED_FISC_NOCORR AS COLUMNA_39,
        SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR AS COLUMNA_40,
        SUP_BLC_ACT.IMP_GS_PAGXADEL AS COLUMNA_41,
        SUP_BLC_ACT.IMP_CTAS_PART_ACC AS COLUMNA_42,
        SUP_BLC_ACT.IMP_DIVERSOS_NOCORR AS COLUMNA_43,
        SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR AS COLUMNA_44,
        COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) AS COLUMNA_45,
        SUP_BLC_ACT.IMP_OTRASINV AS COLUMNA_46,
        SUP_BLC_ACT.IMP_INV_SOCCYV AS COLUMNA_47,
        COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0) + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0) AS COLUMNA_48,
        SUP_BLC_ACT.IMP_TERRENOS AS COLUMNA_49,
        SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS AS COLUMNA_50,
        SUP_BLC_ACT.IMP_MAQEQHERR AS COLUMNA_51,
        SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD AS COLUMNA_52,
        SUP_BLC_ACT.IMP_OTROS AS COLUMNA_53,
        SUP_BLC_ACT.IMP_OBRAS_CURSO AS COLUMNA_54,
        SUP_BLC_ACT.IMP_ANTPROVBSUSO AS COLUMNA_55,
        SUP_BLC_ACT.IMP_AMORT_ACUM AS COLUMNA_56,
        COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0) + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0) + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0) + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0) + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0) AS COLUMNA_57,
        SUP_BLC_ACT.IMP_MARCAS_PATENTES AS COLUMNA_58,
        SUP_BLC_ACT.IMP_GS_ORGYREEST AS COLUMNA_59,
        SUP_BLC_ACT.IMP_VALOR_LLAVE AS COLUMNA_60,
        SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT AS COLUMNA_61,
        COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0) AS COLUMNA_62,
        SUP_BLC_ACT.IMP_BS_CBIO_LP AS COLUMNA_63,
        SUP_BLC_ACT.IMP_OTRACTNOCTE AS COLUMNA_64,
        COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0) + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0) + COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0) + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0) + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0) + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0) + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0) + COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0) + COALESCE(IMP_BS_CBIO_LP, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRACTNOCTE, 0) AS COLUMNA_65,
        COALESCE(SUP_BLC_ACT.IMP_CAJABANCOS, 0) + COALESCE(SUP_BLC_ACT.IMP_VALORES_DEPOSITAR, 0) + COALESCE(SUP_BLC_ACT.IMP_PROD_TERM_MERC, 0) + COALESCE(SUP_BLC_ACT.IMP_PRODENPROS, 0) + COALESCE(SUP_BLC_ACT.IMP_MATPRIMAS, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS_INSUMOS, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0)
            + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSCBIO, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_DESV, 0) + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_DOCCOBRAR, 0)
            + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH, 0) + COALESCE(SUP_BLC_ACT.IMP_CTASXCOB, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC, 0)
            + COALESCE(SUP_BLC_ACT.IMP_CREFISC, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL_CORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRCRECOV, 0)
            + COALESCE(SUP_BLC_ACT.IMP_HONDIVANT, 0) + COALESCE(SUP_BLC_ACT.IMP_HONDIVAPRANT, 0) + COALESCE(SUP_BLC_ACT.IMP_APOR_PEND_INT_CTA_SOC, 0)
            + COALESCE(SUP_BLC_ACT.IMP_DIVCOBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED, 0) + COALESCE(IMP_INVER_TRANS, 0)
            + COALESCE(SUP_BLC_ACT.IMP_DSVENTAS_SERVDOC_COBRAR, 0) + COALESCE(SUP_BLC_ACT.IMP_DEUGEST_CHRECH_NOCORR, 0)
            + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_VTAS, 0) + COALESCE(SUP_BLC_ACT.IMP_PREVDS_INC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_CRED_FISC_NOCORR, 0)
            + COALESCE(SUP_BLC_ACT.IMP_CRED_SOCVINC_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_PAGXADEL, 0) + COALESCE(SUP_BLC_ACT.IMP_CTAS_PART_ACC, 0)
            + COALESCE(SUP_BLC_ACT.IMP_DIVERSOS_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_PREV_OTR_CRED_NOCORR, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRASINV, 0)
            + COALESCE(SUP_BLC_ACT.IMP_INV_SOCCYV, 0) + COALESCE(SUP_BLC_ACT.IMP_TERRENOS, 0) + COALESCE(SUP_BLC_ACT.IMP_EDIFICIOS_MEJORAS, 0)
            + COALESCE(SUP_BLC_ACT.IMP_MAQEQHERR, 0) + COALESCE(SUP_BLC_ACT.IMP_MUTILES_INSTALAC_ROD, 0) + COALESCE(SUP_BLC_ACT.IMP_OTROS, 0)
            + COALESCE(SUP_BLC_ACT.IMP_OBRAS_CURSO, 0) + COALESCE(SUP_BLC_ACT.IMP_ANTPROVBSUSO, 0) + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM, 0)
            + COALESCE(SUP_BLC_ACT.IMP_MARCAS_PATENTES, 0) + COALESCE(SUP_BLC_ACT.IMP_GS_ORGYREEST, 0) + COALESCE(SUP_BLC_ACT.IMP_VALOR_LLAVE, 0)
            + COALESCE(SUP_BLC_ACT.IMP_AMORT_ACUM_BSINT, 0) + COALESCE(IMP_BS_CBIO_LP, 0) + COALESCE(SUP_BLC_ACT.IMP_OTRACTNOCTE, 0) AS COLUMNA_66,
        SUP_BLC_PAS.IMP_CCRED_IMP AS COLUMNA_67,
        SUP_BLC_PAS.IMP_DEUFINBAN AS COLUMNA_68,
        SUP_BLC_PAS.IMP_OTRDEUFINLP AS COLUMNA_69,
        SUP_BLC_PAS.IMP_TCTEDEULP AS COLUMNA_70,
        COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINBAN, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDEUFINLP, 0) + COALESCE(SUP_BLC_PAS.IMP_TCTEDEULP, 0) AS COLUMNA_71,
        SUP_BLC_PAS.IMP_PROVEEDORES AS COLUMNA_72,
        SUP_BLC_PAS.IMP_PROVSUBAFIL AS COLUMNA_73,
        SUP_BLC_PAS.IMP_DEUFINSOC AS COLUMNA_74,
        SUP_BLC_PAS.IMP_OTRAS_DEUDAS AS COLUMNA_75,
        COALESCE(SUP_BLC_PAS.IMP_PROVSUBAFIL, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS, 0) AS COLUMNA_76,
        SUP_BLC_PAS.IMP_MORAT_CTES AS COLUMNA_77,
        SUP_BLC_PAS.IMP_DIFIMP_CTES AS COLUMNA_78,
        SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES AS COLUMNA_79,
        COALESCE(SUP_BLC_PAS.IMP_MORAT_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES, 0) AS COLUMNA_80,
        SUP_BLC_PAS.IMP_OTRPASCP AS COLUMNA_81,
        SUP_BLC_PAS.IMP_HONYDIVAPAG AS COLUMNA_82,
        SUP_BLC_PAS.IMP_PREVPASCTE AS COLUMNA_83,
        COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINBAN, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDEUFINLP, 0)
            + COALESCE(SUP_BLC_PAS.IMP_TCTEDEULP, 0) + COALESCE(SUP_BLC_PAS.IMP_PROVEEDORES, 0) + COALESCE(SUP_BLC_PAS.IMP_PROVSUBAFIL, 0)
            + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS, 0) + COALESCE(SUP_BLC_PAS.IMP_MORAT_CTES, 0)
            + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_CTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRPASCP, 0)
            + COALESCE(SUP_BLC_PAS.IMP_HONYDIVAPAG, 0) + COALESCE(SUP_BLC_PAS.IMP_PREVPASCTE, 0) AS COLUMNA_84,
        SUP_BLC_PAS.IMP_CCRED_IMP_NCTES AS COLUMNA_85,
        SUP_BLC_PAS.IMP_DEUBANCLP AS COLUMNA_86,
        SUP_BLC_PAS.IMP_OBLIGNEG_NCTES AS COLUMNA_87,
        SUP_BLC_PAS.IMP_CTASAPAGLP AS COLUMNA_88,
        COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUBANCLP, 0) + COALESCE(SUP_BLC_PAS.IMP_OBLIGNEG_NCTES, 0) AS COLUMNA_89,
        SUP_BLC_PAS.IMP_CTASAPAGSUBAFIL AS COLUMNA_90,
        SUP_BLC_PAS.IMP_DEUFINSOC_NCTES AS COLUMNA_91,
        SUP_BLC_PAS.IMP_OTRAS_DEUDAS_NCTES AS COLUMNA_92,
        COALESCE(SUP_BLC_PAS.IMP_CTASAPAGSUBAFIL, 0) + COALESCE(SUP_BLC_PAS.IMP_DEUFINSOC_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRAS_DEUDAS_NCTES, 0) AS COLUMNA_93,
        SUP_BLC_PAS.IMP_MORAT_NCTES AS COLUMNA_94,
        SUP_BLC_PAS.IMP_DIFIMP_NCTES AS COLUMNA_95,
        SUP_BLC_PAS.IMP_OTRDS_SOCFISC_NCTES AS COLUMNA_96,
        COALESCE(SUP_BLC_PAS.IMP_MORAT_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_DIFIMP_NCTES, 0) + COALESCE(SUP_BLC_PAS.IMP_OTRDS_SOCFISC_NCTES, 0) AS COLUMNA_97,
        SUP_BLC_PAS.IMP_OTRPASLP AS COLUMNA_98,
        SUP_BLC_PAS.IMP_PREVPASNCTE AS COLUMNA_99,
        SUP_BLC_PAS.IMP_PART3ENSOCC AS COLUMNA_100,
        COALESCE(SUP_BLC_PAS.IMP_CCRED_IMP_NCTES,0)
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
            +COALESCE(SUP_BLC_PAS.IMP_PART3ENSOCC,0) AS COLUMNA_101,
        COALESCE(SUP_BLC_PAS.IMP_CSSUSCINT,0) AS COLUMNA_102,
        COALESCE(SUP_BLC_PAS.IMP_RESVREVBSUSO,0) AS COLUMNA_103,
        COALESCE(SUP_BLC_PAS.IMP_RESVLEGAL,0) AS COLUMNA_104,
        COALESCE(SUP_BLC_PAS.IMP_RESNOASIG,0) AS COLUMNA_105,
        COALESCE(SUP_BLC_PAS.IMP_GANNETEJER,0) AS COLUMNA_106,
        COALESCE(SUP_BLC_PAS.IMP_CSSUSCINT,0)
            +COALESCE(IMP_RESVREVBSUSO,0)
            +COALESCE(IMP_RESVLEGAL,0)
            +COALESCE(IMP_RESNOASIG,0)
            +COALESCE(IMP_GANNETEJER,0) AS COLUMNA_107,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0) AS COLUMNA_108,
        COALESCE(SUP_BLC_ER.IMP_CSTOVTAS,0) AS COLUMNA_109,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0) +COALESCE(IMP_CSTOVTAS,0) AS COLUMNA_110,
        COALESCE(SUP_BLC_ER.IMP_GSADMIN,0) AS COLUMNA_111,
        COALESCE(SUP_BLC_ER.IMP_GSCOMERYOTR,0) AS COLUMNA_112,
        COALESCE(SUP_BLC_ER.TOT_GSOPERAT,0) AS COLUMNA_113,
        COALESCE(SUP_BLC_ER.IMP_BF_INDUSTEXPOR,0) AS COLUMNA_114,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)
            +COALESCE(IMP_CSTOVTAS,0)
            +COALESCE(IMP_GSADMIN,0)
            +COALESCE(IMP_GSCOMERYOTR,0)
            +COALESCE(TOT_GSOPERAT,0)
            +COALESCE(IMP_BF_INDUSTEXPOR,0) AS COLUMNA_115,
        COALESCE(SUP_BLC_ER.IMP_RDOINVERS,0) AS COLUMNA_116,
        COALESCE(SUP_BLC_ER.IMP_REINT_EXP,0) AS COLUMNA_117,
        COALESCE(SUP_BLC_ER.IMP_OTROS_INGRESOS,0) AS COLUMNA_118,
        COALESCE(SUP_BLC_ER.IMP_OTROS_EGRESOS,0) AS COLUMNA_119,
        COALESCE(SUP_BLC_ER.IMP_INTERGAN,0) AS COLUMNA_120,
        COALESCE(SUP_BLC_ER.IMP_REI_RES_FIN_GEN_ACTIVOS,0) AS COLUMNA_121,
        COALESCE(SUP_BLC_ER.IMP_DIFCBIO_RES_FIN_GEN_ACT,0) AS COLUMNA_122,
        COALESCE(SUP_BLC_ER.IMP_RES_TEN_RES_FIN_GEN_ACT,0) AS COLUMNA_123,
        COALESCE(SUP_BLC_ER.IMP_OTROS_RES_FIN_GEN_ACT,0) AS COLUMNA_124,
        COALESCE(SUP_BLC_ER.IMP_INTERGAN,0)
            +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
            +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
            +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
            +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0) AS COLUMNA_125,
        COALESCE(SUP_BLC_ER.IMP_INTERPAG ,0)AS COLUMNA_126,
        COALESCE(SUP_BLC_ER.IMP_REI_RES_FIN_GEN_PAS,0) AS COLUMNA_127,
        COALESCE(SUP_BLC_ER.IMP_DIFCBIO_RES_FIN_GEN_PAS,0) AS COLUMNA_128,
        COALESCE(SUP_BLC_ER.IMP_RESTEN_RES_FIN_GEN_PAS,0) AS COLUMNA_129,
        COALESCE(SUP_BLC_ER.IMP_OTROS_RES_FIN_GEN_PAS,0) AS COLUMNA_130,
        COALESCE(SUP_BLC_ER.IMP_INTERPAG,0)
            +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) AS COLUMNA_131,
        COALESCE(SUP_BLC_ER.IMP_INTERGAN,0)
            +COALESCE(IMP_REI_RES_FIN_GEN_ACTIVOS,0)
            +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_ACT,0)
            +COALESCE(IMP_RES_TEN_RES_FIN_GEN_ACT,0)
            +COALESCE(IMP_OTROS_RES_FIN_GEN_ACT,0)
            +COALESCE(IMP_INTERPAG,0)
            +COALESCE(IMP_REI_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_DIFCBIO_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_RESTEN_RES_FIN_GEN_PAS,0)
            +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) AS COLUMNA_132,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)
            +COALESCE(IMP_CSTOVTAS,0)
            +COALESCE(IMP_GSADMIN,0)
            +COALESCE(IMP_GSCOMERYOTR,0)
            +COALESCE(TOT_GSOPERAT,0)
            +COALESCE(IMP_BF_INDUSTEXPOR,0)
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
            +COALESCE(IMP_OTROS_RES_FIN_GEN_PAS,0) AS COLUMNA_133,
        COALESCE(SUP_BLC_ER.IMP_GCIAEXTRA,0) AS COLUMNA_134,
        COALESCE(SUP_BLC_ER.IMP_PERDEXTRA,0) AS COLUMNA_135,
        COALESCE(SUP_BLC_ER.IMP_PART3ENRESSOCC,0) AS COLUMNA_136,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)
            +COALESCE(IMP_CSTOVTAS,0)
            +COALESCE(IMP_GSADMIN,0)
            +COALESCE(IMP_GSCOMERYOTR,0)
            +COALESCE(TOT_GSOPERAT,0)
            +COALESCE(IMP_BF_INDUSTEXPOR,0)
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
            +COALESCE(IMP_PART3ENRESSOCC,0) AS COLUMNA_137,
        COALESCE(SUP_BLC_ER.IMP_IMPUESTOS,0) AS COLUMNA_138,
        COALESCE(SUP_BLC_ER.IMP_VTASNETAS,0)
            +COALESCE(IMP_CSTOVTAS,0)
            +COALESCE(IMP_GSADMIN,0)
            +COALESCE(IMP_GSCOMERYOTR,0)
            +COALESCE(TOT_GSOPERAT,0)
            +COALESCE(IMP_BF_INDUSTEXPOR,0)
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
            -COALESCE(IMP_IMPUESTOS,0) AS COLUMNA_139,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_CPROD,0) AS COLUMNA_140,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_GSADM,0) AS COLUMNA_141,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_GSCOM,0) AS COLUMNA_142,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_OTRGSOP,0) AS COLUMNA_143,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_OTREGRESOS,0) AS COLUMNA_144,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_PERDEXT,0) AS COLUMNA_145,
        COALESCE(SUP_BLC_DA.IMP_DEPREC_CPROD,0)
            +COALESCE(IMP_DEPREC_GSADM,0)
            +COALESCE(IMP_DEPREC_GSCOM,0)
            +COALESCE(IMP_DEPREC_OTRGSOP,0)
            +COALESCE(IMP_DEPREC_OTREGRESOS,0)
            +COALESCE(IMP_DEPREC_PERDEXT,0) AS COLUMNA_146,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_CPROD,0) AS COLUMNA_147,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GSADM,0) AS COLUMNA_148,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GSCOM,0) AS COLUMNA_149,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_OTRGSOP,0) AS COLUMNA_150,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_OTRINGRESOS,0) AS COLUMNA_151,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_GANEXT,0) AS COLUMNA_152,
        COALESCE(SUP_BLC_DA.IMP_DESRESRTBSUSO_CPROD,0)
            +COALESCE(IMP_DESRESRTBSUSO_GSADM,0)
            +COALESCE(IMP_DESRESRTBSUSO_GSCOM,0)
            +COALESCE(IMP_DESRESRTBSUSO_OTRGSOP,0)
            +COALESCE(IMP_DESRESRTBSUSO_OTRINGRESOS,0)
            +COALESCE(IMP_DESRESRTBSUSO_GANEXT,0) AS COLUMNA_153,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_CPROD,0) AS COLUMNA_154,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_GSADM,0) AS COLUMNA_155,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_GSCOM,0) AS COLUMNA_156,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_OTRGSOP,0) AS COLUMNA_157,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_OTREGRESOS,0) AS COLUMNA_158,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_PERDEXT,0) AS COLUMNA_159,
        COALESCE(SUP_BLC_DA.IMP_AMORTINV_OTREGRESOS,0) AS COLUMNA_160,
        COALESCE(SUP_BLC_DA.IMP_AMORTINT_CPROD,0)
            +COALESCE(IMP_AMORTINT_GSADM,0)
            +COALESCE(IMP_AMORTINT_GSCOM,0)
            +COALESCE(IMP_AMORTINT_OTRGSOP,0)
            +COALESCE(IMP_AMORTINT_OTREGRESOS,0)
            +COALESCE(IMP_AMORTINT_PERDEXT,0)
            +COALESCE(IMP_AMORTINV_OTREGRESOS,0) AS COLUMNA_161,
        COALESCE(SUP_BLC_DA.IMP_PREV_CPROD,0) AS COLUMNA_162,
        COALESCE(SUP_BLC_DA.IMP_PREV_GSADM,0) AS COLUMNA_163,
        COALESCE(SUP_BLC_DA.IMP_PREV_GSCOM,0) AS COLUMNA_164,
        COALESCE(SUP_BLC_DA.IMP_PREV_OTRGSOP,0) AS COLUMNA_165,
        COALESCE(SUP_BLC_DA.IMP_PREV_OTREGRESOS,0) AS COLUMNA_166,
        COALESCE(SUP_BLC_DA.IMP_PREV_PERDEXT,0) AS COLUMNA_167,
        COALESCE(SUP_BLC_DA.IMP_PREV_CPROD,0)
            +COALESCE(IMP_PREV_GSADM,0)
            +COALESCE(IMP_PREV_GSCOM,0)
            +COALESCE(IMP_PREV_OTRGSOP,0)
            +COALESCE(IMP_PREV_OTREGRESOS,0)
            +COALESCE(IMP_PREV_PERDEXT,0) AS COLUMNA_168,
        COALESCE(SUP_BLC_DA.IMP_HONDIR,0) AS COLUMNA_169,
        FEC_ASAMBLEA AS COLUMNA_170,
        COALESCE(IMP_DIVHAPXPAG,0) AS COLUMNA_171,
        COALESCE(SUP_BLC_DA.IMP_GRATIFPERSONAL,0) AS COLUMNA_172,
        COALESCE(IMP_PROVIMP,0) AS COLUMNA_173,
        COALESCE(IMP_COMPRAS,0) AS COLUMNA_174,
        COALESCE(SUP_BLC_DA.IMP_GTOSPROD,0) AS COLUMNA_175,
        COALESCE(SUP_BLC_DA.IMP_ALTAS_BIENES_USO,0) AS COLUMNA_176,
        COALESCE(SUP_BLC_DA.IMP_BAJAS_BIENES_USO,0) AS COLUMNA_177,
        COALESCE(SUP_BLC_DA.IMP_DISMAMORT,0) AS COLUMNA_178,
        COALESCE(SUP_BLC_DA.IMP_ALTAS_INV_PERM,0) AS COLUMNA_179,
        COALESCE(SUP_BLC_DA.IMP_BAJAS_INV_PERM,0) AS COLUMNA_180,
        COALESCE(SUP_BLC_DA.IMP_DIVCOB_SOCVINCULADAS,0) AS COLUMNA_181,
        COALESCE(SUP_BLC_DA.IMP_ALTAS_INT_VAL_LLAVE,0) AS COLUMNA_182,
        COALESCE(SUP_BLC_DA.IMP_ALTAS_INT_GTOS_REORG,0) AS COLUMNA_183,
        COALESCE(SUP_BLC_DA.IMP_BAJAS_INT,0) AS COLUMNA_184,
        COALESCE(SUP_BLC_DA.IMP_VAR_PREV_PASIVO,0) AS COLUMNA_185,
        COALESCE(SUP_BLC_DA.IMP_APORTES_CAPITAL,0) AS COLUMNA_186,
        COALESCE(SUP_BLC_DA.IMP_CONSTRRT,0) AS COLUMNA_187,
        COALESCE(SUP_BLC_DA.IMP_DESAFRRT,0) AS COLUMNA_188,
        COALESCE(SUP_BLC_DA.IMP_AJUSTES_RESANTNAF,0) AS COLUMNA_189,
        COALESCE(SUP_BLC_DA.IMP_AJUSTE_EJERANT,0) AS COLUMNA_190,
        COALESCE(SUP_BLC_DA.IMP_ACT_MONEX,0) AS COLUMNA_191,
        COALESCE(SUP_BLC_DA.IMP_PAS_MONEX,0) AS COLUMNA_192,
        COALESCE(IMP_DOC_DESC,0) AS COLUMNA_193,
        COALESCE(IMP_DOC_ENDOS,0) AS COLUMNA_194 ,
        COALESCE(SUP_BLC_DA.IMP_AJEJERANT_AJINFLA,0) AS COLUMNA_195,
        BLC.ORIGEN AS COLUMNA_196,
        SUP_BLC_ACT.FEC_INI_BLC AS COLUMNA_197,
        SUP_BLC_ACT.TPO_BLC AS COLUMNA_198,
        BLC.NUMERO_PROPUESTA AS COLUMNA_199
    FROM balances BLC
    LEFT JOIN bi_corp_staging.sge_blc_act_prop SUP_BLC_ACT ON SUP_BLC_ACT.id_blc = BLC.id_balance AND SUP_BLC_ACT.nro_prop = BLC.numero_propuesta and SUP_BLC_ACT.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    LEFT JOIN bi_corp_staging.sge_blc_pas_prop SUP_BLC_PAS ON SUP_BLC_PAS.id_blc = BLC.id_balance AND SUP_BLC_PAS.nro_prop = BLC.numero_propuesta and SUP_BLC_PAS.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    LEFT JOIN bi_corp_staging.sge_blc_eres_prop SUP_BLC_ER ON SUP_BLC_ER.id_blc = BLC.id_balance AND SUP_BLC_ER.nro_prop = BLC.numero_propuesta and SUP_BLC_ER.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    LEFT JOIN bi_corp_staging.sge_blc_dadic_prop SUP_BLC_DA ON SUP_BLC_DA.id_blc = BLC.id_balance	AND SUP_BLC_DA.nro_prop = BLC.numero_propuesta and SUP_BLC_DA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    WHERE BLC.ORIGEN = 'SUPERVISION'

    UNION ALL

    -- CPP
    SELECT
        CPP_BLC.ID_BALANCE AS COLUMNA_0,
        CPP_BLC.FEC_BALANCE AS COLUMNA_1,
        PROP.PENUMPER AS COLUMNA_2,
        'N' AS COLUMNA_3,
        CPP_BLC.TPO_BALANCE AS COLUMNA_4,
        'CPP' AS COLUMNA_5,
        CPP_BLC.DISP_ACT_CP AS COLUMNA_6,
        NULL AS COLUMNA_7,
        NULL AS COLUMNA_8,
        NULL AS COLUMNA_9,
        NULL AS COLUMNA_10,
        NULL AS COLUMNA_11,
        NULL AS COLUMNA_12,
        NULL AS COLUMNA_13,
        CPP_BLC.CRED_VTAS_ACT_CP AS COLUMNA_14,
        CPP_BLC.CRED_OTROS_ACT_CP AS COLUMNA_15,
        NULL AS COLUMNA_16,
        NULL AS COLUMNA_17,
        CPP_BLC.CUEN_PART_SOC_CP AS COLUMNA_18,
        NULL AS COLUMNA_19,
        NULL AS COLUMNA_20,
        NULL AS COLUMNA_21,
        CPP_BLC.OTROS_CORR_ACT_CP AS COLUMNA_22,
        NULL AS COLUMNA_23,
        NULL AS COLUMNA_24,
        NULL AS COLUMNA_25,
        NULL AS COLUMNA_26,
        NULL AS COLUMNA_27,
        NULL AS COLUMNA_28,
        NULL AS COLUMNA_29,
        NULL AS COLUMNA_30,
        CPP_BLC.BIEN_CAMBIO_ACT_CP AS COLUMNA_31,
        NULL AS COLUMNA_32,
        COALESCE(CPP_BLC.DISP_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_VTAS_ACT_CP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_OTROS_ACT_CP, 0) + COALESCE(CPP_BLC.OTROS_CORR_ACT_CP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_CP, 0) AS COLUMNA_33,
        NULL AS COLUMNA_34,
        NULL AS COLUMNA_35,
        NULL AS COLUMNA_36,
        NULL AS COLUMNA_37,
        CPP_BLC.CTAS_COBRAR_ACT_LP AS COLUMNA_38,
        NULL AS COLUMNA_39,
        NULL AS COLUMNA_40,
        NULL AS COLUMNA_41,
        CPP_BLC.CUEN_PART_SOC_LP AS COLUMNA_42,
        NULL AS COLUMNA_43,
        NULL AS COLUMNA_44,
        NULL AS COLUMNA_45,
        CPP_BLC.INV_ACT_LP AS COLUMNA_46,
        NULL AS COLUMNA_47,
        NULL AS COLUMNA_48,
        NULL AS COLUMNA_49,
        NULL AS COLUMNA_50,
        NULL AS COLUMNA_51,
        NULL AS COLUMNA_52,
        NULL AS COLUMNA_53,
        NULL AS COLUMNA_54,
        NULL AS COLUMNA_55,
        NULL AS COLUMNA_56,
        CPP_BLC.BIEN_USO_ACT_LP AS COLUMNA_57,
        NULL AS COLUMNA_58,
        NULL AS COLUMNA_59,
        NULL AS COLUMNA_60,
        NULL AS COLUMNA_61,
        CPP_BLC.BIEN_INTANGIBLE_AC AS COLUMNA_62,
        CPP_BLC.BIEN_CAMBIO_NO_CORR AS COLUMNA_63,
        CPP_BLC.OTROS_NCORR_ACT_LP AS COLUMNA_64,
        COALESCE(CPP_BLC.INV_ACT_LP, 0) + COALESCE(CPP_BLC.CTAS_COBRAR_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_NO_CORR, 0) + COALESCE(CPP_BLC.OTROS_NCORR_ACT_LP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_LP, 0) + COALESCE(CPP_BLC.BIEN_USO_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_INTANGIBLE_AC, 0) AS COLUMNA_65,
        COALESCE(CPP_BLC.DISP_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_VTAS_ACT_CP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_ACT_CP, 0) + COALESCE(CPP_BLC.CRED_OTROS_ACT_CP, 0) + COALESCE(CPP_BLC.OTROS_CORR_ACT_CP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_CP, 0) + COALESCE(CPP_BLC.INV_ACT_LP, 0) + COALESCE(CPP_BLC.CTAS_COBRAR_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_CAMBIO_NO_CORR, 0) + COALESCE(CPP_BLC.OTROS_NCORR_ACT_LP, 0) + COALESCE(CPP_BLC.CUEN_PART_SOC_LP, 0) + COALESCE(CPP_BLC.BIEN_USO_ACT_LP, 0) + COALESCE(CPP_BLC.BIEN_INTANGIBLE_AC, 0) AS COLUMNA_66,
        NULL AS COLUMNA_67,
        NULL AS COLUMNA_68,
        NULL AS COLUMNA_69,
        NULL AS COLUMNA_70,
        CPP_BLC.DEU_BAN_PAS_CP AS COLUMNA_71,
        NULL AS COLUMNA_72,
        NULL AS COLUMNA_73,
        NULL AS COLUMNA_74,
        NULL AS COLUMNA_75,
        NULL AS COLUMNA_76,
        NULL AS COLUMNA_77,
        NULL AS COLUMNA_78,
        NULL AS COLUMNA_79,
        NULL AS COLUMNA_80,
        NULL AS COLUMNA_81,
        NULL AS COLUMNA_82,
        NULL AS COLUMNA_83,
        COALESCE(CPP_BLC.DEU_BAN_PAS_CP, 0) + COALESCE(CPP_BLC.PROV_PAS_CP, 0) + COALESCE(CPP_BLC.DEU_SOC_PAS_CP, 0) + COALESCE(CPP_BLC.OTROS_CORR_PAS_CP, 0) + COALESCE(CPP_BLC.DIV_HON_PAGAR_CP, 0) AS COLUMNA_84,
        NULL AS COLUMNA_85,
        NULL AS COLUMNA_86,
        NULL AS COLUMNA_87,
        NULL AS COLUMNA_88,
        CPP_BLC.DEU_BAN_PAS_LP AS COLUMNA_89,
        NULL AS COLUMNA_90,
        NULL AS COLUMNA_91,
        NULL AS COLUMNA_92,
        NULL AS COLUMNA_93,
        NULL AS COLUMNA_94,
        NULL AS COLUMNA_95,
        NULL AS COLUMNA_96,
        NULL AS COLUMNA_97,
        NULL AS COLUMNA_98,
        NULL AS COLUMNA_99,
        NULL AS COLUMNA_100,
        COALESCE(CPP_BLC.DEU_BAN_PAS_LP, 0) + COALESCE(CPP_BLC.PROV_PAS_LP, 0) + COALESCE(CPP_BLC.DEU_SOC_PAS_LP, 0) + COALESCE(CPP_BLC.OTROS_NO_CORR_PAS_LP, 0) + COALESCE(CPP_BLC.PREVISIONES_LP, 0) AS COLUMNA_101,
        NULL AS COLUMNA_102,
        NULL AS COLUMNA_103,
        NULL AS COLUMNA_104,
        NULL AS COLUMNA_105,
        NULL AS COLUMNA_106,
        COALESCE(CPP_BLC.MON_CAPITAL_SOCIAL, 0) + COALESCE(CPP_BLC.MON_RESERVA_TEC, 0) + COALESCE(CPP_BLC.MON_RESERVA_RES, 0) + COALESCE(CPP_BLC.MON_RES_NASIGNADOS, 0) + COALESCE(CPP_BLC.RES_EJERCICIO, 0) AS COLUMNA_107,
        CPP_BLC.ING_VTAS_SERVICIOS AS COLUMNA_108,
        CPP_BLC.COST_VENTAS AS COLUMNA_109,
        COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) AS COLUMNA_110,
        CPP_BLC.GTOS_ADMIN_RES AS COLUMNA_111,
        CPP_BLC.GTOS_COMERC_RES AS COLUMNA_112,
        CPP_BLC.OTROS_GTOS_RES AS COLUMNA_113,
        NULL AS COLUMNA_114,
        COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) AS COLUMNA_115,
        NULL AS COLUMNA_116,
        NULL AS COLUMNA_117,
        CPP_BLC.OTROS_ING_RES AS COLUMNA_118,
        CPP_BLC.OTROS_EGR_RES AS COLUMNA_119,
        NULL AS COLUMNA_120,
        NULL AS COLUMNA_121,
        NULL AS COLUMNA_122,
        NULL AS COLUMNA_123,
        NULL AS COLUMNA_124,
        NULL AS COLUMNA_125,
        NULL AS COLUMNA_126,
        NULL AS COLUMNA_127,
        NULL AS COLUMNA_128,
        NULL AS COLUMNA_129,
        NULL AS COLUMNA_130,
        NULL AS COLUMNA_131,
        CPP_BLC.RES_FINANCIEROS AS COLUMNA_132,
        COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) + COALESCE(CPP_BLC.RES_FINANCIEROS, 0) AS COLUMNA_133,
        CPP_BLC.ING_EXTRAORDINARIO AS COLUMNA_134,
        CPP_BLC.EGR_EXTRAORDINARIO AS COLUMNA_135,
        NULL AS COLUMNA_136,
        COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(COST_VENTAS, 0) - COALESCE(GTOS_ADMIN_RES, 0) - COALESCE(GTOS_COMERC_RES, 0) - COALESCE(OTROS_GTOS_RES, 0) + COALESCE(OTROS_ING_RES, 0) - COALESCE(OTROS_EGR_RES, 0) + COALESCE(RES_FINANCIEROS, 0) + COALESCE(ING_EXTRAORDINARIO, 0) - COALESCE(EGR_EXTRAORDINARIO, 0) AS COLUMNA_137,
        IMPU_RES AS COLUMNA_138,
        COALESCE(CPP_BLC.ING_VTAS_SERVICIOS, 0) - COALESCE(CPP_BLC.COST_VENTAS, 0) - COALESCE(CPP_BLC.GTOS_ADMIN_RES, 0) - COALESCE(CPP_BLC.GTOS_COMERC_RES, 0) - COALESCE(CPP_BLC.OTROS_GTOS_RES, 0) + COALESCE(CPP_BLC.OTROS_ING_RES, 0) - COALESCE(CPP_BLC.OTROS_EGR_RES, 0) + COALESCE(CPP_BLC.RES_FINANCIEROS, 0) + COALESCE(CPP_BLC.ING_EXTRAORDINARIO, 0) - COALESCE(CPP_BLC.EGR_EXTRAORDINARIO, 0) - COALESCE(CPP_BLC.IMPU_RES, 0)  AS COLUMNA_139,
        NULL AS COLUMNA_140,
        NULL AS COLUMNA_141,
        NULL AS COLUMNA_142,
        NULL AS COLUMNA_143,
        NULL AS COLUMNA_144,
        NULL AS COLUMNA_145,
        CPP_BLC.AMORT_BIEN_USO AS COLUMNA_146,
        NULL AS COLUMNA_147,
        NULL AS COLUMNA_148,
        NULL AS COLUMNA_149,
        NULL AS COLUMNA_150,
        NULL AS COLUMNA_151,
        NULL AS COLUMNA_152,
        NULL AS COLUMNA_153,
        NULL AS COLUMNA_154,
        NULL AS COLUMNA_155,
        NULL AS COLUMNA_156,
        NULL AS COLUMNA_157,
        NULL AS COLUMNA_158,
        NULL AS COLUMNA_159,
        NULL AS COLUMNA_160,
        CPP_BLC.AMORT_BIEN_INTANGIBLE AS COLUMNA_161,
        NULL AS COLUMNA_162,
        NULL AS COLUMNA_163,
        NULL AS COLUMNA_164,
        NULL AS COLUMNA_165,
        NULL AS COLUMNA_166,
        NULL AS COLUMNA_167,
        NULL AS COLUMNA_168,
        NULL AS COLUMNA_169,
        CPP_BLC.FEC_ASAMBLEA AS COLUMNA_170,
        NULL AS COLUMNA_171,
        NULL AS COLUMNA_172,
        NULL AS COLUMNA_173,
        CPP_BLC.MON_COMPRAS AS COLUMNA_174,
        NULL AS COLUMNA_175,
        NULL AS COLUMNA_176,
        NULL AS COLUMNA_177,
        NULL AS COLUMNA_178,
        NULL AS COLUMNA_179,
        NULL AS COLUMNA_180,
        NULL AS COLUMNA_181,
        NULL AS COLUMNA_182,
        NULL AS COLUMNA_183,
        NULL AS COLUMNA_184,
        NULL AS COLUMNA_185,
        NULL AS COLUMNA_186,
        NULL AS COLUMNA_187,
        NULL AS COLUMNA_188,
        NULL AS COLUMNA_189,
        NULL AS COLUMNA_190,
        NULL AS COLUMNA_191,
        NULL AS COLUMNA_192,
        NULL AS COLUMNA_193,
        NULL AS COLUMNA_194,
        NULL AS COLUMNA_195,
        BLC.ORIGEN AS COLUMNA_196,
        NULL AS COLUMNA_197,
        NULL AS COLUMNA_198,
        BLC.NUMERO_PROPUESTA AS COLUMNA_199
    FROM balances BLC
    LEFT JOIN bi_corp_staging.sge_balances_prop CPP_BLC ON CPP_BLC.id_balance = BLC.id_balance AND BLC.numero_propuesta = CPP_BLC.nro_prop and CPP_BLC.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    LEFT JOIN bi_corp_staging.sge_propuesta PROP ON PROP.NRO_PROP = CPP_BLC.NRO_PROP and PROP.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}'
    WHERE BLC.origen = 'CPP'
) as balance_final;