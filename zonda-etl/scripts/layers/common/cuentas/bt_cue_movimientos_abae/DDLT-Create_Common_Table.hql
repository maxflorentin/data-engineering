CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cue_movimientos_abae  (
 cod_cue_origen string
,ts_cue_transaccion timestamp
,dt_cue_transaccion string
,dt_cue_banco string
,cod_cue_estado_transaccion int
,ds_cue_estado_transaccion string
,ds_cue_receptor_producto string
,cod_cue_version_dpc int
,ds_cue_num_dpc int
,flag_cue_primer_uso int
,cod_cue_tipo_mensaje int
,ds_cue_tipo_mensaje string
,cod_cue_tarjeta bigint
,cod_cue_tipo_movimiento int
,ds_cue_tipo_movimiento string
,cod_cue_tipo_cuenta_desde int
,ds_cue_tipo_cuenta_desde string
,cod_cue_tipo_cuenta_hasta int
,ds_cue_tipo_cuenta_hasta string
,fc_cue_importe_cuenta decimal(18,2)
,fc_cue_importe_compra_impuesto decimal(18,2)
,fc_cue_importe_compra_usd decimal(18,2)
,cod_cue_comercio_camp string
,cod_cue_autorizacion int
,cod_cue_tipo_tarjeta_banelco int
,flag_cue_tarjeta_banelco int
,cod_cue_trace_host int
,cod_cue_terminal string
,cod_cue_entidad_tarjeta int
,cod_cue_moneda string
,ds_cue_moneda string
,cod_cue_reverso int
,ds_cue_reverso string
,cod_cue_canal string
,ds_cue_canal string
,flag_cue_consumidor_final int
,cod_cue_semrcard string
,cod_cue_semtrnad string
,flag_cue_operacion_aceptada int
,ds_cue_terminal_ciudad string
,cod_cue_dni_cuil_cta_destino bigint
,fc_cue_comision decimal(18,2)
,cod_cue_seg_tipo_mov int
,cod_cue_seg_tipo_seguro int
,cod_cue_seg_identificacion int
,fc_cue_seg_costo_cobertura decimal(18,2)
,cod_cue_sor_banelco int
,fc_cue_sor_banelco_importe decimal(18,2)
,cod_cue_sor_bancos int
,fc_cue_sor_bancos_importe decimal(18,2)
,cod_cue_longitud_cuenta_desde int
,cod_cue_debito_desde bigint
,cod_cue_credito_desde bigint
,cod_suc_sucursal_desde int
,cod_cue_cuenta_desde bigint
,cod_cue_longitud_cuenta_hasta int
,cod_cue_debito_hasta bigint
,cod_cue_credito_hasta bigint
,cod_suc_sucursal_hasta int
,cod_cue_cuenta_hasta bigint
,cod_cue_terminal_pais string
,ds_cue_terminal_pais string
,cod_cue_tarjeta_banco string
,cod_cue_terminal_banco string
,cod_cbu_destino string
,cod_cue_titularidad int
,ds_cue_titularidad string
,cod_cue_tipo_oper string
,cod_cue_doc_benef string
,ds_cue_num_benef string
,cod_cue_mand_identif string
,cod_cue_comercio string
,cod_cue_atm string
,cod_cue_comprobante int
,cod_cue_sembanc_1 string
,fc_cue_porcen_rentencion int
,fc_cue_porcen_percepcion int
,fc_cue_retencion_imp_pais int
,cod_cue_tipo_extraccion int
,cod_cue_tipo_deposito int
,cod_cue_tipo_debito int
,cod_cue_tipo_pago string
,cod_cue_tipo_tarjeta string
,ds_cue_tipo_tarjeta string
,ds_cue_origen string
,cod_cue_tipo_debin string
,cod_cue_debin string
,cod_cue_cuit_bco_cdor string
,cod_cue_cbu_bco_cdor string
,cod_cue_cuit_bco_vdor string
,cod_cue_cbu_bco_vdor string
,dt_cue_neg_coel string
,cod_cue_debin_scoring int
,cod_cue_debin_cpto  string
,ds_cue_cpto_deb string
,cod_cue_corresp_titu int
,cod_cue_moneda_origen string
,ds_cue_moneda_origen string
,fc_cue_importe_original decimal(18,2)
,ds_cue_fefa string
,cod_cue_cotco int
,cod_cue_cotve int
,fc_cue_semant5 int
,fc_cue_imp_ganan int
,ds_cue_num_control string
,ds_cue_banco_receptor string
,cod_cue_concepto int
,ds_cue_concepto string
,ds_cue_trans_referencia string
,cod_cue_num_cargo bigint
,cod_cue_porc_devl_clte int
,cod_cue_porc_devl_comer int
,cod_cue_rubro string
,cod_cue_balanceo int
,cod_cue_tipo_cajero string
,cod_per_nup_tarjeta int
,cod_per_nup_titular int
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/bt_cue_movimientos_abae'