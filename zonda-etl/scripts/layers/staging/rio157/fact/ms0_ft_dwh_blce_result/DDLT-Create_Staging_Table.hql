CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_ft_dwh_blce_result (
    idf_cto_ods  string,
    cod_contenido  string,
    fec_data  string,
    cod_pais  string,
    idf_pers_ods  string,
    cod_entidad_espana  string,
    cod_producto_gest  string,
    cod_divisa  string,
    cod_reajuste  string,
    cod_agrp_func  string,
    cod_est_sdo  string,
    cod_cta_cont_gestion  string,
    cod_area_negocio  string,
    cod_tip_ajuste  string,
    cod_centro_cont  string,
    cod_ofi_comercial  string,
    cod_origen_inf  string,
    cod_sis_origen  string,
    ind_conciliacion  string,
    ind_entidad  string,
    tip_concepto_ctb  string,
    ind_pool  string,
    imp_sdo_cap_ml  string,
    imp_sdo_cap_mo  string,
    imp_sdo_int_ml  string,
    imp_sdo_int_mo  string,
    imp_sdo_reajuste_ml  string,
    imp_sdo_reajuste_mo  string,
    imp_sdo_insolv_ml  string,
    imp_sdo_insolv_mo  string,
    imp_sdo_cap_med_ml  string,
    imp_sdo_cap_med_mo  string,
    imp_sdo_med_int_ml  string,
    imp_sdo_med_int_mo  string,
    imp_sdo_med_reajuste_ml  string,
    imp_sdo_med_reajuste_mo  string,
    imp_sdo_med_insolv_ml  string,
    imp_sdo_med_insolv_mo  string,
    imp_egr_cap_ml  string,
    imp_egr_cap_mo  string,
    imp_egr_per_ml  string,
    imp_egr_per_mo  string,
    imp_egr_reaj_ml  string,
    imp_egr_reaj_mo  string,
    imp_ing_cap_ml  string,
    imp_ing_cap_mo  string,
    imp_ing_per_ml  string,
    imp_ing_per_mo  string,
    imp_ing_reaj_ml  string,
    imp_ing_reaj_mo  string,
    imp_ajtti_egr_tb_cap_ml  string,
    imp_ajtti_egr_tb_cap_mo  string,
    imp_ajtti_egr_sl_cap_ml  string,
    imp_ajtti_egr_sl_cap_mo  string,
    imp_ajtti_egr_per_ml  string,
    imp_ajtti_egr_per_mo  string,
    imp_ajtti_egr_reajuste_ml  string,
    imp_ajtti_egr_reajuste_mo  string,
    imp_ajtti_ing_tb_cap_ml  string,
    imp_ajtti_ing_tb_cap_mo  string,
    imp_ajtti_ing_sl_cap_ml  string,
    imp_ajtti_ing_sl_cap_mo  string,
    imp_ajtti_ing_per_ml  string,
    imp_ajtti_ing_per_mo  string,
    imp_ajtti_ing_reajuste_ml  string,
    imp_ajtti_ing_reajuste_mo  string,
    imp_efec_enc_ml  string,
    imp_efec_enc_mo  string,
    imp_egr_cap_ml_acum  string,
    imp_egr_cap_mo_acum  string,
    imp_egr_per_ml_acum  string,
    imp_egr_per_mo_acum  string,
    imp_egr_reaj_ml_acum  string,
    imp_egr_reaj_mo_acum  string,
    imp_ing_cap_ml_acum  string,
    imp_ing_cap_mo_acum  string,
    imp_ing_per_ml_acum  string,
    imp_ing_per_mo_acum  string,
    imp_ing_reaj_ml_acum  string,
    imp_ing_reaj_mo_acum  string,
    imp_ajtti_ing_tb_cap_ml_acum  string,
    imp_ajtti_ing_tb_cap_mo_acum  string,
    imp_ajtti_ing_sl_cap_ml_acum  string,
    imp_ajtti_ing_sl_cap_mo_acum  string,
    imp_ajtti_ing_per_ml_acum  string,
    imp_ajtti_ing_per_mo_acum  string,
    imp_ajtti_ing_reajuste_ml_acum  string,
    imp_ajtti_ing_reajuste_mo_acum  string,
    imp_ajtti_egr_tb_cap_ml_acum  string,
    imp_ajtti_egr_tb_cap_mo_acum  string,
    imp_ajtti_egr_sl_cap_ml_acum  string,
    imp_ajtti_egr_sl_cap_mo_acum  string,
    imp_ajtti_egr_per_ml_acum  string,
    imp_ajtti_egr_per_mo_acum  string,
    imp_ajtti_egr_reajuste_ml_acum  string,
    imp_ajtti_egr_reajuste_mo_acum  string,
    imp_efec_enc_ml_acum  string,
    imp_efec_enc_mo_acum  string,
    var_imp_sdo_cap_mes_ant_ml  string,
    var_imp_sdo_cap_dic_ant_ml  string,
    var_imp_sdo_int_mes_ant_ml  string,
    var_imp_sdo_int_dic_ant_ml  string,
    ind_rem  string,
    origen_tasa  string,
    out_tti  string,
    out_tti_per  string,
    out_tti_sub  string,
    out_tti_remar  string,
    out_tti_volatil  string,
    out_tti_estab  string,
    spread_liq  string,
    spread_oper  string,
    tasa_base  string,
    tipo_tasa_adis  string,
    plz_prx_rev_x_sdo  string,
    plz_vto_rem_x_sdo  string,
    imp_tc_sdo_ml  string,
    imp_tti_sdo_ml  string,
    cla_operacion_com  string,
    idf_fondo  string,
    cod_tip_fondo  string,
    timest_umo  string,
    userid_umo  string,
    cod_proceso  string,
    cod_destino_fondo  string,
    cod_tip_coste  string,
    cod_promemoria  string,
    cod_lote  string,
    nom_fichero  string,
    fec_carga  string,
    ind_reparto  string,
    cod_posicion_valor  string,
    ind_devengo  string,
    cod_segmento_gest  string,
    cod_tip_tas  string,
    signo  string,
    cod_gestor  string,
    cod_usuario_alta  string,
    tipo_rtt  string,
    imp_sdo_cap_dic_ml  string,
    imp_sdo_int_dic_ml  string,
    out_efenpuro_ml  string,
    out_efenpuro_mo  string,
    cod_cartera_gestion  string,
    des_ajuste  string,
    cod_univ  string,
    cod_gestor_prod  string
        )
    PARTITIONED BY (
      partition_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/fact/ms0_ft_dwh_blce_result'