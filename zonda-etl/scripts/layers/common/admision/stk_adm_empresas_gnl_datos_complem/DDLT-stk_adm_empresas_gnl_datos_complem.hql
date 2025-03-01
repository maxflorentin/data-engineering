CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_gnl_datos_complem (
    ds_adm_nrotramite_zx string,
    cod_per_nup string,
    ds_adm_cliente_nuevo string,
    ds_adm_banca string,
    ds_adm_segm_origen string,
    ds_adm_tipo_per string,
    ds_adm_tipo_emp string,
    ds_adm_tipo_linea string,
    int_adm_nro_prop int,
    dt_adm_fecvigencia string,
    ds_adm_mca_vigencia string,
    ds_adm_obs_vigencia string,
    ds_adm_mca_calificacion string,
    fc_adm_mon_lin_otorgada decimal(16,2),
    fc_adm_mon_lin_otorgada_no_cesion decimal(16,2),
    ds_adm_real_calificacion string,
    ds_adm_obs_calificacion string,
    ds_adm_mca_disponible string,
    ds_adm_real_disponible string,
    fc_adm_mon_disponible decimal(16,2),
    ds_adm_obs_disponible string,
    ds_adm_mca_garantia string,
    ds_adm_real_garantia string,
    ds_adm_obs_garantia string,
    ds_adm_mca_cli_vin string,
    ds_adm_real_cli_vin string,
    ds_adm_obs_cli_vin string,
    ds_adm_mca_afip string,
    ds_adm_real_afip string,
    ds_adm_obs_afip string,
    ds_adm_mca_bcra string,
    ds_adm_real_bcra string,
    ds_adm_obs_bcra string,
    ds_adm_secu_f487  string,
    ds_adm_mca_opera string,
    ds_adm_obs_opera string,
    ds_adm_mca_especial_granel string,
    ds_adm_obs_especial_granel string,
    ds_adm_mca_especial_f487 string,
    ds_adm_obs_especial_f487 string,
    ds_adm_motiv_nopera string,
    cod_adm_pevalind string,
    cod_adm_peindica string,
    dec_adm_tasa_min decimal(16,2),
    int_adm_cta_cte bigint,
    ds_adm_prod_cta_cte string,
    ds_adm_subp_cta_cte string,
    dt_adm_fecalta string,
    ds_adm_usu_alta string,
    dt_adm_fecmodif string,
    ds_adm_usu_modif string,
    ds_adm_rechazo_antecedentes_int string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_gnl_datos_complem';