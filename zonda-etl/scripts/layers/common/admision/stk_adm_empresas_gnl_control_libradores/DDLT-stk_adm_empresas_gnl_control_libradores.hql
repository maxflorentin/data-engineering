CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_gnl_control_libradores (
    cod_adm_nro_tramite_zx int,
    cod_per_nup int,
    id_adm_librador int,
    cod_adm_nro_doc bigint,
    int_adm_plazo int,
    cod_adm_suc_cta_credito int,
    ds_adm_tipo_cta_credito string,
    cod_nro_cuenta_credito bigint,
    fc_adm_monto_librador decimal(16,2),
    flag_adm_mca_atomizacion string,
    flag_adm_real_atomizacion string,
    flag_adm_nosis_realizado string,
    flag_adm_nosis_bcra string,
    flag_adm_nosis_inhabilitados string,
    flag_adm_nosis_cheques_rec string,
    flag_adm_nosis_quiebra string,
    flag_adm_nosis_concurso string,
    ds_adm_observaciones string,
    ts_adm_fec_alta string,
    ds_adm_usu_alta string,
    ts_adm_fec_modif string,
    ds_adm_usu_modif string,
    ds_adm_mca_vinculado string,
    ds_adm_obs_vinculado string,
    ds_adm_rechazo_antecedentes_int string,
    cod_adm_tipo_cliente string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_gnl_control_libradores';