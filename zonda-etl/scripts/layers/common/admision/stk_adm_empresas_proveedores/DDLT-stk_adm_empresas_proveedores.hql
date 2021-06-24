CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_proveedores (
    cod_adm_penumper int,
    cod_adm_nro_ultima_prop int,
    cod_adm_prov int,
    ds_adm_prov string,
    int_adm_plz_pago int,
    int_adm_porc_compras int,
    cod_adm_ref_obt string,
    cod_adm_usu_alta string,
    dt_adm_fec_alta string,
    cod_adm_usu_mod string,
    dt_adm_fec_mod string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_proveedores';