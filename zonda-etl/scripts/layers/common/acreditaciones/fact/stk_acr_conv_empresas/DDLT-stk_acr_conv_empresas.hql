CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_acr_conv_empresas (
    cod_acr_suscriptor              bigint,
    ds_acr_suscriptor               string,
    cod_acr_num_convenio            bigint,
    cod_acr_convenio                string,
    cod_acr_tipo_suscriptor         string,
    ds_acr_tipo_suscriptor          string,
    cod_per_nup                     bigint,
    ds_per_cuit                     string,
    cod_acr_entidad                 string,
    cod_acr_sucursal                bigint,
    cod_acr_cuenta                  bigint,
    cod_div_divisa                  string,
    cod_prod_producto_contab        string,
    cod_prod_subproducto_contab     string,
    cod_acr_estado                  string,
    dt_acr_estado                   string,
    cod_acr_concepto                string,
    fc_acr_porc_suscriptor          bigint,
    fc_acr_porc_entidad             bigint,
    fc_acr_porc_cliente             bigint,
    cod_acr_tbgb_entidad_umo        string,
    cod_acr_tbgb_centro_umo         bigint,
    cod_acr_tbgb_userid_umo         string,
    cod_acr_tbgb_netname_umo        string,
    ts_acr_tbgb_timest_umo          string,
    cod_acr_mco_entidad_umo         string,
    cod_acr_mco_centro_umo          bigint,
    cod_acr_mco_userid_umo          string,
    cod_acr_mco_netname_umo         string,
    ts_acr_mco_timest_umo           string
)
PARTITIONED BY (
    partition_date          string)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/acreditaciones/fact/stk_acr_conv_empresas';
