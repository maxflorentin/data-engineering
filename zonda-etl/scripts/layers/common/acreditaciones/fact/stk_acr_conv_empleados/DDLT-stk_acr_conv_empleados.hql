CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_acr_conv_empleados (
    cod_acr_suscriptor              bigint,
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
    cod_prod_producto               string,
    cod_prod_subproducto            string,
    cod_prod_producto_contab        string,
    cod_prod_subproducto_contab     string,
    cod_acr_estado                  string,
    dt_acr_estado                   string,
    cod_acr_mae_entidad_umo         string,
    cod_acr_mae_centro_umo          bigint,
    cod_acr_mae_userid_umo          string,
    cod_acr_mae_netname_umo         string,
    cod_acr_mae_timest_umo          string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/acreditaciones/fact/stk_acr_conv_empleados';
