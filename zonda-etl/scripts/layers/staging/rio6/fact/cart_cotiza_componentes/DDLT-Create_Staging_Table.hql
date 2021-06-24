CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_cotiza_componentes(
    cacx_capj_cd_sucursal     string,
    cacx_cazb_nu_cotizacion   string,
    cacx_carp_cd_ramo         string,
    cacx_capu_cd_producto     string,
    cacx_capb_cd_plan         string,
    cacx_cafp_cd_fr_pago      string,
    cacx_capp_cd_componente   string,
    cacx_mt_componente        string,
    cacx_ta_componente        string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cart_cotiza_componentes';