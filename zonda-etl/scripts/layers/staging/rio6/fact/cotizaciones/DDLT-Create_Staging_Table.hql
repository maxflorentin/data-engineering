CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cotizaciones
(
   --   ivct_fe_operacion
        ivct_cd_pais	string,
        ivct_de_pais	string,
        ivct_cd_entidad	string,
        ivct_de_entidad	string,
        ivct_cd_compania	string,
        ivct_de_compania	string,
        ivct_cd_canal	string,
        ivct_de_canal	string,
        ivct_cd_region	string,
        ivct_de_region	string,
        ivct_cd_zona	string,
        ivct_de_zona	string,
        ivct_cd_sucursal	string,
        ivct_de_sucursal	string,
        ivct_cd_producto_seguro	string,
        ivct_de_producto_seguro	string,
        ivct_cd_producto_bancario	string,
        ivct_de_producto_bancario	string,
        ivct_cd_moneda	string,
        ivct_de_moneda	string,
        ivct_cd_tipo_mercado	string,
        ivct_de_tipo_mercado	string,
        ivct_cd_tipo_cliente	string,
        ivct_de_tipo_cliente	string,
        ivct_cd_segmento_cliente	string,
        ivct_de_segmento_cliente	string,
        ivct_tp_documento	string,
        ivct_nu_documento	string,
        ivct_nu_nup	string,
        ivct_nm_asegurado	string,
        ivct_cd_usuario	string,
        ivct_nm_usuario	string,
        ivct_nu_cotizacion	string,
        ivct_cd_ramo	string,
        ivct_de_ramo	string,
        ivct_cd_producto_rector	string,
        ivct_de_producto_rector	string,
        ivct_cd_plan_rector	string,
        ivct_de_plan_rector	string,
        ivct_cd_clase_rector	string,
        ivct_de_clase_rector	string,
        ivct_mt_premio	string,
        ivct_mt_prima	string,
        ivct_mt_comision	string,
        ivct_mt_suma_asegurada	string,
        ivct_fe_emision	string,
        ivct_nu_poliza	string,
        ivct_nu_certificado	string
)
PARTITIONED BY (
  ivct_fe_operacion string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/cotizaciones/'