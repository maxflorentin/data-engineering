CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio53_marc_pin (
    penumper string,
    tipo_clave string,
    fecha_alta string,
    fecha_cambio string,
    fecha_acceso string,
    marca_bloqueo string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio53/fact/marc_pin'