CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_sorpresa_cascada(
fecha_envio  string,
nup          string,
fecha_inicio string,
fecha_fin    string,
cascada_yn   string,
canal_alta   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/sorpresa_cascada';