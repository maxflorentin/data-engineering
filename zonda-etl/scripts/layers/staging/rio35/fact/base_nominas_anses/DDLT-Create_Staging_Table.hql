CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio35_base_nominas_anses(
FECHA                string,
CUIT                 string,
PENUMPER             string,
CAPITAS_BRIO         string,
CAPITAS_ANSES        string,
IND_BAJA             string,
TITULAR              string,
TIENE_PAS            string,
MASA_SALARIAL        string
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio35/fact/base_nominas_anses';