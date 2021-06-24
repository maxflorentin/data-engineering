CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_tipoestadosolicitud(

CODIGO           STRING,
DESCRI           STRING,
GESTION          STRING,
ESTADOPRODUCTO   STRING,
ESTADOASOL       STRING,
ORDENBO          STRING,
IMPRIMESOLICITUD STRING,
ASOL_BO          STRING,
ESTADORECTOR     STRING,
ACTIVO           STRING,
TIPO_ESTADO      STRING,
DOCUMENTACION    STRING,
CODIGOMENSAJE    STRING
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/tipoestadosolicitud';