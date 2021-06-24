CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt063(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de persona',
pecodafip string COMMENT 'codigo afip',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'terminal de ultima modificacion',
pesucmod string COMMENT 'sucursal de ultima modificacion',
pehstamp string COMMENT 'timestamp'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt063';