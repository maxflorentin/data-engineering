CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_ptb_pedt052(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de persona',
peindica string COMMENT 'codigo de indicador',
pevalind string COMMENT 'valor del indicador',
peusualt string COMMENT 'usuario de alta del registro',
pefecalt string comment 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string comment 'timestamp ultima modif del registro'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe-post-batch/fact/pedt052';