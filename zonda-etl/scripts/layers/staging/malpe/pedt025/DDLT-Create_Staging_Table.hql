CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt025(
pecdgent string COMMENT 'codigo de la entidad',
penumpar string COMMENT 'n de participante (identificacion del vinculado al grupo)',
penumgru string COMMENT 'numero de grupo',
petipgru string COMMENT 'tipo de vinculo',
petiprel string COMMENT 'tipo de relacion',
peporpar string COMMENT 'porcentaje de participacion',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modificacion del registro'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt025';