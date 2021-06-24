CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt030(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de cliente',
peclaseg string COMMENT 'clase de segmento',
pesegcla string COMMENT 'segmento de la clase definida',
pefecseg string comment 'fecha de segmentacion',
pesegman string COMMENT 'indicador de segmentacion (manual/automatica)',
pemarper string COMMENT 'marca de permanente o sujeto a resegmentacion',
pesegcal string COMMENT 'segmento calculado',
peusualt string COMMENT 'usuario de alta',
pefecalt string comment 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string comment 'timestamp ultima modificacion del registro'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt030';