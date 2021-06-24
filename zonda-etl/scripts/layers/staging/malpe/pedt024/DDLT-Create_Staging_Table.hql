CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt024(
pecdgent string COMMENT 'codigo de entidad',
penumgru string COMMENT 'numero de grupo.',
pedesgru string COMMENT 'descripcion del grupo',
petipgru string COMMENT 'tipo de grupo',
petipact string COMMENT 'tipo de actividad',
pesucgru string COMMENT 'sucursal',
peforjur string COMMENT 'forma juridica',
pepaires string COMMENT 'pais de residencia',
pemodope string COMMENT 'modo de operacion',
perescom string COMMENT 'responsable comercial',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modif del registro'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt024';