CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt036(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de cliente',
petiping string COMMENT 'tipo de ingreso',
pefecing string COMMENT 'fecha de actualización del ingreso',
pecodmon string comment 'codigo de moneda',
peimping string COMMENT 'importe de ingreso',
peimphas string COMMENT 'importe hasta',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta registro',
peusumod string comment 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modificacion del registro',
peperren string ,
petipcom string,
pefonpag string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt036';