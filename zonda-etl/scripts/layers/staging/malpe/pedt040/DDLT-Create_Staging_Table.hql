CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt040(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de cliente',
pesucadm string COMMENT 'sucursal administradora',
pebancap string COMMENT 'banca',
pedivisi string COMMENT 'division',
petealea string COMMENT 'team leader',
peejecue string COMMENT 'ejecutivo de cuenta',
pejefare string COMMENT 'jefe de area',
pegerent string COMMENT 'gerente',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal  ultima modificacion del registro',
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt040';