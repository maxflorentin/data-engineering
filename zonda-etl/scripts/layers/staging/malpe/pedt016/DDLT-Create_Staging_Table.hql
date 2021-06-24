CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt016(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero del cliente',
penclref string COMMENT 'numero de cliente refundido',
peestref string COMMENT 'estado de la refundicion ( c/p/r/e)',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modificacion del registro',
pedatper_len string COMMENT 'datos basicos del cliente que permanece',
pedatper_text string COMMENT 'datos basicos del cliente que permanec',
pedatdes_len string COMMENT 'datos basicos del cliente que desaparece',
pedatdes_text string COMMENT 'datos basicos del cliente que desaparece'

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt016';