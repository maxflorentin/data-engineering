CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_ptb_pedt015(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero del cliente',
petipdoc string COMMENT 'tipo de documento',
penumdoc string COMMENT 'numero del documento',
pesecdoc string COMMENT 'secuencia de identificacion del documento',
peexppor string COMMENT 'expedido por',
pefecven string COMMENT 'fecha de vencimiento',
pemarver string COMMENT 'marca de verificacion contra padron externo',
pecondoc string COMMENT 'condicion del documento (principal/alternativo)',
pecodpai string COMMENT 'codigo de pais/provincia',
pefecexp string COMMENT 'fecha de expedicion',
pelugexp string COMMENT 'lugar de expedicion',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'nro de usuario de ult modificacion',
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe-post-batch/fact/pedt015';