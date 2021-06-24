CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_ptb_pedt003(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero del cliente',
pesecdom string COMMENT 'secuencia de domicilio',
pesecdom_sgn string COMMENT 'signo',
petipdom string COMMENT 'tipos de domicilios',
petipvia string COMMENT 'tipo de via',
penomcal string COMMENT 'calle',
petipcnt string COMMENT 'tipo construccion',
petipnur string COMMENT 'tipo nucleo urbano',
pesuccas string COMMENT 'sucursal casilla correos',
penumblo string COMMENT 'casilla/aptdo correos',
penomloc string COMMENT 'localidad / ciudad',
pedesloc string COMMENT 'descripcion localidad / ciudad',
penomcom string COMMENT 'comuna',
pedescmn string COMMENT 'descripcion comuna',
pecodpos string COMMENT 'codigo postal',
perutcar string COMMENT 'ruta cartero',
perutcar_sgn string COMMENT 'signo',
pecodpro string COMMENT 'codigo de provincia/region',
pecodpai string COMMENT 'codigo de pais',
petitdom string COMMENT 'titularidad del domicilio',
pefecver string COMMENT 'fecha de verificacion',
pemarnor string COMMENT 'marca de normalizacion',
pemardom string COMMENT 'marca de domicilio erroneo',
pemarcor string COMMENT 'marca de correspondencia devuelta',
pemotdev string COMMENT 'motivo de devolucion',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modificacion del registro',
peobserv string COMMENT 'complemento variable'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe-post-batch/fact/pedt003';