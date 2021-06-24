CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt223(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero de persona',
peentpre string COMMENT 'codigo de universidad',
petipreu string COMMENT 'tipo de relacion con universidad (alumno,academico,administrativo)',
pecodcar string COMMENT 'codigo de la carrera',
pecodgra string COMMENT 'codigo de grado',
pefeciun string COMMENT 'fecha de ingreso',
peconven string COMMENT 'codigo de convenio',
peconven_sgn string COMMENT 'signo',
penomcor string COMMENT 'nombre corto del universitario',
peusobib string COMMENT 'codigo usuario bibioteca unap',
pecodbar string COMMENT 'codigo de barra puc, uc',
petipdpa string COMMENT 'tipo de documento del padre',
pedocpad string COMMENT 'numero de documento del padre',
penompad string COMMENT 'nombre del padre',
petipdma string COMMENT 'tipo de documento de la madre',
pedocmad string COMMENT 'numero de documento de la madre',
penommad string COMMENT 'nombre de la madre',
perenfam string COMMENT 'monto renta familiar',
perenfam_sgn string COMMENT 'signo',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta',
peusumod string COMMENT 'usuario de modificacion',
petermod string COMMENT 'terminal de modificacion',
pesucmod string COMMENT 'sucursal de modificacion',
pehstamp string COMMENT 'timestamp ultima modif del registro',
pecodclo string COMMENT 'codigo de ciclo',
peacredit string COMMENT 'indicador credito'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt223';