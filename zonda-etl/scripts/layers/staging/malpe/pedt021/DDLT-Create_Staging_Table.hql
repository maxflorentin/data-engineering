CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt021(
pecdgent string COMMENT 'codigo de la entidad',
penumpue string COMMENT 'numero de puesto',
penumper string COMMENT 'numero de cliente',
petipofi string COMMENT 'tipo de oficial (tc258)',
peuserid string COMMENT 'user id del oficial',
petipban string COMMENT 'tipo de banca (tc232)',
peregofi string COMMENT 'region del oficial (tc259)',
pezonofi string COMMENT 'zona del oficial (tc260)',
penumsuc string COMMENT 'numero de sucursal (tcdt050)',
pecencos string COMMENT 'centro de costos asociado (tcdt050)',
penumpto string COMMENT 'nro. de puesto del que depende',
penivres string COMMENT 'nivel de responsable (tc-262)',
peestact string COMMENT 'estado actual',
peestant string COMMENT 'estado anterior',
pefecest string COMMENT 'fecha de estado',
peranofi string COMMENT 'rango (tc263)',
pesegofi string COMMENT 'segmento del oficial (banco santiago) (tc272)',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ult modif del registro',
peobserv_len string COMMENT 'observaciones',
peobserv_text string COMMENT 'observaciones'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt021';