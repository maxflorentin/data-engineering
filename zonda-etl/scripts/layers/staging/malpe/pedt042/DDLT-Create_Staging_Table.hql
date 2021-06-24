CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt042(
pecdgent string COMMENT 'codigo de la entidad',
penumcon string COMMENT 'numero de contrato de ccc',
pecodofi string COMMENT 'codigo de oficina/sucursal de ccc',
pecodent string COMMENT 'codigo de entidad ccc',
pecodpro string COMMENT 'codigo de producto',
pecodsub string COMMENT 'codigo de subproducto',
pecodmon string COMMENT 'codigo de moneda',
pesucope string COMMENT 'sucursal de operacion',
peofiope string COMMENT 'oficial de operacion',
pecanven string COMMENT 'canal de venta',
peofiven string COMMENT 'oficial de venta',
peoficom string COMMENT 'oficial comercial',
pesdoant string COMMENT 'saldo al dia anterior',
pesdoant_sgn string COMMENT 'signo',
pesdopro string COMMENT 'saldo promedio del mes anterior',
pesdopro_sgn string COMMENT 'signo',
pefecini string comment 'fecha de inicio de la operacion',
pefecter string comment 'fecha de termino de la operacion',
peestope string COMMENT 'estado de la operacion',
pefecest string comment 'fecha del estado',
pemotest string COMMENT 'motivo del estado',
pesubest string COMMENT 'subestado',
perelban string COMMENT 'numero de relacion bancaria',
pepaqper string COMMENT 'ccc del paquete al que pertenece',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha de alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modificacion'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt042';