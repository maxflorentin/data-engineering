CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_ptb_pedt008(
pecdgent string comment 'codigo de la entidad',
penumper string comment 'numero de cliente',
penumcon string comment 'numero de contrato de ccc',
pecodofi string comment 'codigo de oficina/sucursal de ccc',
pecodent string comment 'codigo de entidad ccc',
pecalpar string comment 'calidad participacion ccc',
peordpar string comment 'orden de participacion de ccc',
peordpar_sgn string comment 'signo',
pecodpro string comment 'codigo de producto',
pecodsub string comment 'codigo de subproducto',
pefecbrb string comment 'fecha de baja relacion pe-ccc/pe-pe',
peestrel string comment 'estado relacion pe-ccc/ccc',
peresint string comment '% responsabilidad en la intervencion',
peresint_sgn string comment 'signo',
pemarpaq string comment 'marca pertenencia a paquete',
pemotbaj string comment 'motivo de la baja',
petipdoc string comment 'tipo de documento del cliente',
penumdoc string comment 'n de documento del cliente',
peusualt string comment 'usuario de alta',
pefecalt string comment 'fecha de alta del registro',
peusumod string comment 'usuario de ultima modificacion',
petermod string comment 'numero de terminal ultima modificacion',
pesucmod string comment 'sucursal ultima modificacion del registro',
pehstamp string comment 'timestamp ultima modificacion'
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe-post-batch/fact/pedt008';