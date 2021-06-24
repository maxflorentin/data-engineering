CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt002(
pecdgent string comment 'codigo de la entidad',
penumper string comment 'numero de cliente',
pesecdom string comment 'secuencia de domicilio',
pesecdom_sgn string comment 'signo',
pesectel string comment 'secuencia de telefono',
pesectel_sgn string comment 'signo',
pemaremp string comment 'marca de empleado. ',
penomemp string comment 'nombre de su empresa',
petipact string comment 'tipo de actividad',
peramemp string comment 'ramo de la empresa',
pecaremp string comment 'cargo en la empresa',
pefecing string comment 'fecha de ingreso a la empresa',
pereldep string comment 'marca de relacion de dependencia. (profesional, autonomo, comerciante, empresario, otro.',
perellab string comment 'tipo de relacion laboral ( c/f )',
peempemp string comment 'tipo de empresa empleadora',
pepercar string comment 'personas a cargo',
pepercar_sgn string comment 'signo',
penumhij string comment 'numero de hijos',
penumhij_sgn string comment 'signo',
penommad string comment 'apellidos y nombre de la madre',
penompad string comment 'apellidos y nombre del padre',
peregmat string comment 'regimen matrimonial',
penudcny string comment 'numero de documento del conyuge',
petipcny string comment 'tipo de documento del conyuge',
pefecfal string comment 'fecha de fallecimiento. ',
petiprel string comment 'tipo de relacion del banco con el cliente ',
penivest string comment 'nivel de estudios',
peestrat string comment 'estrato',
pelugnac string comment 'lugar de nacimiento',
peprofes string comment 'profesion',
peulttit string comment 'ultimo titulo adquirido. ',
pecodsec string comment 'codigo de actividad secundaria',
pedomant string comment 'antiguedad en el domicilio anterior',
pedomant_sgn string comment 'signo',
peentpre string comment 'codigo de entidad de prevision',
peusualt string comment 'usuario de alta',
pefecalt string comment 'fecha de alta del registro',
peusumod string comment 'usuario de ultima modificacion',
petermod string comment 'numero de terminal ultima modificacion',
pesucmod string comment 'sucursal ultima modificacion del registro',
pehstamp string comment 'timestamp ultima modificacion del registro'
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt002';