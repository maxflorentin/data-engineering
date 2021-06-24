CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malpe_pedt005(
pecdgent string COMMENT 'codigo de la entidad',
penumper string COMMENT 'numero del cliente',
pecotbol string COMMENT 'cotiza en bolsa',
petipent string COMMENT 'tipo de entidad comercial /no comercial',
pecanper string COMMENT 'cantidad de personas',
pecanper_sgn string COMMENT 'signo',
petiprel string COMMENT 'tipo de relacion del cliente con el banco',
pefecdis string COMMENT 'fecha de disolucion',
pefecini string COMMENT 'fecha de inicio de la sociedad',
peartemp string COMMENT 'art. correspondiente a la empresa',
peartemp_sgn string COMMENT 'signo',
pepladur string COMMENT 'plazo de duracion de la sociedad',
pepladur_sgn string COMMENT 'signo',
pefirsol string COMMENT 'firma de solicitud',
pefeccdi string COMMENT 'fecha cambio de directorio',
pefecvdi string COMMENT 'fecha vencimiento de directorio',
pefecins string COMMENT 'fecha de inscripcion',
penomnot string COMMENT 'nombre del notario que registra la sociedad. para pj en chile.',
penumreg string COMMENT 'numero registro de inscripcion',
petomins string COMMENT 'tomo de inscripcion en el registro',
pefolins string COMMENT 'folio de inscripcion  en  el  registro',
pejurins string COMMENT 'jurisdiccion de inscripcion en el registro',
pejurins_sgn string COMMENT 'signo',
peproemp string COMMENT 'propiedad ( nacional/ multinacional)',
peramemp string COMMENT 'ramo de la empresa',
pefecmcc string COMMENT 'ult modificacion de la inscripcion en el registro de camara de comercio',
pefacanu string COMMENT 'facturacion anual',
pefacanu_sgn string COMMENT 'signo',
pepatnet string COMMENT 'patrimonio neto',
pepatnet_sgn string COMMENT 'signo',
pecodmon string COMMENT 'codigo de moneda',
peusualt string COMMENT 'usuario de alta',
pefecalt string COMMENT 'fecha alta del registro',
peusumod string COMMENT 'usuario de ultima modificacion',
petermod string COMMENT 'numero de terminal ultima modificacion',
pesucmod string COMMENT 'sucursal ultima modificacion del registro',
pehstamp string COMMENT 'timestamp ultima modif del registro',
peobjsoc_len string COMMENT 'objeto social',
peobjsoc_text string COMMENT 'objeto social',
pefecres string COMMENT '',
pefecves string COMMENT '',
pefecbal string COMMENT '',
pefecrba string COMMENT '',
pefecvba string COMMENT '',
pefecrfa string COMMENT ''
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malpe/fact/pedt005';