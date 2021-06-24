CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_histclientessesion(
SESION                         STRING,
OPERADORA                         STRING,
CLAVE                         STRING,
TIPODOC                         STRING,
DOCUMENTO                         STRING,
RAZONSOCIAL                         STRING,
NACIMIENTO                         STRING,
DOMICILIOPART                         STRING,
CANTPRODUCTOS                         STRING,
TIPOPERSONA                         STRING,
NROUNICOPERSONA                         STRING,
CANTFAXPENDIENTES                         STRING,
FIRMANTESESION                         STRING,
IDFIRMANTE                         STRING,
PINFIRMANTE                         STRING,
INDSINONIMO                         STRING,
MDLWOK                         STRING,
NODOARBOLIVR                         STRING,
FECHA                         STRING,
NOMBREALTAIR                         STRING,
PRIMERAPELLIDO                         STRING,
SEGUNDOAPELLIDO                         STRING,
NROUNICOPERSONAALTAIR                         STRING,
IDRACF                         STRING,
PWDRACF                         STRING,
NOCTURNO                         STRING,
TIPOPUBLICIDAD                         STRING,
MARCAIP                         STRING,
MARCAANPH                         STRING,
SEMAFOROFACTURACION                         STRING,
SEMAFORORENTABILIDAD                         STRING,
SEMAFORORIESGO                         STRING,
MARCACV                         STRING,
IDCONTACTO                         STRING,
IDSERVICIO                         STRING,
IDCONVERSACION                         STRING,
CATCLIENTE                         STRING,
ESMONOPRODUCTO                         STRING,
TIPOCLAVE                         STRING,
MORA                         STRING,
HORARIOMORA                         STRING
)

PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/histclientessesion';