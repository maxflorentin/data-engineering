CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio147_hb_client_master(
DOCUMENTO                     STRING,
FECHA_DE_NACIMIENTO           STRING,
TIPO_DE_DOCUMENTO             STRING,
NOMBRE                        STRING,
APELLIDO                      STRING,
FECHA_DE_ALTA_INSCRIPCION     STRING,
CANAL_VENTA                   STRING,
FECHA_INICIO_USO              STRING,
SUCURSAL_ORIGEN               STRING,
FECHA_INICIO_HABIL            STRING,
F_ALTA_REGISTRO               STRING,
ACEPTACION_CONTRATO           STRING,
CANAL_ACTIVACION              STRING,
FECHA_ACEPTACION_CONTRATO     STRING,
ACEPTAC_CONTRATO_DDI          STRING,
NUP                           STRING,
ACEPTAC_CONTRATO_PROG         STRING,
CPI_FCI_C                     STRING,
CPI_FCI_T                     STRING,
CPI_BON_C                     STRING,
CPI_ACC_C                     STRING,
ACEPTAC_CONTRATO_VIAJ         STRING,
ACEPTACION_CONTRATO_TAG       STRING,
FECHA_ACEPTACION_CONTRATO_TAG STRING,
ADHESION_BILLETERA_TODOPAGO   STRING,
ACEPTACION_CONTRATO_EXT_ATM   STRING,
ECHEQ                         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio147/fact/hb_client_master';