CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio35_nrm_normal_telefonos(
ID                            STRING,
INTER_CIF_ID                  STRING,
PREFIJO_INTERURBANO           STRING,
TELEFONO                      STRING,
XPREFIJO_INTERNACIONAL        STRING,
XPREFIJO_INTERURBANO          STRING,
XNUMERO_TELEFONO              STRING,
ULTIMO                        STRING,
VUELTAS                       STRING,
EXISTE_GUIA                   STRING,
XFECHA_BAJA                   STRING,
MODIFICADO                    STRING,
XTIPO_TELEFONO                STRING,
XLOCALIDAD                    STRING,
XCODIGO_POSTAL                STRING,
XAPELLIDO                     STRING,
EXISTE_PERSONAGUIA            STRING,
INSERTO_TELEFONO              STRING,
CARACTERISTICA                STRING,
NORMALIZADO                   STRING,
STATUS                        STRING,
XNUMERO_PUERTA                STRING,
XPISO                         STRING,
XDEPTO                        STRING,
BUSQUEDA_GUIA                 STRING,
ORD_TEL                       STRING,
ERRONEO                       STRING,
ORIGEN                        STRING,
PESECTEL                      STRING,
FALTA                         STRING,
FMODIFICACION                 STRING,
FILLER1                       STRING,
FILLER2                       STRING,
COMPANIA                      STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio35/dim/nrm_normal_telefonos';