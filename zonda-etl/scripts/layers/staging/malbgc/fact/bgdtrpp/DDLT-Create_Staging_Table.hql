CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtrpp(
entidad			STRING,
centro_alta		STRING,
paquete			STRING,
entidad2		STRING,
centro_alta2	STRING,
cuenta			STRING,
producto		STRING,
subprodu		STRING,
divisa1			STRING,
divisa2			STRING,
ind_cta_piv		STRING,
estarel			STRING,
entidad_umo		STRING,
centro_umo		STRING,
userid_umo		STRING,
netname_umo		STRING,
timest_umo		STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtrpp/consolidated';