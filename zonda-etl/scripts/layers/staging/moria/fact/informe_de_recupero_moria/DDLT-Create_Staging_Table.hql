CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.moria_informe_de_recupero(
moec1800_fec-can-oper string,
moec1800_filler_01 string,
moec1800_cod-diviorig string,
moec1800_filler_02 string,
moec1800_imp-totcanco string,
moec1800_filler_03 string,
moec1800_cod-gestor string,
moec1800_filler_04 string,
moec1800_cod-centrop string,
moec1800_filler_05 string,
moec1800_num-pp string,
moec1800_filler_06 string,
moec1800_num-persona string,
moec1800_filler_07 string,
moec1800_nombre-cliente string,
moec1800_filler_08 string,
moec1800_cod-centro string,
moec1800_filler_09 string,
moec1800_num-contrato string,
moec1800_filler_10 string,
moec1800_cod-producto string,
moec1800_filler_11 string,
moec1800_cod-subprodu string,
moec1800_filler_12 string,
moec1800_cod-divisa string,
moec1800_filler_13 string,
moec1800_num-bufete string,
moec1800_filler_14 string,
moec1800_cod-centro-pp string,
moec1800_filler_15 string,
moec1800_imp-totpp string,
moec1800_filler_16 string,
moec1800_imp-cuota0 string,
moec1800_filler_17 string,
moec1800_fec-pago string,
moec1800_filler_18 string,
moec1800_cod-entidadr string,
moec1800_filler_19 string,
moec1800_cod-centror string,
moec1800_filler_20 string,
moec1800_num-contratr string,
moec1800_filler_21 string,
moec1800_cod-productr string,
moec1800_filler_22 string,
moec1800_cod-subprodr string,
moec1800_filler_23 string,
moec1800_cod-estado string,
moec1800_filler_24 string,
moec1800_cod-estado-md string,
moec1800_filler_25 string,
moec1800_fec-reconoci string,
moec1800_filler_26 string,
moec1800_fec-vtocuo0 string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/moria/fact/moria_informe_de_recupero'