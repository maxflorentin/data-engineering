CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtcom(
com_concepto STRING,
  com_comision STRING,
  com_fecha_desde STRING,
  com_fecha_hasta STRING,
  com_estado STRING,
  com_fecha_estado STRING,
  com_period_com STRING,
  com_divisa STRING,
  com_period_cobr STRING,
  com_cpo_libre INT,
  com_comi_im DECIMAL(17,4),
  com_comi_po DECIMAL(8,5),
  com_comi_min DECIMAL(17,4),
  com_comi_max DECIMAL(17,4),
  com_dia_nat_cobr INT,
  com_ind_tramos STRING,
  com_ind_bof STRING,
  com_dias_calc_prop INT,
  com_entidad_umo STRING,
  com_centro_umo STRING,
  com_userid_umo STRING,
  com_netname_umo STRING,
  com_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtcom/consolidated';