CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.garra_contratos_ref(
086_cod_entidad STRING,
086_cod_centro STRING ,
086_num_contrato STRING,
086_cod_producto STRING,
086_cod_subprodu STRING,
086_cod_divisa STRING,
086_cod_centrod STRING,
086_num_contratd STRING,
086_cod_productd STRING,
086_cod_subprodd STRING,
086_cod_divisad STRING,
086_imp_refnacdo decimal(13,4),
086_imp_refnacdl decimal(13,4),
086_imp_cbiomorg decimal(13,4),
086_fec_refinanc STRING,
086_cod_reesctr STRING,
086_ind_antconint STRING,
086_imp_anticipo decimal(11,2),
086_cod_marca_seg_esp STRING,
086_cod_marca STRING,
086_cod_submarca STRING,
086_fec_primer_imp STRING,
086_num_persona STRING,
086_ws_marca_reestruc STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garra/fact/contratos_ref/consolidated';
