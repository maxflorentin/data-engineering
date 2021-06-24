CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_trf_param_conceptos (
	cod_trf_origen                string,
	cod_trf_concepto              string,
    ds_trf_concepto_orig          string,
    ds_trf_concepto_normalizado   string,
    dt_trf_fecha_ult_modif        string
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/transferencias/dim/dim_trf_param_conceptos';
