CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_trf_canales (
    cod_trf_canal           string,
	ds_trf_canal            string,
	dt_trf_fecha_ult_modif  string,
	ds_trf_canal_unificado  string,
	cod_trf_origen          string
 )
PARTITIONED BY (
    partition_date          string)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/transferencias/dim/dim_trf_canales';
