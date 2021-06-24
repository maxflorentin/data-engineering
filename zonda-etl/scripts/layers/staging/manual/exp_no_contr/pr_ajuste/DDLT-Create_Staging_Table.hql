CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.manual_pr_ajuste(
        sociedad string,
		ajuste string,
		cargabal string,
		sociedad_eliminacion string,
		moneda_informada string,
		importe string,
		ajuste_fiscal string

)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/exp_no_contr/pr_ajuste';