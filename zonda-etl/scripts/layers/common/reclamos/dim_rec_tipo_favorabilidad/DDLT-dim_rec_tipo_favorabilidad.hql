CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_tipo_favorabilidad (
	cod_rec_entidad string,
    cod_rec_tipo_favorabilidad int,
    ds_rec_tipo_favorabilidad string,
    cod_rec_est_tipo_favorabilidad string,
    cod_rec_usuario_alta_tipo_favorabilidad string,
    ts_rec_alta_tipo_favorabilidad timestamp,
    cod_rec_usuario_modif_tipo_favorabilidad string,
    ts_rec_modif_tipo_favorabilidad timestamp,
    cod_rec_sector_owner string,
	partition_date string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_tipo_favorabilidad';