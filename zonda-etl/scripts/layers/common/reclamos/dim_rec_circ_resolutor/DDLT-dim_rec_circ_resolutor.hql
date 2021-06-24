CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_circ_resolutor (
	cod_rec_entidad string ,
	cod_rec_circuito int ,
	cod_rec_sector string ,
	cod_rec_orden_circuito int,
	cod_rec_prior_circuito int,
	flag_rec_obl_resp int,
	cod_rec_temp_circuito_resol int,
	cod_rec_estado_circuito_resol int,
	cod_rec_usuario_alta_circ_resol string,
	ts_rec_alta_circ_resol timestamp,
	cod_rec_usuario_modif_circ_resol string,
	ts_rec_modif_circ_resol timestamp,
	partition_date string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_circ_resolutor';