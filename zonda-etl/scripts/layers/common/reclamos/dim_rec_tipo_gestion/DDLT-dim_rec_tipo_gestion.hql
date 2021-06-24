CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_tipo_gestion (
	cod_rec_entidad STRING ,
	cod_rec_tipo_gestion INT ,
	ds_rec_tipo_gestion STRING ,
	ds_rec_detalle_tipo_gestion STRING ,
	ds_rec_estado_tipo_gestion STRING ,
	cod_rec_tipo_gestion_cosmos STRING ,
	ds_rec_tipo_gestion_cosmos STRING ,
	flag_rec_informa_bcra int ,
	ds_rec_prefijo_ticket STRING ,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_tipo_gestion';