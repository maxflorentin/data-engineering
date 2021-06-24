CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_casuistica_motivos (
	cod_rec_casuistica int,
	cod_rec_motivo int,
	ds_rec_motivo string,
	flag_rec_casuistica int,
	cod_rec_usuario_alta string,
	ts_rec_alta string,
	cod_rec_usuario_modif string,
	ts_rec_modif string,
	ts_rec_baja string,
	cod_rec_idmotivo int,
	cod_rec_tipo_gestion int,
	partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_casuistica_motivos';