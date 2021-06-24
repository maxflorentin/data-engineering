CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_motdes_arbol_resultado (
  cod_rec_casuistica INT,
  cod_rec_resultado INT,
  ds_rec_resultado STRING,
  ds_rec_mensaje STRING,
  cod_rec_favorabilidad STRING,
  cod_rec_usuario_alta STRING,
  dt_rec_fecha_alta TIMESTAMP,
  cod_rec_usuario_modif STRING,
  dt_rec_fecha_modif TIMESTAMP,
  dt_rec_fecha_baja TIMESTAMP,
  partition_date STRING
)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_motdes_arbol_resultado';