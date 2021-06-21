CREATE EXTERNAL TABLE bi_corp_common.dim_rec_usuario (
  cod_rec_usuario STRING,
  ds_rec_usuario_apellido STRING,
  ds_rec_usuario_nombre STRING,
  ds_rec_usuario_email STRING,
  cod_rec_guid_tibco STRING,
  cod_usuario_alta STRING,
  dt_rec_usuario_alta TIMESTAMP,
  dt_rec_usuario_baja TIMESTAMP,
  cod_usuario_modif STRING,
  dt_rec_usuario_modif TIMESTAMP,
  cod_suc_sucursal INT,
  cod_rec_oficina STRING,
  partition_date STRING
)
STORED AS PARQUET
LOCATION 'santander/bi-corp/common/reclamos/dim_rec_usuario'