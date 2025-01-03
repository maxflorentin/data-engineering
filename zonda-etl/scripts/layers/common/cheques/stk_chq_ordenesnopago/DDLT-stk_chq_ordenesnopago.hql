CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_chq_ordenesnopago (
cod_chq_entidad string,
cod_chq_cuentacheque bigint,
cod_chq_nrocheque bigint,
cod_chq_sucursalgirada bigint,
cod_chq_codigopostal int,
cod_chq_secuencia string,
cod_chq_source string,
flag_chq_active int,
dt_chq_fechaalta string,
dt_chq_fechabaja string,
dt_chq_fechaconfirm string,
cod_chq_divisa string,
cod_chq_contonp string,
cod_chq_periodico string,
ds_chq_motivo string,
ds_chq_observaciones string,
ds_chq_denunciante string,
ds_chq_nombrebenef string,
cod_chq_tipoiddenun string,
cod_chq_iddenun string,
ds_chq_horaalta string,
ds_chq_usuarioalta string,
cod_chq_estado string,
cod_chq_tipobaja string,
cod_chq_baja string,
ds_chq_horabaja string,
ds_chq_usuariobaja string,
cod_chq_tipoconf string,
cod_chq_conf string,
ds_chq_usuarioconfir string,
ds_chq_horaconfirm string,
cod_suc_sucursalumo bigint,
cod_chq_userumo string,
ds_chq_netnameumo string,
ts_chq_fechaumo string,
cod_chq_entidadogl string,
cod_suc_sucursalogl bigint,
cod_chq_cuentaogl bigint,
cod_chq_entidadumo string,
cod_chq_divisaogl string,
fc_chq_importe decimal(15,2))
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cheques/stk_chq_ordenesnopago'