CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_chq_pagoatribuciones(
cod_chq_entidad string,
cod_chq_nrocheque bigint,
cod_chq_cuentacheque bigint,
cod_chq_sucursalgirada bigint,
cod_chq_codigopostal int,
cod_suc_sucursal bigint,
cod_chq_producto string,
cod_chq_subproducto string,
cod_per_nup bigint,
cod_chq_cuentacorriente bigint,
cod_chq_divisa string,
ds_chq_nombretitular string,
cod_chq_segmento string,
cod_chq_camara string,
cod_chq_nivelworkflow string,
cod_chq_marcaprcheque string,
cod_chq_marcaprmotivo string,
cod_chq_motivorechazo string,
cod_chq_justificacionpago string,
cod_chq_legajogestionn1 string,
ds_chq_horagestionn1 string,
cod_chq_legajogestionn2 string,
ds_chq_horagestionn2 string,
cod_chq_legajogestionn3 string,
ds_chq_horagestionn3 string,
cod_chq_legajoaprobacion string,
ds_chq_horaaprobacion string,
fc_chq_montoacuerdo decimal(15,2),
fc_chq_montocalificado decimal(15,2),
fc_chq_importe decimal(15,2),
fc_chq_saldodeudor decimal(15,2),
fc_chq_saldodisponible decimal(15,2))
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cheques/stk_chq_pagoatribuciones'