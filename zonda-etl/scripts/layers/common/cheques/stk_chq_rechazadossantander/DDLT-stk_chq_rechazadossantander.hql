CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_chq_rechazadossantander (
cod_chq_entidad string,
cod_chq_nrocheque bigint,
cod_chq_cuentacheque bigint,
cod_chq_sucursalgirada bigint,
cod_chq_codigopostal int,
ds_chq_bancoemisor string,
cod_suc_sucacred bigint,
cod_chq_ctaacred bigint,
cod_suc_sucgirada bigint,
cod_chq_ctagirada bigint,
cod_chq_divisa string,
ds_chq_estado string,
dt_chq_fechacompensacion string,
cod_per_nupgirada bigint,
cod_per_nupacred bigint,
cod_chq_segmento string,
cod_chq_agrupacionsegmento string,
cod_chq_subsegmento string,
cod_chq_clasesegmento string,
fc_chq_monto decimal(15,2),
cod_chq_productocontab string,
cod_chq_tipocanje string,
cod_chq_producto string,
cod_chq_subproducto string,
cod_suc_sucursaltrx string,
cod_chq_canaltrx string,
ds_chq_tipocheque string,
dt_chq_fechapresentacion string,
dt_chq_fechadeposito string,
ds_chq_motivorechazoppal string,
ds_chq_motivorechazodepo string,
ds_chq_cmc7 string)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cheques/stk_chq_rechazadossantander'