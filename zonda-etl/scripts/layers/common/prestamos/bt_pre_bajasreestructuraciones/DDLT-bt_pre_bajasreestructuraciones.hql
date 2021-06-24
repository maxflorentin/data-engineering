CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_pre_bajasreestructuraciones (
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
dt_pre_fechaaltaproducto string,
dt_pre_fechabaja string,
ds_pre_tiporeestructuracion string,
flag_pre_mora int,
flag_pre_vigilanciaespecial int,
ds_pre_subestandar string,
ds_pre_estadogestion string,
cod_pre_segmento string,
ds_pre_segmento string,
ds_pre_renta string,
ds_pre_categoriaproducto string,
flag_pre_normalizado int,
ds_pre_bucket string,
cod_pre_tipoclasificacion string,
ds_pre_tipoclasificacion string,
ds_pre_tipomovimiento string,
ds_pre_origendereestructuracion string,
fc_pre_reestructuracionsucesiva int,
fc_pre_diasatraso int,
fc_pre_importeriesgo decimal(15,2),
fc_pre_importedispuesto decimal(15,2),
fc_pre_porcentajeprestamohipotecario decimal(15,2),
fc_pre_porcentajeprestamoprendario decimal(15,2),
fc_pre_porcentajeprestamopersonales decimal(15,2),
fc_pre_porcentajetarjetacredito decimal(15,2),
fc_pre_porcentajecuentacorriente decimal(15,2),
fc_pre_porcentajeotroproducto decimal(15,2),
fc_pre_porcentajedeudapendientepago decimal(15,2),
fc_pre_porcentajedeudapagada decimal(15,2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/bt_pre_bajasreestructuraciones';
