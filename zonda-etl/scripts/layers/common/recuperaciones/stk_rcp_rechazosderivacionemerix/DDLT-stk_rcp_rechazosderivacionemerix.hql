CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_rechazosderivacionemerix(
ds_rcp_zonasucadm string,
cod_rcp_sucadm bigint,
ds_rcp_sucnombre string,
cod_rcp_nroposicion string,
cod_rcp_tipodocumento string,

ds_rcp_numerodocumento string,
cod_rcp_gestor string,
ds_rcp_nombregestor string,
cod_per_nup bigint,
ds_rcp_apellidocliente string,

ds_rcp_nombrecliente string,
cod_rcp_banca string,
ds_rcp_calificacion string,
dt_rcp_fechaingreso string,
fc_rcp_riesgototal decimal(17,4),

cod_rcp_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,

ds_rcp_contratolargo string,
ds_rcp_estadocontable string,
ds_rcp_marca string,
ds_rcp_submarca string,
cod_rcp_garantia string,

dt_rcp_fechasituacionirregular string,
cod_rcp_estado string,
ds_rcp_estado string,
cod_rcp_escenario string,
ds_rcp_escenario string,

dt_rcp_fechasituacion string,
dt_rcp_fechaprimerimpago string,
fc_rcp_deudatotal decimal(17,4),
fc_rcp_diasmora int,
fc_rcp_diasestado int,

fc_rcp_diasescenario int,
ds_rcp_segmento string,
ds_rcp_paquete string

)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_rechazosderivacionemerix'
;