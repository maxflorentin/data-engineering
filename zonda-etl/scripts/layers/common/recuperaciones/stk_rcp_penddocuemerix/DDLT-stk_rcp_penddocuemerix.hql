CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_penddocuemerix(
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
fs_rcp_riesgototal string,
ds_rcp_contratolargo string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
ds_rcp_estadocontable string,
dt_rcp_fechaingresoescenario string,
ds_rcp_marca string,
ds_rcp_submarca string,
cod_rcp_garantia string,
dt_rcp_fechaprimerimpago string,
fc_rcp_deudatotal decimal(17,4),
cod_rcp_escenario string,
cod_rcp_estado string,
fc_rcp_diasmora int,
cod_rcp_buffete string,
ds_rcp_nombrebuffete string,
dt_rcp_fechainicioestado string,
fc_rcp_diasestado int,
fc_rcp_diasescenario int,
cod_rcp_categoriaproducto string,
ds_rcp_segmento string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_penddocuemerix'
;