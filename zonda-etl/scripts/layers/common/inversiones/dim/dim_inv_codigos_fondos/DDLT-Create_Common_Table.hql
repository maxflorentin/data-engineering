CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_inv_codigos_fondos (
cod_inv_id_sociedad_gerente int,
cod_inv_fondo_gerente string,
cod_inv_fondo_clase string,
cod_inv_fondo_depositaria string,
ds_inv_fondo string,
cod_inv_moneda string,
cod_inv_isin string,
cod_inv_cnv string,
cod_inv_custodia string,
cod_inv_formato_fondo string,
int_inv_carencia int,
cod_inv_tipo_fondo int,
ds_inv_rendimiento string,
ds_inv_reglamento string,
cod_inv_estado int,
flag_inv_habilitado int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/inversiones/dim/dim_inv_codigos_fondos'