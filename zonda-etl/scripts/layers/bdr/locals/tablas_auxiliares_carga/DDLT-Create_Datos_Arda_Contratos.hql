create external table if not exists bi_corp_bdr.datos_arda_contratos(
num_persona string,
id_cto_bdr string,
id_cto_contratos_sin_divisa string,
cod_centro string,
cod_producto string,
cod_subprodu string,
cod_divisa string,
fec_alta_cto string,
fec_vencimiento string,
cod_tip_tasa string,
cod_reesctr string,
limite_credito STRING,
deuda STRING,
disponible STRING
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/datos_arda_contratos';