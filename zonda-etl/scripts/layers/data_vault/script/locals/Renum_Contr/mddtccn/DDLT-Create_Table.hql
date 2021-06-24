create external table if not exists bi_corp_bdr.mp_02_mddtccn(
cod_entidadg string,
cod_centrog string,
num_contratg string,
cod_productg string,
cod_subprodg string,
cod_entidad string,
cod_centro string,
num_contrato string,
cod_producto string,
cod_subprodu string,
fec_altareg string,
cod_divisa string,
fec_baja string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/dv/mp_02_mddtccn'
;