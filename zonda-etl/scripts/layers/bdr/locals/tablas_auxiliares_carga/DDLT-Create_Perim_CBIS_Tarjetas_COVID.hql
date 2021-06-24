create external table if not exists bi_corp_bdr.perim_contratos_bis_tarjetas_covid(
cred_cod_centro string,
cred_num_cuenta string,
cred_cod_producto string,
cred_cod_subprodu_altair string,
g4095_contra1 string,
g4095_fechaper string,
g4095_feccance string,
g4095_coddiv string,
contr_partition_date string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_contratos_bis_tarjetas_covid';