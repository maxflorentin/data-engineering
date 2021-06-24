CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.upgrade_tarjetas (
nup string,
old_entidad string,
old_centro string,
old_cuenta string,
old_producto string,
old_subprodu string,
new_entidad string,
new_centro string,
new_cuenta string,
new_producto string,
new_subprodu string,
imp_reest string,
divisa string,
fec_renum string
)
PARTITIONED BY (partition_date string)
STORED AS parquet
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/upgrade_tarjetas'