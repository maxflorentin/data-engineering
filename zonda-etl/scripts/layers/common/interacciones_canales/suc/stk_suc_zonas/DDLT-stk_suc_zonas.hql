CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_suc_zonas (
cod_suc_zona string,
ds_suc_zona string,
ds_suc_gerente string,
ds_suc_estado string,
cod_suc_sucursal int,
dt_suc_fechaalta string,
dt_suc_fechamodif string,
dt_suc_fechabaja string
)
PARTITIONED BY ( partition_date string) 
STORED AS PARQUET 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/sucursal/fact/stk_suc_zonas';
