CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.agg_comisiones_mantenimiento (
    cod_cuenta STRING,
    cod_sucursal INT,
    ds_sucursal STRING,
    cod_zona INT,
    cod_nup INT,
    ds_segmento STRING,
    ds_subsegmento STRING,
    flag_mipyme INT,
    flag_cuentablanca INT,
    flag_vip INT,
    flag_iu INT,
    ds_audiencia STRING,
    cod_concepto STRING,
    cod_divisa STRING,
    cod_producto STRING,
    ds_producto STRING,
    cod_subproducto STRING,
    ds_subproducto STRING,
    cod_plan STRING,
    cod_campania STRING,
    flag_campania INT,
    ds_mes_bonificacion STRING,
    cod_num_convenio INT,
    fc_comision decimal(18,2),
    fc_comision_cap decimal(18,2),
    dt_fecha_proliq STRING
    )
PARTITIONED BY(partition_date string)
STORED AS PARQUET
LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/business/cuentas/agg_comisiones_mantenimiento'