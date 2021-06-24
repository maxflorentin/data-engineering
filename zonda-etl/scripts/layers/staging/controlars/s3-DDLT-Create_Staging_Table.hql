CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.control_ars (
    sucursal_radicacion  string,
    cuenta  string,
    cod_op  string,
    importe  string,
    sucursal_operacion  string,
    extraction_date  string
)
PARTITIONED BY(partition_date string)
STORED AS PARQUET
LOCATION 's3a://sard1ae1ssszonda0plat001/bi-corp/staging/malbgc/fact/control_ars';