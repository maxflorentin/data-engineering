CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio44_ba_equipos_dispo_atm_men_tb (
    fecha string,
    sigla string,
    n_sucursal string,
    nombre string,
    zona string,
    zonal string,
    marca string,
    operador string,
    descripcion_posicion string,
    calle string,
    numero string,
    localidad string,
    provincia string,
    disponibilidad string,
    cash_out string,
    balanceo string,
    dispensador string,
    hard string,
    suministro string,
    comunicacion string,
    supervisor string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio44/fact/ba_equipos_dispo_atm_men_tb';