CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_origen_interfaz_aux(
        fecha_carga string,
        fecha_aceptacion string,
        millas string,
        sexo string,
        nup_referente string,
        dni string,
        nro_tarjeta string,
        importe string,
        puntos string,
        cod_banco string,
        cuenta_tc string,
        sucursal string,
        id_solicitud string,
        col_05 string,
        col_04 string,
        col_03 string,
        col_02 string,
        col_01 string,
        col_00 string,
        nro_cuenta string,
        nup string,
        id_premiacion string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_origen_interfaz_aux';