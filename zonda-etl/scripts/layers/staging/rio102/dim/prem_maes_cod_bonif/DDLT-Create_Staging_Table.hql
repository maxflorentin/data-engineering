CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_maes_cod_bonif(
        codigo_bono string,
        id_premiacion string,
        nombre string,
        fecha_desde string,
        fecha_hasta string,
        monto_tope string,
        porc_visa string,
        porc_amex string,
        productos string,
        id_solicitud string,
        desmarcacion string,
        suscriptores string,
        monto_desde string,
        monto_hasta string,
        plan string,
        porc_bonif string,
        id_secuencia string,
        cuota_1 string,
        cuota_2 string,
        cuota_3 string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '/${DATA_LAKE_SERVER}santander/bi-corp/staging/rio102/dim/rio102_prem_maes_cod_bonif';