CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio130_txn_acum_loy(
        num_cuo1 string,
        plan_cuo1 string,
        rubro1 string,
        puntos_acumulados string,
        puntos_canjeados string,
        numero string,
        subtipo string,
        tipo string,
        programa string,
        producto string,
        estado string,
        monto string,
        miembro string,
        apellido string,
        nombre string,
        tipo_tarjeta string,
        num_trj string,
        comercio string,
        x_commerce_city string,
        x_commerce_code string,
        x_commer_sector string,
        codigo_moneda string,
        fecha_de_cierre string,
        fecha_de_consumo string,
        fecha_de_proceso string,
        estado_delivery string,
        estado_prev_delivery string,
        fecha_estado_delivery string,
        motivo_cance_canje string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio130/fact/txn_acum_loy';