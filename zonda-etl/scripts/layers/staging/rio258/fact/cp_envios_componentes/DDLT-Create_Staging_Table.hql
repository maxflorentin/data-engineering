CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_envios_componentes(
        cod_componente string,
        crupier_id string,
        cod_cliente string,
        cod_producto string,
        nro_tarjeta string,
        nro_novedad_amas string,
        nro_secuencia_amas string,
        cod_producto_tarjeta string,
        marca_adicional string,
        -- ultima_modif string,
        cuentamcnro string,
        creado_por string,
        ultima_modif_por string,
        marca_retenido string,
        tarjeta_rechazo_abae string,
        secuencia string,
        cod_distribucion string
      )
    PARTITIONED BY (
      ultima_modif string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_cp_envios_componentes'