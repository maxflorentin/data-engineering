  CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.risksql5_carteras_basicas
     (
		codigo string,
        nombre string,
        segmento string,
        renta string,
        producto string
        )

  PARTITIONED BY (
                  fecha_informacion string)
        ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION
        '${DATA_LAKE_SERVER}/santander/bi-corp/staging/SQL5/dim/risksql5_carteras_basicas'