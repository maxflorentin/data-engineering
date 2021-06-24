  CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.risksql5_productos
        (
        codigo_producto string,
         codigo_subproducto string,
         descripcion string,
         categoria_particulares string,
         categoria_carterizados string,
         categoria_cartera string,
         refinanciacion string,
         tipo_reestructuracion string,
         origen_de_la_reestructuracion string,
         fecha_desde string,
         contingente string,
         uva string,
         codigo_producto_mora string,
         codigo_subproducto_mora string,
         tipo_producto string
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
        '${DATA_LAKE_SERVER}/santander/bi-corp/staging/SQL5/dim/risksql5_productos'