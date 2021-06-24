CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.aqua_grupos_economicos(
        entidad_legal string ,
        nombre_largo string ,
        fecha_informacion string ,
        facturacion string ,
        ingresos_de_intereses string ,
        otros_ingresos string ,
        primas_netas string ,
        margen_financiero string ,
        patrimonio_neto string ,
        total_activos string ,
        divisa string ,
        unidad_monetaria string ,
        auditores string ,
        numero_empleados string ,
        prioridad string ,
        sector_aqua string ,
        codigo_iso_del_pais string ,
        codigo_kgr_de_la_matriz string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/aqua/grupos_economicos';