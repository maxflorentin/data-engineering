CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.aqua_clientes_asoc_geconomicos(
         unidad_operativa string ,
        nombre_unidad_operativa_kgl string ,
        país_unidad_operativa_kgl string ,
        entidad_legal string ,
        tipo_persona string ,
        nombre_legal_kgl string ,
        tipo_identificador string ,
        cif string ,
        tipo_cartera string ,
        flag_garante string ,
        garante string ,
        fecha_información string ,
        facturación string ,
        ingresos_de_intereses string ,
        otros_ingresos string ,
        primas_netas string ,
        margen_financiero string ,
        patrimonio_neto string ,
        total_activos string ,
        divisa string ,
        unidad_monetaria string ,
        auditores string ,
        número_empleados string ,
        glcs_grupo string ,
        rating_interno string ,
        fecha_rating_interno string ,
        estado_feve string ,
        fecha_estado_feve string ,
        modelo_rating_interno string ,
        versión_modelo string ,
        tipo_entidad string ,
        tipo_entidad_financiera string ,
        sector_bbg string ,
        sector_interno_1 string ,
        sector_bankscope string ,
        prioridad string ,
        rating_regulatorio string ,
tipo_de_project_finace string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/aqua/clientes_asoc_geconomicos';