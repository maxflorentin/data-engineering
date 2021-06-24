CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_solicitudes(
       id_solicitud string,
        id_premiacion string,
        fecha_alta string,
        fecha_desde string,
        fecha_hasta string,
        periodicidad string,
        origen_datos string,
        id_usuario string,
        tipo_interfaz string,
        codigo_camp string,
        descrip_camp string,
        m_gestion string,
        accion_ra string,
        id_usuario_peticion string,
        fecha_ult_modificacion string,
        f_dia string,
        f_semana string,
        f_dia_ejec string,
        id_usuario_ult_modificacion string,
        sector_auto string,
        sector_ctrl string,
        estado_auto string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_solicitudes';