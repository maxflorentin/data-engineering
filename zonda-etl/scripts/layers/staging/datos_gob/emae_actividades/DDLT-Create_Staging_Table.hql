CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.datos_gob_emae_actividades(
        fecha string,
        agricultura_ganaderia_caza_silvicultura string,
        pesca string,
        explotacion_minas_canteras string,
        industria_manufacturera string,
        electricidad_gas_agua string,
        construccion string,
        comercio_mayorista_minorista_reparaciones string,
        hoteles_restaurantes string,
        transporte_comunicaciones string,
        intermediacion_financiera string,
        actividades_inmobiliarias_empresariales_alquiler string,
        admin_publica_planes_seguridad_social_afiliacion_obligatoria string,
        ensenianza string,
        servicios_sociales_salud string,
        otras_actividades_servicios_comunitarias_sociales_personales string,
        impuestos_netos_subsidios string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/datos_gob/emae_actividades';