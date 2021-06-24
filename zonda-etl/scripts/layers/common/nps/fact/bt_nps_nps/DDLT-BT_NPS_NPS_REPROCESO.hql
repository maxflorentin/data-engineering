CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_nps_nps(
    cod_per_nup STRING,
    cod_nps_respuesta STRING,
    cod_per_nro_doc STRING,
    cod_nps_nro_ubicacion STRING,
    ds_nps_agrupados STRING,
    ds_nps_canal STRING,
    ds_nps_descripcion STRING,
    ds_nps_desc_ubicacion STRING,
    ds_per_edad STRING,
    ds_nps_ejecutivo STRING,
    ds_nps_legajo STRING,
    ds_nps_mejor_prod STRING,
    ds_nps_momento_critico STRING,
    ds_nps_negocio STRING,
    ds_per_nombre STRING,
    ds_nps_nombre_sucursal STRING,
    ds_nps_renta STRING,
    ds_nps_rentabilidad DECIMAL(12,2),
    ds_per_segmento STRING,
    ds_per_sexo STRING,
    ds_per_tipo_doc STRING,
    ds_nps_tipo_encuesta STRING,
    dt_nps_fecha_encuesta STRING,
    ds_nps_antiguedad_meses INT,
    ds_nps_orden INT,
    ds_nps_pregunta STRING,
    ds_nps_respuesta STRING
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/common/nps/fact/bt_nps_nps'