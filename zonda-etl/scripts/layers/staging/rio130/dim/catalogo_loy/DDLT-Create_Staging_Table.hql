CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio130_catalogo_loy(
        id_catalogo  string,
        cod_catalogo  string,
        catalogo  string,
        partnum  string,
        producto  string,
        descripcion  string,
        id_categoria  string,
        linea_prodcuto  string,
        categoria  string,
        id_producto  string,
        puntos  string,
        linea_producto  string,
        costo_punto  string,
        nombre_categoria  string,
        nombre_visible_cat  string,
        fecha_vig_ini_prod  string,
        fecha_vig_fin_prod  string,
        medio_de_pago  string,
        producto_agrupador  string,
        porcentaje_desc  string,
        fecha_show  string,
        ubicacion_show  string,
        origen  string,
        stock_inicial string,
        flujo_canje  string,
        id_prod_par  string,
        id_cate_par  string,
        tipo_prod_loy string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio130/dim/rio130_catalogo_loy';