CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio130_saldos_loy(
        mem_num string,
        programa string,
        fecha_de_vencimiento string,
        puntos_totales string,
        puntos_a_vencer string,
        val_score string,
        fecha_ultima_acc string,
        fecha_ultima_inc string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio130/fact/saldos_loy';