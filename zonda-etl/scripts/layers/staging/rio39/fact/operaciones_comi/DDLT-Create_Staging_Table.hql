CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_operaciones_comi(
      cod_ops string,
     secuencia string,
     comnum string,
     carga_fecha string,
     estado string,
     estado_tmp string,
     cuit string,
     com_tipo string,
     monto_oper string,
     cotizacion string,
     usuario string,
     cuenta string,
     monto_comi_usd string,
     monto_comi_ars string,
     swift_usd string,
     swift_ars string,
     idboleto string,
     excepto_our string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/operaciones_comi';