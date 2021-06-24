CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.control_ars (
     sucursal_radicacion  string,
        cuenta  string,
        cod_op  string,
        importe  string,
       --  partition_date  string,
        sucursal_operacion  string,
        extraction_date  string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/control_ars';