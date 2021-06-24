CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.campanias_responsys_riid_hist(
ID_COMPUESTO            string,
PERSONA_ID              string,
RIID                    string,
PENUMPER                string

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/campanias/fact/responsys_riid_hist';