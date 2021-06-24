CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.wahac690(
    empresa string,
    nucta string,
    cedestin string,
    divisa string,
    saldo_actual string,
    numeral_actual string,
    saldo_anterior string,
    numeral_anterior string,
    saldo_actual_ml string,
    numeral_actual_ml string,
    saldo_anterior_ml string,
    numeral_anterior_ml string)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malha/fact/wahac690';