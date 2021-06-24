CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.watson_chat (
    canal string,
    jsessionid string,
    fechahora timestamp,
    emisortipo string,
    emisornomrbre string,
    message string,
    intent string,
    confidence string,
    ask_answer_was_useful string,
    possiblequestions string,
    suggestion_topics string,
    options string,
    nupcliente string,
    idgenesys string,
    transferidoahumano string,
    fuepossiblequestions string,
    fuesuggestion_topics string,
    fueoptions string
)
PARTITIONED BY(partition_date string COMMENT 'Partition date in format yyyy-mm-dd')
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/watson/fact/chat';
