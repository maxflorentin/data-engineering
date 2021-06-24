CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.hadt040(
    EMPRESA string,
    HAPLAN string,
    NUMCTARE string,
    NUCTA string,
    SIGNO string,
    SOCELIM string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='\;',
  'serialization.format'='\;')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malha/dim/hadt040';