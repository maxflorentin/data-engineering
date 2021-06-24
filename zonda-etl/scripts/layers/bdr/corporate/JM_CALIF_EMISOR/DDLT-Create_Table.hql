create external table if not exists bi_corp_bdr.jm_calif_emisor (
e0664_s1emp  string,
e0664_id_pais string,
e0664_idnumcli string,
e0664_tipmodel string,
e0664_fecini string,
e0664_cod_agen string,
e0664_fechasta string,
e0664_califmae string,
e0664_indicadr string,
e0664_fecultmo string,
e0664_feccali string,
e0664_outlook string
)
partitioned by (`partition_date` string)
row format serde 'org.apache.hadoop.hive.ql.io.parquet.serde.parquethiveserde'
stored as inputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat'
outputformat 'org.apache.hadoop.hive.ql.io.parquet.mapredparquetoutputformat'
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/jm_calif_emisor';
