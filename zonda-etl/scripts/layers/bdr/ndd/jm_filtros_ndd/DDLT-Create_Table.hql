CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_filtros_ndd(
feoperac string,
s1emp string,
contra1 string,
cta_cont string,
tip_impt string,
centctbl string,
agrctacb string,
ctacgbal string,
importh string,
filtroex string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/ndd/jm_filtros_ndd';