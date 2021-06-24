CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_contr_ndd(
feoperac string,
s1emp string,
contra1 string,
idnumcli string,
nivelapl string,
idumbral string,
salonbal string,
umbrabs string,
umbrrel string,
umbrpul string,
diaprper string,
flgperim string,
procecto string,
umbpuls3 string,
flgpers3 string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/ndd/jm_contr_ndd';