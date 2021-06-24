CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_soc_entbdr(
r3903_s1emp string,
r3903_entcgbal string,
r3903_descsoci string,
r3903_coddiv string,
r3903_fecdesde string,
r3903_fechasta string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/output_bdr/jm_soc_entbdr';