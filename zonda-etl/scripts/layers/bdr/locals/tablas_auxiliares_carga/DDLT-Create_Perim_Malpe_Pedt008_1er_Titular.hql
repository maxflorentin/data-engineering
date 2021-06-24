create external table if not exists bi_corp_bdr.perim_malpe_pedt008_1er_titular(
pecodpro string,
pecodsub string,
pecodent string,
pecodofi string,
penumcon string,
est_relacion string,
fec_baja string,
cal_participacion string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_malpe_pedt008_1er_titular';