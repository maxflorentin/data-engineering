create external table if not exists bi_corp_bdr.tactico_mitre_clientes(
----Clientes Bis----
empresa_CB  string,
idnumcli_CB string,
tip_pers_CB string,
apnomper_CB string,
carter_CB   string,
id_pais_CB  string,
cod_sect_CB string,
cod_sec2_CB string,
cod_sec3_CB string,
clisegm_CB  string,
tipsegl1_CB string,
tipsegl2_CB string,
fecha_desde string,
fecha_hasta string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tactico_mitre/tactico_mitre_clientes';