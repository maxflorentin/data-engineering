create external table if not exists bi_corp_bdr.garant_miga(
num_persona  string,
cod_estado  string,
fec_alta  string,
fec_vcto  string,
tip_cobertur  string,
tip_instrumentacion  string,
pignoracion  string,
tip_aval  string,
cod_divisa  string,
imp_limite  string,
num_persona_garante  string,
tip_gara_bdr  string,
cod_garantia_bdr  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/gara_miga';