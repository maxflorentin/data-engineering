create external table if not exists bi_corp_bdr.tactico_mitre_rubros(
----Importe Posicion Contrato----
contrato_IPC string,
empresa_IPC  string,
cta_cont_IPC string,
tip_impt_IPC string,
agrctacb_IPC string,
coddiv_IPC   string,
ctacgbal_IPC string,
fecha_desde  string,
fecha_hasta  string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tactico_mitre/tactico_mitre_rubros';