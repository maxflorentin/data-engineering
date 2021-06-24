create external table if not exists bi_corp_bdr.tactico_mitre_intervinientes_contratos(
contrato    string,
----Intervinientes Contratos----
empresa_IC  string,
tipintev_IC string, 
tipintv2_IC string, 
numordin_IC string,
idnumcli_IC string,
----Validez----
fecha_desde string,
fecha_hasta string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tactico_mitre/tactico_mitre_intervinientes_contratos';