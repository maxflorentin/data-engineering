create external table if not exists bi_corp_bdr.tactico_mitre_contratos(
contrato    string,
----Contratos Bis----
empresa_CB  string,
sucursal_CB string,
id_pais_CB  string,
id_prod_CB  string,
idpro_lc_CB string,
fecvento_CB string,
fecve2_CB   string,
fechaper_CB string,
idfinali_CB string,
coddiv_CB   string,
indlim_CB   string,
----Intervinientes Contratos----
empresa_IC  string,
----Contratos Otros Datos----
empresa_CO  string,
idsubprd_CO string,
gest_sit_CO string,
ges2_sit_CO string,
emprepor_CO string,
finiutct_CO string,
ffinutct_CO string,
----Importe Posicion Contrato----
empresa_IPC string,
----Validez----
fecha_desde string,
fecha_hasta string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tactico_mitre/tactico_mitre_contratos';