create external table if not exists bi_corp_bdr.tactico_mitre_garantias(
contrato       string,
----Garantias Contrato----
s1emp          string,
tip_gara       string,
biengar1       string,
fecini         string,
fecbaja        string,
fecvcto        string,
cod_gar        string,
cod_gar2       string,
tipo_ins       string,
ind_pign       string,
tip_aval       string,
tip_cobe       string,
est_gara       string,
comf_let       string,
num_aseg       string,
coddiv         string,
idnumcli       string,
indblo         string,
indrgosb       string,
indcobpf       string,
valaseju       string,
ordapgar       string,
rangohip       string,
n_impago       string,
----Validez----
fecha_desde    string,
fecha_hasta    string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/tactico_mitre/tactico_mitre_garantias';