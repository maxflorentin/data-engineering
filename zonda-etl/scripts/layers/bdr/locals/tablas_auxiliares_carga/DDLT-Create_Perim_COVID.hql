create external table if not exists bi_corp_bdr.perim_contratos_covid(
    entidad string,
    oficina string,
    cuenta string,         
    producto string,
    subpro string, 
    num_propues string,
    cuotas string,
    impconce string,
    feforma string,
    fecha_pago string,
    divisa string,
    Cuenta_Org string,
    tipo string
)
partitioned by (partition_date string)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/perim_contratos_covid';
