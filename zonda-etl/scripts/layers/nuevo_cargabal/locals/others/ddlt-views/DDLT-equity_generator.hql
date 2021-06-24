create external table if not exists bi_corp_nuevo_cargabal.master_equity_generator (
id string,
description string,
descripcion string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/nuevo_cargabal/views/master_equity_generator';