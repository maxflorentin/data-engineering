create external table if not exists bi_corp_nuevo_cargabal.master_classified_as_held_for_sale (
id string,
description string,
descripcion string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/nuevo_cargabal/views/master_classified_as_held_for_sale';