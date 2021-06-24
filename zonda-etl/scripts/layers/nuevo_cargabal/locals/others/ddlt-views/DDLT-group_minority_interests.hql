create external table if not exists bi_corp_nuevo_cargabal.master_group_minority_interests (
id string,
description string,
descripcion string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/nuevo_cargabal/views/master_group_minority_interests';