create external table if not exists bi_corp_nuevo_cargabal.master_spanish_corporate_income_tax (
id string,
description string,
descripcion string
)
stored as parquet
location '${DATA_LAKE_SERVER}/santander/bi-corp/nuevo_cargabal/views/master_spanish_corporate_income_tax';