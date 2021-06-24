CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_chq_rechazadosbcra(
cod_chq_nrocheque bigint,
dt_chq_fecharechazo string,
dt_chq_fechalevantamiento string,
ds_chq_pagomulta string,
ds_chq_cuit string,
cod_chq_judicial string,
ds_chq_judicial string,
cod_chq_revision string,
ds_chq_revision string,
cod_chq_causal string,
ds_chq_causal string,
ds_chq_cuitrelacionado string,
fc_chq_monto decimal(15,2))
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cheques/stk_chq_rechazadosbcra'