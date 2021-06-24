CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_discrepanciasasdf (
  int_cys_ejecucion int,
  dt_cys_periodo int,
  cod_per_nup int,
  ds_per_numdoc bigint,
  ds_cys_razonsocial string,
  ds_cys_clasificacionedfsindiscrepancia string,
  ds_cys_clasificaciondiscrepancia string,
  ds_clasificacionedf string,
  fc_cys_deudatotal decimal(17,4),
  fc_cys_previsiontotalsindiscrepancia decimal(17,4),
  fc_previsiontotal decimal(17,4)
)
PARTITIONED BY(partition_date string)
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_cys_discrepanciasasdf' ;