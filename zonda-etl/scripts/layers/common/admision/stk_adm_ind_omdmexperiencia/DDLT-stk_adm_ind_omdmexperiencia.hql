CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmexperiencia (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
ds_adm_variable string,
int_adm_cantadelantosenefectivodolares int,
int_adm_cantadelantosenefectivopesos int,
fc_adm_monadelantosenefectivodolares double,
fc_adm_monadelantosenefectivopesos double,
ds_adm_rentacliente string,
ds_adm_tipocliente string,
ds_adm_tipopaqueteposee string,
int_adm_cantcheqrechsinfondonoresc int,
int_adm_cantcheqsinfondoresc int,
fc_adm_imporcheqsinfondonoresc double,
fc_adm_imporcheqsinfondoresc double,
int_adm_cantatr30ult12 int,
int_adm_cantatr60ult12 int,
int_adm_cantatr90ult12 int,
int_adm_cantatrm90ult12 int,
dt_adm_fechaalta string,
fc_adm_limitedeacuerdo double,
fc_adm_mayormontodelexceso double,
fc_adm_montodelexceso double,
int_adm_plazomora int,
ds_adm_tipocuenta string,
cod_adm_in00causal1 string,
dt_adm_in00fechainicio1 string,
dt_adm_in00fechavto1 string,
int_adm_cantdiasdeatrasos int,
flag_adm_espreacordadomonoproducto string,
dt_adm_fechabaja string,
fc_adm_importeultimacuota double,
cod_adm_modalidad string,
fc_adm_saldoimpago double,
cod_adm_tipomonoproducto string,
cod_adm_tipoprestamo string,
cod_adm_atrasos int,
cod_adm_estado string,
dt_adm_fechaboletin string,
ds_adm_marca string,
fc_adm_montoultimopagominimo double,
fc_adm_montoultimoresumen double,
cod_adm_motivobaja string,
ds_adm_tipotarjeta string,
cod_adm_marcabloqueoseguimiento string,
flag_adm_marcacuotaphonemoroso string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/admision/stk_adm_ind_omdmexperiencia'
;