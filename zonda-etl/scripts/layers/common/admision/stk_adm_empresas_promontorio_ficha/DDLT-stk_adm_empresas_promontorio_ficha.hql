CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_promontorio_ficha (
id_adm_fichapromontorio bigint,
id_adm_promontorioestadoempresa bigint,
id_adm_promontorioestado bigint,
cod_per_nup bigint,
fc_adm_valorrating decimal(16,2),
ds_adm_tiporating string,
ds_adm_razonsocial string,
ds_adm_feve string,
dec_adm_razonsociallimite decimal(16,2),
dec_adm_razonsocialdispuesto decimal(16,2),
dec_adm_razonsocialgarreal decimal(16,2),
fc_adm_totallimite decimal(16,2),
fc_adm_totaldispuesto decimal(16,2),
fc_adm_totalgarreal decimal(16,2),
dec_adm_financierocplimite decimal(16,2),
dec_adm_financierocpdispuesto decimal(16,2),
dec_adm_financierocpgarreal decimal(16,2),
dec_adm_financierolplimite decimal(16,2),
dec_adm_financierolpdispuesto decimal(16,2),
dec_adm_financierolpgarreal decimal(16,2),
dec_adm_avalestecnicoslimite decimal(16,2),
dec_adm_avalestecnicosdispuesto decimal(16,2),
dec_adm_avalestecnicosgarreal decimal(16,2),
dec_adm_otroslimite decimal(16,2),
dec_adm_otrosdispuesto decimal(16,2),
dec_adm_otrosgarreal decimal(16,2),
fc_adm_total2limite decimal(16,2),
fc_adm_total2dispuesto decimal(16,2),
fc_adm_total2garreal decimal(16,2),
fc_adm_ventas decimal(16,2),
dec_adm_ebitda decimal(16,2),
dec_adm_ebit decimal(16,2),
dec_adm_bai decimal(16,2),
dec_adm_cashflowlibre decimal(16,2),
dec_adm_fondospropios decimal(16,2),
dec_adm_deudafinancieraneta decimal(16,2),
dec_adm_deudafinancieralp decimal(16,2),
dec_adm_capitalcirculanteoperativo decimal(16,2),
fc_adm_totalbalance decimal(16,2),
dec_adm_margenordinariovalor decimal(16,2),
dec_adm_margenordinarioroa decimal(16,2),
dec_adm_cuotasanlimites decimal(16,2),
dec_adm_cuotasandispuestos decimal(16,2),
ts_adm_bcrafecha string,
dec_adm_bcravalor decimal(16,2),
ts_adm_fechaentradafeve string,
ts_adm_fechaprevia string,
int_adm_antiguedad bigint,
int_adm_calificacionactual bigint,
ts_adm_fechaultimacalificacion string,
ds_adm_calificacionfeve string,
ds_adm_situacionactual string,
ds_adm_focodepreocupacion string,
ds_adm_proximospasos string,
ds_adm_recomendacioncomite string,
ds_adm_zona string,
cod_adm_sucursal bigint,
cod_adm_grupoeconomico bigint,
ts_adm_fechainicio string,
ts_adm_fechafinalizacion string,
ts_adm_fechavtocalifactual string,
ts_adm_fecultimobalance string

)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_promontorio_ficha';