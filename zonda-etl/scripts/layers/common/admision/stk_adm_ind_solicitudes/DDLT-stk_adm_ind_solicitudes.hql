CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_solicitudes (
cod_adm_tramite string,
cod_suc_sucursal bigint,
cod_adm_nrosolicitud bigint,
ts_adm_fecingresoscoring string,
ts_adm_fecingresorio2 string,
ts_adm_fecenviosw1 string,
ts_adm_fecenviosw2 string,
cod_adm_estadosw string,
ts_adm_fecdesicionsw string,
ts_adm_fecingresosrs string,
ts_adm_fecasig_anasrs string,
ts_adm_fecini_resolsrs string,
ts_adm_fecfin_resolsrs string,
cod_adm_estadosrs string,
ts_adm_fecresolsuc string,
cod_adm_resolsuc string,
ts_adm_fecresolaltas string,
cod_adm_resolaltas string,
cod_adm_estadoactual string,
ts_adm_fec_estado_actual string,
cod_adm_estadoasol string,
ts_adm_fec_estado_asol string,
ds_adm_nomsectoraltas string,
ts_adm_fecswsrs string,
ds_adm_estadoswsrs string,
ds_adm_estado string,
cod_adm_destinofondos string,
fc_adm_valorbien bigint,
fc_adm_montosolicitado bigint,
fc_adm_plazosolicitado bigint,
fc_adm_importecuota bigint,
fc_adm_nroscore bigint,
ds_adm_maraprobrech string,
ds_adm_obsscor string,
int_adm_canparticipante bigint,
ds_adm_marplansueldo string,
ts_adm_fecingresosco string,
ts_adm_hraingresosco string,
cod_adm_analista string,
cod_adm_supervisor string,
cod_adm_promotor string,
cod_adm_promscor string,
cod_adm_marbp string,
cod_adm_emp_pas string,
ds_adm_marlimitesoptimizados string,
cod_adm_colaasignada string,
cod_adm_estadosco string,
cod_adm_sector string,
cod_adm_motzg string,
ds_adm_marpreembozado string,
fc_adm_limpppvigente bigint,
fc_adm_limaccvigente bigint,
ds_adm_mar_aso1_auto string,
ds_adm_obser string,
cod_adm_prendarioordensolicituddesc bigint,
cod_adm_sucursalprev bigint,
cod_adm_nrosolicitudprev bigint,
dt_adm_fecresolucionpreevaluacion string,
ds_adm_resolucionpreevaluacion string,
cod_adm_nrotramitefe_pdv bigint,
cod_adm_tramite2dollamado_pdv string,
cod_adm_participante bigint,
ds_adm_martitulo string,
ds_adm_marprodpas string,
cod_adm_bca string,
cod_adm_divs string,
cod_adm_tlead bigint,
cod_adm_ejeccta bigint,
ds_adm_marempinh string,
cod_adm_bolinh bigint,
cod_adm_nroorden bigint,
cod_adm_seccinh bigint,
cod_adm_nrobanco bigint,
dt_adm_fecaltinh string,
dt_adm_fecvtoinh string,
fc_adm_caninh bigint,
cod_adm_causinh string,
ds_adm_causinh string,
ds_adm_nomnombre string,
ds_adm_nomapellido string,
ds_adm_tpodoc string,
cod_adm_nrodoc bigint,
dt_adm_fecnacimiento string,
ds_adm_marsexo string,
cod_adm_nacionalidad string,
cod_adm_estadocivil string,
int_adm_canpersacargo int,
cod_adm_nivelestudio string,
cod_adm_rolensoli string,
ds_adm_maraprbrechscor string,
int_adm_canconsul bigint,
int_adm_canantecmenor bigint,
fc_adm_canantecmayr bigint,
int_adm_canantecregu bigint,
dt_adm_fecaltaprodcb string,
int_adm_canprodactvcb bigint,
int_adm_canprodactvbc bigint,
cod_adm_experpreviasis string,
cod_adm_informeveraz string,
fc_adm_monsuel bigint,
fc_adm_monsac bigint,
fc_adm_moncomision bigint,
fc_adm_mongratif bigint,
fc_adm_monrentas bigint,
fc_adm_monotringr bigint,
ds_adm_otringr string,
fc_adm_monalquiler bigint,
fc_adm_monexpensas bigint,
ds_adm_marcotitular string,
cod_adm_profesion string,
ds_adm_marcliente string,
int_adm_nropartcony bigint,
fc_adm_cantarjetas bigint,
int_adm_canantecmayrlev bigint,
int_adm_canantecmayrgra bigint,
cod_adm_sucverazreut bigint,
fc_adm_nrosolicverazreut bigint,
cod_adm_idenup bigint,
cod_adm_valdocu string,
ds_adm_tpoins string,
fc_adm_nroins bigint,
ds_adm_marfraude string,
ds_adm_marexperiencia string,
fc_adm_nroscoreveraz bigint,
cod_adm_grupoveraz string,
cod_adm_poblacveraz string,
ds_adm_indicadorriesgo string,
cod_adm_nupempresaasociada bigint,
ds_adm_marmaxir string,
fc_adm_moningvalidos bigint,
fc_adm_limtoperenta bigint,
ds_adm_marpyme string,
dt_adm_fecmarjuanito string,
ds_adm_email string,
ds_adm_celular string,
ds_adm_documentacion string,
fc_adm_hijos bigint,
fc_adm_importecolegio bigint,
ds_adm_nroloteveraz_reut string,
ds_adm_marciti string,
ds_adm_marrelamex string,
ds_adm_actividadafip string,
ds_adm_antig_actafip string,
ds_adm_tipocliente string,
cod_adm_tiporenta string,
fc_adm_cantadeleftvo_pesos bigint,
fc_adm_monadeleftvo_pesos decimal(17,2),
fc_adm_cantadeleftvo_dolares bigint,
fc_adm_monadeleftvo_dolares decimal(17,2),
ds_adm_politicas string,
ds_adm_universidad string,
ds_adm_carrera string,
int_adm_cantmatfalt bigint,
ts_adm_fecingresouni string,
ts_adm_fecegresouni string,
ds_adm_tposolicitud string,
cod_adm_nrosolicitudfe bigint,
cod_adm_canal string,
ds_adm_tpobien string,
fc_adm_porcltv decimal(17,2),
ds_adm_obscobertura string,
int_adm_aniobien bigint,
int_adm_valrelcuotaingreso bigint,
int_adm_moningresototalgrupo bigint,
ds_adm_tpo_uso string,
ds_adm_nom_aseguradora string,
ds_adm_periodicidad string,
ds_adm_marca string,
ds_adm_modelo string,
ds_adm_marverazrenovado string,
ds_adm_habilitamejora string,
fc_adm_pzoprestamo decimal(17,2),
fc_admmonacredhab bigint,
int_admdiasacredhab bigint,
fc_adm_tasa12meses decimal(17,2),
fc_adm_tasa24meses decimal(17,2),
fc_adm_tasa36meses decimal(17,2),
fc_adm_tasa48meses decimal(17,2),
fc_adm_tasa60meses decimal(17,2),
fc_adm_alicuotaivaprend decimal(17,2),
fc_adm_seguroautomotorprend decimal(17,2),
fc_adm_segurovida_prend decimal(17,7),
fc_adm_tasa12mesesuva decimal(17,2),
fc_adm_tasa24mesesuva decimal(17,2),
fc_adm_tasa36mesesuva decimal(17,2),
fc_adm_tasa48mesesuva decimal(17,2),
fc_adm_tasa60mesesuva decimal(17,2),
ds_adm_producto string,
ds_adm_productodetalle string,
ds_adm_canal string,
ds_adm_sucursalregion string,
ds_adm_sucursalnombre string,
ds_adm_sucursalzonacomer string,
cod_adm_concesionario string,
ds_adm_nomconcesionario string,
ds_adm_maralta string,
dt_adm_fecestado31 string,
ts_adm_estado_31 string,
cod_adm_sectorusu31 string,
cod_adm_usuario31 string,
dt_adm_fecestado10 string,
ts_adm_estado_10 string,
cod_adm_sectorusu10 string,
cod_adm_usuario10 string,
dt_adm_fecestado13 string,
ts_adm_estado_13 string,
cod_adm_sectorusu13 string,
cod_adm_usuario13 string,
dt_adm_fecestado12 string,
ts_adm_estado_12 string,
cod_adm_sectorusu12 string,
cod_adm_usuario12 string,
fc_adm_limvisa decimal(17,2),
fc_adm_limacte decimal(17,2),
fc_adm_limppp decimal(17,2),
fc_adm_limamex decimal(17,2),
fc_adm_limcheques decimal(17,2),
fc_adm_limtotal decimal(17,2),
cod_nro_cuenta bigint,
ds_adm_estadoresolucioncvc string,
ts_adm_fecingresocvc string,
ts_adm_feciniresolcvc string,
ts_adm_fecfinresolcvc string,
cod_adm_estadocvc string,
ds_adm_motresol1cvc string,
ds_adm_motresol2cvc string,
ds_adm_motresol3cvc string,
ds_adm_motresol4cvc string,
ds_adm_motresol5cvc string,
cod_adm_analistaCVC string,
ds_adm_estadoresolucionsri string,
ts_adm_fecingresosri string,
ts_adm_feciniresolsri string,
ts_adm_fecfinresolsri string,
cod_adm_estadosri string,
ds_adm_motresol1sri string,
ds_adm_motresol2sri string,
ds_adm_motresol3sri string,
ds_adm_motresol4sri string,
ds_adm_motresol5sri string,
cod_adm_analistasri string,
ds_adm_codigohash string,
ds_adm_tipotramitehash string,
int_adm_valor int,
cod_adm_ocupac string,
cod_adm_emptpo string,
flag_adm_motivosupnoforzaje int,
ds_adm_tipotramite string,
ds_adm_productopromocion string,
dt_adm_fecaltaminima string,
cod_per_segmentoduro string,
ds_per_segmento string,
ds_per_subsegmento string,
cod_per_cuadrante string,
dt_adm_fecultresolriesgos string,
cod_adm_estadoultresolriesgos string,
ds_adm_sector_ultresolriesgos string,
ds_adm_estadoultresolriesgos string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_solicitudes'
;