CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmparticipantes (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
fc_adm_acreditacionhaberesu6m double,
fc_adm_acreditacionhonorariosminimau6m double,
fc_adm_acreditacionhonorariosu6m double,
ds_adm_actividad string,
fc_adm_afectaciondisponible double,
fc_adm_afectaciondisponibleconrevolving double,
fc_adm_afectacionpreseleccion double,
cod_adm_antecedentesbsrdowngrade string,
flag_adm_antecedentesintenainh string,
flag_adm_antecedentesinternosenbsr string,
int_adm_antecedentesprodrgoactantbsr int,
int_adm_antiguedadactividad int,
int_adm_antiguedadantnegmasreciente string,
int_adm_antiguedadautomovil int,
int_adm_antiguedadbehaviour int,
int_adm_antiguedaddosultimosdomicilios string,
int_adm_antiguedaddosultimosempleos string,
int_adm_antiguedadempleoactual string,
int_adm_antiguedadempleoprevio string,
int_adm_antiguedadenbsr int,
int_adm_antiguedadmarcapba int,
int_adm_antiguedadpaqueterecalifica string,
int_adm_antiguedadprimerconsultaveraz int,
int_adm_antiguedadprodrgoactantbsr int,
int_adm_antiguedadprodrgopasivosantbsr int,
int_adm_antiguedadproductoactivomasantiguo int,
int_adm_antiguedadultimaconsultaveraz int,
fc_adm_asistenciaacuerdopreseleccion double,
fc_adm_asistenciaamexpreseleccion double,
fc_adm_asistenciachequepreseleccion double,
fc_adm_asistenciamasterpreseleccion double,
fc_adm_asistenciaprestamosmonoproductopreseleccion double,
fc_adm_asistenciaprespreacpreseleccion double,
fc_adm_asistenciatotalpreseleccion double,
fc_adm_asistenciavisabusinesspreseleccion double,
fc_adm_asistenciavisapreseleccion double,
ds_adm_automarca string,
ds_adm_automodelo string,
ds_adm_barrio string,
int_adm_cantidadantecedentesmayores int,
int_adm_cantidadantecedentesmenores int,
int_adm_cantidadantecedentesregul int,
int_adm_cantidadchequesrechazados int,
int_adm_cantidadconsultas int,
int_adm_cantidadconsultasu2m int,
int_adm_cantidaddiasdesdeultimaacreditacion int,
int_adm_cantidadingresoreca int,
int_adm_cantidadmayoresgraves int,
int_adm_cantidadmayoresmenosgraves int,
int_adm_cantidadmesesacredhaberes int,
int_adm_cantidadmesesacredhonorarios int,
int_adm_cantidadpersonasacargo int,
int_adm_cantidadprodactconbuencompor int,
int_adm_cantidadprodactcredbureau int,
int_adm_cantidadrecondcerrado2a1os int,
int_adm_cantidadrecondcerradoindep int,
int_adm_cantidadrefaccerrado2a1os int,
int_adm_cantidadrefaccerradoindep int,
int_adm_cantidadsolicitudingresadaenpdv double,
int_adm_cantidadtarjetacredito int,
int_adm_cantidadtarjetasveraz int,
int_adm_cantidadtelefonosinformados int,
ds_adm_cargofuncion string,
ds_adm_celular string,
cod_adm_clasificacionscoremapa int,
cod_adm_codemppas int,
cod_adm_codigoactividad string,
cod_adm_codigopostal string,
cod_adm_codigoproductovigente string,
cod_adm_codigosegmentoactual string,
cod_adm_codigosegmentopyme string,
cod_adm_codproductopreseleccion string,
cod_adm_codprogramapreseleccion string,
int_adm_continuidadenantigdomicilio int,
fc_adm_cuentaencicloatraso1 int,
fc_adm_cuentaencicloatraso2 int,
fc_adm_cuentaencicloatraso3 int,
fc_adm_cuentaencicloatraso4 int,
fc_adm_cuotamaximaporendeudamiento double,
fc_adm_cuotaprestamopreseleccion double,
int_adm_diasdesdesolicitudanterior int,
int_adm_diasdesdeultimaacreedauph string,
int_adm_diasdesdeultimasolicituddowngrade int,
int_adm_diasdesdeultimoantecedent int,
int_adm_diasnuevomapa int,
ds_adm_documento string,
fc_adm_edad int,
ds_adm_email string,
fc_adm_endeudamientoconrevolving double,
fc_adm_endeudamientopreseleccion double,
fc_adm_endeudamientosfcongtia double,
cod_adm_entidadsituacionbcra string,
cod_adm_estadocivil string,
ds_adm_estadocivilvsvivienda string,
fc_adm_excesivasobligaciones double,
fc_adm_exigiblectacteenbsr double,
fc_adm_exigiblectacteensf double,
fc_adm_exigiblecuotificado double,
fc_adm_exigiblemensual double,
fc_adm_exigiblepagomintarjenbsr double,
fc_adm_exigiblepagomintarjensf double,
fc_adm_exigiblepresthipoenbsr double,
fc_adm_exigiblepresthipoensf double,
fc_adm_exigibleprestpersenbsr double,
fc_adm_exigibleprestpersensf double,
fc_adm_exigibleprestprendenbsr double,
fc_adm_exigibleprestprendensf double,
fc_adm_exigiblesprestamosbsr double,
fc_adm_exigiblestarjetasbsr double,
cod_adm_experienciaconbsr string,
flag_adm_experienciapreviasistema string,
dt_adm_fechavencimientopreseleccion string,
fc_adm_gastomensualvivienda double,
cod_adm_grupoveraz string,
cod_adm_grupoverazpreseleccion string,
flag_adm_habilitaingresos string,
int_adm_incomepredictorveraz int,
cod_adm_indicadoringreso string,
cod_adm_indicadorrentabilidad string,
cod_adm_indicadorriesgo string,
flag_adm_informacionveraz string,
cod_adm_informeveraz string,
fc_adm_ingresoacreditadoultbasepas double,
fc_adm_ingresoami double,
fc_adm_ingresodeclarado double,
fc_adm_ingresodemostrado double,
fc_adm_ingresoextraordinario double,
fc_adm_ingresoinferido double,
fc_adm_ingresoinferidoneto double,
fc_adm_ingresoinferidospredext double,
fc_adm_ingresoipveraz double,
fc_adm_ingresomapa double,
fc_adm_ingresomensualordinario double,
fc_adm_ingresominimoproducto double,
fc_adm_ingresopresuntopreseleccion double,
fc_adm_ingresotabladatosexp double,
fc_adm_ingresoverificaanteriores double,
fc_adm_ingresoverificadosolicitudanterior double,
fc_adm_ipveraz double,
fc_adm_limiteacuerdotriad int,
fc_adm_limiteacuerdovigente double,
fc_adm_limitecantchequestriad double,
fc_adm_limitecompraamexvigente int,
fc_adm_limitecompramastervigente double,
fc_adm_limitecompraprendariotriad double,
fc_adm_limitecompratarjetatriad double,
fc_adm_limitecompravisabusinessvigente double,
fc_adm_limitecompravisavigente double,
fc_adm_limitepreacordadovigente double,
fc_adm_limitepreshipotecariotriad double,
fc_adm_limitepreshipotecariovigente double,
fc_adm_limitepresmonoproductotriad double,
fc_adm_limiteprespersonalvigente double,
fc_adm_limitepresprendariovigente int,
fc_adm_limiteprestpersonalvigente double,
fc_adm_limitetotallineasrotativo double,
ds_adm_localidad string,
ds_adm_marcacausal84 string,
flag_adm_marcacliente string,
flag_adm_marcacomportamiento string,
flag_adm_marcacotitularidad string,
flag_adm_marcadatosvalidos string,
flag_adm_marcaempresapas string,
flag_adm_marcaexciti string,
flag_adm_marcagrupoinferencia string,
flag_adm_marcainginferidoultimasolicitud string,
flag_adm_marcalineasofertadastriad string,
flag_adm_marcaposeeauto string,
flag_adm_marcaposeecajaahorro string,
cod_adm_marcaposeectacte string,
flag_adm_marcaposeeinfinityplatinum string,
flag_adm_marcaprocesoincremento string,
flag_adm_marcarecalificacionobligatoria string,
flag_adm_marcarenta string,
cod_adm_marcarentapresunta string,
cod_adm_marcasolicitudentramite string,
flag_adm_marcatelefonoparticular string,
cod_adm_marcatitularidaddeproducto string,
flag_adm_marcaverificacionriesgonet string,
flag_adm_maxcodsubrodofrecer string,
cod_adm_maxprodofrecer string,
cod_adm_mesesmapa string,
cod_adm_mesessituacionbcra string,
int_adm_modelocomportamiento string,
cod_adm_modeloinferencia string,
cod_adm_modeloscorecompliance string,
cod_adm_modeloveraz string,
cod_adm_modeloveraz3 string,
cod_adm_montoauph string,
fc_adm_montosituacionbcra string,
fc_adm_motivorechazotriad int,
cod_adm_nacionalidad int,
ds_adm_nivelestudios string,
cod_adm_ocupacion string,
cod_adm_otrosingresosmensuales string,
fc_adm_peorcicloactual double,
cod_adm_partido int,
ds_adm_peorreferenciaveraz string,
cod_adm_periodosituacionbcra string,
cod_adm_poseerecondrefiacuerdopago string,
flag_adm_preembozadovigente string,
flag_adm_productomejorar string,
flag_adm_proporciongastosingreso string,
fc_adm_provinciadomicilio double,
ds_adm_puntajecomportamiento string,
fc_adm_puntajecorte int,
fc_adm_rangominimoriesgonet int,
int_adm_rangoverificadoriesgonet string,
cod_adm_refctacorrienteyahorro string,
cod_adm_reftarjetascredito string,
cod_adm_relacionparticipante string,
ds_adm_riesgo6 string,
flag_adm_rubro int,
cod_adm_scorebehaviour string,
int_adm_scorecomportamiento int,
int_adm_scoremapa int,
int_adm_scoreoriginacion int,
int_adm_scoretriadpreseleccion int,
int_adm_scoreveraz int,
int_adm_scoreveraz3 int,
int_adm_scoreveraz35 int,
int_adm_scoreveraz35rf int,
int_adm_scoreverazcompliance int,
int_adm_scoreverazconconsumo int,
int_adm_scoreverazpreseleccion int,
cod_adm_sectoreconomico string,
cod_adm_segmentacionrentainferencia string,
ds_adm_semafororiesgos double,
int_adm_sexo string,
cod_adm_situacionpas string,
cod_adm_sumacuotaprestamosensf double,
fc_adm_sumalimitecompratc double,
fc_adm_sumatoriaexigiblesenbsr double,
fc_adm_sumatoriaexigiblesensf double,
fc_adm_sumatoriatotalexigibles double,
flag_adm_telefonosvalidos string,
flag_adm_tienesituacionbcra string,
flag_adm_tipodocumento string,
cod_adm_tipoempresa string,
cod_adm_tipoocupacion string,
cod_adm_tipopaqueterecalifica string,
cod_adm_tipopersona string,
cod_adm_tipoprestamomapa string,
cod_adm_tipoproductomejorar int,
cod_adm_tiporelacion string,
cod_adm_tiporelacionlaboral string,
cod_adm_tiposcorecomportamiento string,
cod_adm_tiposituacionbcra int,
cod_adm_tipovaliddniveraz string,
cod_adm_tipovivienda string,
cod_adm_totalingresomensual string,
fc_adm_usoexigible double,
fc_adm_utilizacionrotativos double,
fc_adm_validacioningreso double,
cod_adm_zonadomicilio string,
cod_adm_documentacion string,
fc_adm_asistenciacuotaprestamopreseleccion double,
fc_adm_valorcuotaprestamopreseleccion double,
int_adm_antiguedadtarjmasteractmasant int,
int_adm_antiguedadtarjamexactmasant int,
int_adm_antiguedadtarjvisaactmasant int,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmparticipantes'
;