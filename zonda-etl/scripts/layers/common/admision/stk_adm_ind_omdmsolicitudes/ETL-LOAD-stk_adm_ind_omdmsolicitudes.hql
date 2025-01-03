set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_ind_omdmsolicitudes
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}')
select tipo_tramite as cod_adm_tipotramite,
cod_tramite as cod_adm_tramite,
substr(fec_proceso,1,19) as ts_adm_fecproceso,
cast(b.accAMSF as double) as fc_adm_accamsf,
cast(b.acuerdoOfertado as double) as fc_adm_acuerdoofertado,
cast(b.afectacionCalculada as double) as fc_adm_afectacioncalculada,
cast(b.afectacionFinal as double) as fc_adm_afectacionfinal,
cast(b.afectacionMinima as double) as fc_adm_afectacionminima,
cast(b.afectacionNecesaria as double) as fc_adm_afectacionnecesaria,
cast(b.alicuotaIVAPrendario as double) as fc_adm_alicuotaivaprendario,
cast(b.amexAMSF as double) as fc_adm_amexamsf,
cast(b.amexOfertada as double) as fc_adm_amexofertada,
cast(b.antigVehiculoAPrendar as int) as int_adm_antigvehiculoaprendar,
cast(b.asistenciaAmex as double) as fc_adm_asistenciaamex,
cast(b.asistenciaCuenta as double) as fc_adm_asistenciacuenta,
cast(b.asistenciaMaximaASolaFirma as double) as fc_adm_asistenciamaximaasolafirma,
cast(b.asistenciaMaximaASolaFirmaOfertado as double) as fc_adm_asistenciamaximaasolafirmaofertado,
cast(b.asistenciaMaximaOptimizacion as double) as fc_adm_asistenciamaximaoptimizacion,
cast(b.asistenciaMaximaPorSegmento as double) as fc_adm_asistenciamaximaporsegmento,
cast(b.asistenciaMaximaSegmento as double) as fc_adm_asistenciamaximasegmento,
cast(b.asistenciaMinimaNecesaria as double) as fc_adm_asistenciaminimanecesaria,
cast(b.asistenciaPrestamo as double) as fc_adm_asistenciaprestamo,
cast(b.asistenciaVisa as double) as fc_adm_asistenciavisa,
cast(b.auxiliar as double) as fc_adm_auxiliar,
cast(b.auxiliarInteger as int) as int_adm_auxiliarinteger,
b.canalOrigen as cod_adm_canalorigen,
cast(b.cantidadDiasAltaPaquete as int) as int_adm_cantidaddiasaltapaquete,
cast(b.cesionChequesAMSF as double) as fc_adm_cesionchequesamsf,
cast(b.cesionChequesInfyGold as double) as fc_adm_cesionchequesinfygold,
b.cesionChequesOfertado as fc_adm_cesionchequesofertado,
cast(b.cesionChequesPlatinum as double) as fc_adm_cesionchequesplatinum,
cast(b.cesionChequesUnificada as double) as fc_adm_cesionchequesunificada,
b.codigoDecisionSolAnterior as cod_adm_codigodecisionsolanterior,
b.codigoPaquete as cod_adm_codigopaquete,
cast(b.codigoPaqueteInteger as int) as cod_adm_codigopaqueteinteger,
b.codigoProducto as cod_adm_codigoproducto,
b.codigoProductoAltasMasivas as cod_adm_codigoproductoaltasmasivas,
b.codigoProductoSolicitudAnterior as cod_adm_codigoproductosolicitudanterior,
b.codigoPrograma as cod_adm_codigoprograma,
b.codigoPromocion as cod_adm_codigopromocion,
b.codigoProvincia as cod_adm_codigoprovincia,
b.codigoSector as cod_adm_codigosector,
cast(b.codigoSubzonaInterior as int) as cod_adm_codigosubzonainterior,
b.codigoTarjetaAmex as cod_adm_codigotarjetaamex,
cast(b.codigoTarjetaAmexInteger as int) as cod_adm_codigotarjetaamexinteger,
b.codigoTarjetaAmexMaximo as cod_adm_codigotarjetaamexmaximo,
b.codigoTarjetaMaster as cod_adm_codigotarjetamaster,
b.codigoTarjetaVisa as cod_adm_codigotarjetavisa,
cast(b.codigoTarjetaVisaInteger as int) as cod_adm_codigotarjetavisainteger,
b.codigoTarjetaVisaMaximo as cod_adm_codigotarjetavisamaximo,
cast(b.cuota36meses as double) as fc_adm_cuota36meses,
cast(b.cuotaFinal as double) as fc_adm_cuotafinal,
cast(b.cuotaPrestamo as double) as fc_adm_cuotaprestamo,
cast(b.cuotaPuraFinal as double) as fc_adm_cuotapurafinal,
cast(b.cuotaPuraMaximaAFECT as double) as fc_adm_cuotapuramaximaafect,
cast(b.cuotaPuraMaximaCUOV as double) as fc_adm_cuotapuramaximacuov,
cast(b.cuotaPuraMaximaSolicitud as double) as fc_adm_cuotapuramaximasolicitud,
cast(b.cuotaTotalMaximaAFECT as double) as fc_adm_cuotatotalmaximaafect,
b.datosConsistentes as flag_adm_datosconsistentes,
b.esBancarizado as flag_adm_esbancarizado,
b.esContraofertaPrendario as flag_adm_escontraofertaprendario,
cast(b.esEmpresaSeleccionada as int) as flag_adm_esempresaseleccionada,
b.esMonoproducto as flag_adm_esmonoproducto,
b.esNTB as flag_adm_esntb,
b.esPaquete as flag_adm_espaquete,
b.esPrendarioPyme as flag_adm_esprendariopyme,
b.esPreseleccion as flag_adm_espreseleccion,
b.esScoreBajo as flag_adm_esscorebajo,
b.esUnificacion as flag_adm_esunificacion,
cast(b.excesivoEndeudamiento as double) as fc_adm_excesivoendeudamiento,
b.fechaEntrada as dt_adm_fechaentrada,
b.fechaUltimaSolicitud as dt_adm_fechaultimasolicitud,
b.fechaVigencia as dt_adm_fechavigencia,
b.flujoOMDM as ds_adm_flujoomdm,
b.grupoVerazSolicitudAnterior as cod_adm_grupoverazsolicitudanterior,
b.habilitaMejora as flag_adm_habilitamejora,
cast(b.hit as int) as flag_adm_hit,
cast(b.ingresoFinal as double) as fc_adm_ingresofinal,
cast(b.ingresoGrupo as double) as fc_adm_ingresogrupo,
cast(b.ingresoMinimo as double) as fc_adm_ingresominimo,
cast(b.ingresoUtilizadoMinimo as double) as fc_adm_ingresoutilizadominimo,
cast(b.limiteACCSolicitado as double) as fc_adm_limiteaccsolicitado,
cast(b.limiteAcuerdoTRIAD as int) as fc_adm_limiteacuerdotriad,
cast(b.limiteCompraAmexSolicitado as double) as fc_adm_limitecompraamexsolicitado,
cast(b.limiteCompraCuotasAMEX as double) as fc_adm_limitecompracuotasamex,
cast(b.limiteCompraCuotasMASTER as double) as fc_adm_limitecompracuotasmaster,
cast(b.limiteCompraCuotasVISA as double) as fc_adm_limitecompracuotasvisa,
cast(b.limiteCompraCuotasVISABUSINESS as double) as fc_adm_limitecompracuotasvisabusiness,
cast(b.limiteCompraMasterSolicitado as double) as fc_adm_limitecompramastersolicitado,
cast(b.limiteCompraNuevo as double) as fc_adm_limitecompranuevo,
cast(b.limiteCompraViejo as double) as fc_adm_limitecompraviejo,
cast(b.limiteCompraVisaBusinessSolicitado as double) as fc_adm_limitecompravisabusinesssolicitado,
cast(b.limiteCompraVISASolicitado as double) as fc_adm_limitecompravisasolicitado,
cast(b.limitePreAcordadoSolicitado as double) as fc_adm_limitepreacordadosolicitado,
b.limiteSolicitudVsCodSeg as fc_adm_limitesolicitudvscodseg,
b.limiteUnificadoAMEX as fc_adm_limiteunificadoamex,
b.limiteUnificadoMASTER as fc_adm_limiteunificadomaster,
b.limiteUnificadoVISA as fc_adm_limiteunificadovisa,
b.limiteUnificadoVISABUSINESS as fc_adm_limiteunificadovisabusiness,
cast(b.limiteVisaPropuestoFinal as double) as fc_adm_limitevisapropuestofinal,
b.marcaAutoNuevoUsado as flag_adm_marcaautonuevousado,
b.marcaBaseDesk as ds_adm_marcabasedesk,
b.marcaBasePreVigente as ds_adm_marcabaseprevigente,
b.marcaCanal as ds_adm_marcacanal,
b.marcaDuplicadaPrograma as flag_adm_marcaduplicadaprograma,
b.marcaLimitesOptimizados as flag_adm_marcalimitesoptimizados,
b.marcaListaVehiculosComerciales as ds_adm_marcalistavehiculoscomerciales,
b.marcaModeloAuto as ds_adm_marcamodeloauto,
b.marcaMontoPrestamoVsDestinoFondos as ds_adm_marcamontoprestamovsdestinofondos,
b.marcaPAS as flag_adm_marcapas,
b.marcaPASSolicitudAnterior as flag_adm_marcapassolicitudanterior,
b.marcaProducto1 as ds_adm_marcaproducto1,
b.marcaProductoGarantia as ds_adm_marcaproductogarantia,
b.marcaProductoPromocion as ds_adm_marcaproductopromocion,
b.marcaProgPASUNI as ds_adm_marcaprogpasuni,
b.marcaPrograma as ds_adm_marcaprograma,
b.marcaPromo as ds_adm_marcapromo,
b.marcaRegionalizacionIngreso as flag_adm_marcaregionalizacioningreso,
b.marcaSolicitudDuplicada as flag_adm_marcasolicitudduplicada,
b.marcaSRI as cod_adm_marcasri,
b.marcaTarjetaAmex as ds_adm_marcatarjetaamex,
b.marcaTarjetaMaster as ds_adm_marcatarjetamaster,
b.marcaTarjetaOtorgadaPDV as ds_adm_marcatarjetaotorgadapdv,
b.marcaTarjetaPDVConurbano as ds_adm_marcatarjetapdvconurbano,
b.marcaTarjetaVisa as ds_adm_marcatarjetavisa,
b.marcaTramite as ds_adm_marcatramite,
b.marcaValidacionPromo as ds_adm_marcavalidacionpromo,
cast(b.masterAMSF as double) as fc_adm_masteramsf,
cast(b.maximaCUOVSolicitud as double) as fc_adm_maximacuovsolicitud,
cast(b.maximoOfertadoAcuerdo as double) as fc_adm_maximoofertadoacuerdo,
cast(b.maximoOfertadoAmex as double) as fc_adm_maximoofertadoamex,
cast(b.maximoOfertadoCesionCheques as double) as fc_adm_maximoofertadocesioncheques,
cast(b.maximoOfertadoMaster as double) as fc_adm_maximoofertadomaster,
cast(b.maximoOfertadoPreacordado as double) as fc_adm_maximoofertadopreacordado,
cast(b.maximoOfertadoSC1conPPP as double) as fc_adm_maximoofertadosc1conppp,
cast(b.maximoOfertadoVisa as double) as fc_adm_maximoofertadovisa,
cast(b.maximoOfertadoVisaBusiness as double) as fc_adm_maximoofertadovisabusiness,
b.mejoraOferta as flag_adm_mejoraoferta,
cast(b.minimoOfertadoAcuerdo as double) as fc_adm_minimoofertadoacuerdo,
cast(b.minimoOfertadoAmex as double) as fc_adm_minimoofertadoamex,
cast(b.minimoOfertadoCesionCheques as double) as fc_adm_minimoofertadocesioncheques,
cast(b.minimoOfertadoMaster as double) as fc_adm_minimoofertadomaster,
cast(b.minimoOfertadoPreacordado as double) as fc_adm_minimoofertadopreacordado,
cast(b.minimoOfertadoSC1conPPP as double) as fc_adm_minimoofertadosc1conppp,
cast(b.minimoOfertadoVisa as double) as fc_adm_minimoofertadovisa,
cast(b.minimoOfertadoVisaBusiness as double) as fc_adm_minimoofertadovisabusiness,
b.modeloPuntuacion as ds_adm_modelopuntuacion,
b.modeloVeraz3SolicitudAnterior as cod_adm_modeloveraz3solicitudanterior,
b.modeloVerazSolicitudAnterior as cod_adm_modeloverazsolicitudanterior,
b.monoProductoRecalifica as flag_adm_monoproductorecalifica,
cast(b.montoCesionValores as double) as fc_adm_montocesionvalores,
cast(b.montoContraofertaPrendario as double) as fc_adm_montocontraofertaprendario,
cast(b.montoCUOV as double) as fc_adm_montocuov,
cast(b.montoEstimadoBienEnGarantia as double) as fc_adm_montoestimadobienengarantia,
cast(b.montoFinal as double) as fc_adm_montofinal,
cast(b.montoMaximo as double) as fc_adm_montomaximo,
cast(b.montoMaximoAFECT as double) as fc_adm_montomaximoafect,
cast(b.montoPrestamo as double) as fc_adm_montoprestamo,
b.motivoRechazoTriadEstrategias as ds_adm_motivorechazotriadestrategias,
cast(b.nuevoPctFinanciacion as double) as fc_adm_nuevopctfinanciacion,
b.numeroSolicitud as cod_adm_numerosolicitud,
cast(b.numeroSolicitudPDV as double) as cod_adm_numerosolicitudpdv,
cast(b.numeroSpidAnterior as int) as int_adm_numerospidanterior,
b.numeroSucursalPDV as cod_adm_numerosucursalpdv,
cast(b.ofertaAmex as double) as fc_adm_ofertaamex,
cast(b.ofertaMaster as double) as fc_adm_ofertamaster,
cast(b.ofertaMontoPaquete as double) as fc_adm_ofertamontopaquete,
cast(b.ofertaPaquete as double) as fc_adm_ofertapaquete,
cast(b.ofertaPDVAmex as double) as fc_adm_ofertapdvamex,
cast(b.ofertaPDVAmexP1 as double) as fc_adm_ofertapdvamexp1,
cast(b.ofertaPDVMaster as double) as fc_adm_ofertapdvmaster,
cast(b.ofertaPDVMasterP1 as double) as fc_adm_ofertapdvmasterp1,
cast(b.ofertaPDVVisa as double) as fc_adm_ofertapdvvisa,
cast(b.ofertaPDVVisaP1 as double) as fc_adm_ofertapdvvisap1,
cast(b.ofertaPrestamo as double) as fc_adm_ofertaprestamo,
cast(b.ofertaSC1 as double) as fc_adm_ofertasc1,
cast(b.ofertaVisa as double) as fc_adm_ofertavisa,
cast(b.ofertaVisaBusiness as double) as fc_adm_ofertavisabusiness,
b.otrosParticipantes as flag_adm_otrosparticipantes,
b.participanteMaximoIndicadorRiesgo as cod_adm_participantemaximoindicadorriesgo,
cast(b.pctFinanciacion as double) as fc_adm_pctfinanciacion,
cast(b.pctMaximaFinanciacion as double) as fc_adm_pctmaximafinanciacion,
cast(b.pctMaximaFinanciacionContraOferta as double) as fc_adm_pctmaximafinanciacioncontraoferta,
cast(b.pctMaximoAfectacion as double) as fc_adm_pctmaximoafectacion,
cast(b.pctMaximoEndeudamiento as double) as fc_adm_pctmaximoendeudamiento,
cast(b.plazoAumentoTransitorio as int) as int_adm_plazoaumentotransitorio,
cast(b.plazoContraofertaPrendario as int) as int_adm_plazocontraofertaprendario,
cast(b.plazoFinal as int) as int_adm_plazofinal,
cast(b.plazoMaximo as double) as int_adm_plazomaximo,
cast(b.plazoPrestamo as int) as int_adm_plazoprestamo,
b.politicaPreevaluacion as cod_adm_politicapreevaluacion,
b.poseePreevRecaAprobada as flag_adm_poseepreevrecaaprobada,
b.poseePreevSimVigente as flag_adm_poseepreevsimvigente,
cast(b.pppAMSF as double) as fc_adm_pppamsf,
cast(b.preAcordadoOfertado as double) as fc_adm_preacordadoofertado,
b.productosRequeridos as ds_adm_productosrequeridos,
b.propositoPrestamo as ds_adm_propositoprestamo,
cast(b.relacionCuotaIngreso as double) as fc_adm_relacioncuotaingreso,
cast(b.relacionCuotaIngresoBCRA as double) as fc_adm_relacioncuotaingresobcra,
b.requiereReevaluar as flag_adm_requierereevaluar,
b.resolucionPymes as ds_adm_resolucionpymes,
cast(b.sc1AMSF as double) as fc_adm_sc1amsf,
cast(b.scoreUtilizado as int) as ds_adm_scoreutilizado,
cast(b.segmentoUniveritarioInteger as int) as cod_adm_segmentouniveritariointeger,
cast(b.seguroAutomotorPrendario as double) as fc_adm_seguroautomotorprendario,
cast(b.seguroVidaPrendario as double) as fc_adm_segurovidaprendario,
b.solicitaRecalificacionCesionCheques as flag_adm_solicitarecalificacioncesioncheques,
b.subsanable as flag_adm_subsanable,
cast(b.sucursal as int) as cod_adm_sucursal,
cast(b.tablaAfectacion as int) as int_adm_tablaafectacion,
cast(b.tablaAMSF as int) as int_adm_tablaamsf,
cast(b.tablaDistribucion as int) as int_adm_tabladistribucion,
cast(b.tablaOptimizacion as int) as int_adm_tablaoptimizacion,
cast(b.tablaScoringInteligente as int) as int_adm_tablascoringinteligente,
b.tarjetaAmex as ds_adm_tarjetaamex,
b.tarjetaAmexP1 as ds_adm_tarjetaamexp1,
b.tarjetaMaster as ds_adm_tarjetamaster,
b.tarjetaMasterP1 as ds_adm_tarjetamasterp1,
b.tarjetaMonoproductoPlatinum as ds_adm_tarjetamonoproductoplatinum,
b.tarjetaVisa as ds_adm_tarjetavisa,
b.tarjetaVisaP1 as ds_adm_tarjetavisap1,
cast(b.tasa12MPren as double) as fc_adm_tasa12mpren,
cast(b.tasa24MPren as double) as fc_adm_tasa24mpren,
cast(b.tasa36MPren as double) as fc_adm_tasa36mpren,
cast(b.tasa48MPren as double) as fc_adm_tasa48mpren,
cast(b.tasa60MPren as double) as fc_adm_tasa60mpren,
b.tasaCuotaAUPH as fc_adm_tasacuotaauph,
cast(b.tasaFinal as double) as fc_adm_tasafinal,
cast(b.tasaPrestamo as int) as fc_adm_tasaprestamo,
cast(b.tipoCambio as double) as fc_adm_tipocambio,
b.tipoIngreso as ds_adm_tipoingreso,
b.tipoIngresoDetallado as ds_adm_tipoingresodetallado,
b.tipoIngresoUtilizadoMinimo as ds_adm_tipoingresoutilizadominimo,
b.tipoPaquete as ds_adm_tipopaquete,
b.tipoPaqueteAltasMasivas as ds_adm_tipopaquetealtasmasivas,
b.tipoPaqueteRecalifica as ds_adm_tipopaqueterecalifica,
cast(b.tipoPaqueteInteger as int) as ds_adm_tipopaqueteinteger,
b.tipoProceso as ds_adm_tipoproceso,
b.tipoRecalificacion as ds_adm_tiporecalificacion,
b.tipoScoreUtilizado as ds_adm_tiposcoreutilizado,
b.tipoSolicitud as ds_adm_tiposolicitud,
b.tipoSolicitudAnterior as ds_adm_tiposolicitudanterior,
b.tipoTarjetaAmex as ds_adm_tipotarjetaamex,
b.tipoTarjetaMaster as ds_adm_tipotarjetamaster,
b.tipoTarjetaPDV as ds_adm_tipotarjetapdv,
b.tipoTarjetaVisa as ds_adm_tipotarjetavisa,
b.tipoTasaPrestamo as ds_adm_tipotasaprestamo,
cast(b.tipoUniversidadInteger as int) as ds_adm_tipouniversidadinteger,
b.totalLimiteSolicitudVsAsistenciaMaxima as fc_adm_totallimitesolicitudvsasistenciamaxima,
cast(b.visaAMSF as double) as fc_adm_visaamsf,
cast(b.visaOfertada as double) as fc_adm_visaofertada,
b.zona as ds_adm_zona,
b.zonaPDV as ds_adm_zonapdv,
json as ds_adm_json
from bi_corp_staging.scoring_omdm_solicitudes
lateral view json_tuple(json,
'accAMSF',
'acuerdoOfertado',
'afectacionCalculada',
'afectacionFinal',
'afectacionMinima',
'afectacionNecesaria',
'alicuotaIVAPrendario',
'amexAMSF',
'amexOfertada',
'antigVehiculoAPrendar',
'asistenciaAmex',
'asistenciaCuenta',
'asistenciaMaximaASolaFirma',
'asistenciaMaximaASolaFirmaOfertado',
'asistenciaMaximaOptimizacion',
'asistenciaMaximaPorSegmento',
'asistenciaMaximaSegmento',
'asistenciaMinimaNecesaria',
'asistenciaPrestamo',
'asistenciaVisa',
'auxiliar',
'auxiliarInteger',
'canalOrigen',
'cantidadDiasAltaPaquete',
'cesionChequesAMSF',
'cesionChequesInfyGold',
'cesionChequesOfertado',
'cesionChequesPlatinum',
'cesionChequesUnificada',
'codigoDecisionSolAnterior',
'codigoPaquete',
'codigoPaqueteInteger',
'codigoProducto',
'codigoProductoAltasMasivas',
'codigoProductoSolicitudAnterior',
'codigoPrograma',
'codigoPromocion',
'codigoProvincia',
'codigoSector',
'codigoSubzonaInterior',
'codigoTarjetaAmex',
'codigoTarjetaAmexInteger',
'codigoTarjetaAmexMaximo',
'codigoTarjetaMaster',
'codigoTarjetaVisa',
'codigoTarjetaVisaInteger',
'codigoTarjetaVisaMaximo',
'cuota36meses',
'cuotaFinal',
'cuotaPrestamo',
'cuotaPuraFinal',
'cuotaPuraMaximaAFECT',
'cuotaPuraMaximaCUOV',
'cuotaPuraMaximaSolicitud',
'cuotaTotalMaximaAFECT',
'datosConsistentes',
'esBancarizado',
'esContraofertaPrendario',
'esEmpresaSeleccionada',
'esMonoproducto',
'esNTB',
'esPaquete',
'esPrendarioPyme',
'esPreseleccion',
'esScoreBajo',
'esUnificacion',
'excesivoEndeudamiento',
'fechaEntrada',
'fechaUltimaSolicitud',
'fechaVigencia',
'flujoOMDM',
'grupoVerazSolicitudAnterior',
'habilitaMejora',
'hit',
'ingresoFinal',
'ingresoGrupo',
'ingresoMinimo',
'ingresoUtilizadoMinimo',
'limiteACCSolicitado',
'limiteAcuerdoTRIAD',
'limiteCompraAmexSolicitado',
'limiteCompraCuotasAMEX',
'limiteCompraCuotasMASTER',
'limiteCompraCuotasVISA',
'limiteCompraCuotasVISABUSINESS',
'limiteCompraMasterSolicitado',
'limiteCompraNuevo',
'limiteCompraViejo',
'limiteCompraVisaBusinessSolicitado',
'limiteCompraVISASolicitado',
'limitePreAcordadoSolicitado',
'limiteSolicitudVsCodSeg',
'limiteUnificadoAMEX',
'limiteUnificadoMASTER',
'limiteUnificadoVISA',
'limiteUnificadoVISABUSINESS',
'limiteVisaPropuestoFinal',
'marcaAutoNuevoUsado',
'marcaBaseDesk',
'marcaBasePreVigente',
'marcaCanal',
'marcaDuplicadaPrograma',
'marcaLimitesOptimizados',
'marcaListaVehiculosComerciales',
'marcaModeloAuto',
'marcaMontoPrestamoVsDestinoFondos',
'marcaPAS',
'marcaPASSolicitudAnterior',
'marcaProducto1',
'marcaProductoGarantia',
'marcaProductoPromocion',
'marcaProgPASUNI',
'marcaPrograma',
'marcaPromo',
'marcaRegionalizacionIngreso',
'marcaSolicitudDuplicada',
'marcaSRI',
'marcaTarjetaAmex',
'marcaTarjetaMaster',
'marcaTarjetaOtorgadaPDV',
'marcaTarjetaPDVConurbano',
'marcaTarjetaVisa',
'marcaTramite',
'marcaValidacionPromo',
'masterAMSF',
'maximaCUOVSolicitud',
'maximoOfertadoAcuerdo',
'maximoOfertadoAmex',
'maximoOfertadoCesionCheques',
'maximoOfertadoMaster',
'maximoOfertadoPreacordado',
'maximoOfertadoSC1conPPP',
'maximoOfertadoVisa',
'maximoOfertadoVisaBusiness',
'mejoraOferta',
'minimoOfertadoAcuerdo',
'minimoOfertadoAmex',
'minimoOfertadoCesionCheques',
'minimoOfertadoMaster',
'minimoOfertadoPreacordado',
'minimoOfertadoSC1conPPP',
'minimoOfertadoVisa',
'minimoOfertadoVisaBusiness',
'modeloPuntuacion',
'modeloVeraz3SolicitudAnterior',
'modeloVerazSolicitudAnterior',
'monoProductoRecalifica',
'montoCesionValores',
'montoContraofertaPrendario',
'montoCUOV',
'montoEstimadoBienEnGarantia',
'montoFinal',
'montoMaximo',
'montoMaximoAFECT',
'montoPrestamo',
'motivoRechazoTriadEstrategias',
'nuevoPctFinanciacion',
'numeroSolicitud',
'numeroSolicitudPDV',
'numeroSpidAnterior',
'numeroSucursalPDV',
'ofertaAmex',
'ofertaMaster',
'ofertaMontoPaquete',
'ofertaPaquete',
'ofertaPDVAmex',
'ofertaPDVAmexP1',
'ofertaPDVMaster',
'ofertaPDVMasterP1',
'ofertaPDVVisa',
'ofertaPDVVisaP1',
'ofertaPrestamo',
'ofertaSC1',
'ofertaVisa',
'ofertaVisaBusiness',
'otrosParticipantes',
'participanteMaximoIndicadorRiesgo',
'pctFinanciacion',
'pctMaximaFinanciacion',
'pctMaximaFinanciacionContraOferta',
'pctMaximoAfectacion',
'pctMaximoEndeudamiento',
'plazoAumentoTransitorio',
'plazoContraofertaPrendario',
'plazoFinal',
'plazoMaximo',
'plazoPrestamo',
'politicaPreevaluacion',
'poseePreevRecaAprobada',
'poseePreevSimVigente',
'pppAMSF',
'preAcordadoOfertado',
'productosRequeridos',
'propositoPrestamo',
'relacionCuotaIngreso',
'relacionCuotaIngresoBCRA',
'requiereReevaluar',
'resolucionPymes',
'sc1AMSF',
'scoreUtilizado',
'segmentoUniveritarioInteger',
'seguroAutomotorPrendario',
'seguroVidaPrendario',
'solicitaRecalificacionCesionCheques',
'subsanable',
'sucursal',
'tablaAfectacion',
'tablaAMSF',
'tablaDistribucion',
'tablaOptimizacion',
'tablaScoringInteligente',
'tarjetaAmex',
'tarjetaAmexP1',
'tarjetaMaster',
'tarjetaMasterP1',
'tarjetaMonoproductoPlatinum',
'tarjetaVisa',
'tarjetaVisaP1',
'tasa12MPren',
'tasa24MPren',
'tasa36MPren',
'tasa48MPren',
'tasa60MPren',
'tasaCuotaAUPH',
'tasaFinal',
'tasaPrestamo',
'tipoCambio',
'tipoIngreso',
'tipoIngresoDetallado',
'tipoIngresoUtilizadoMinimo',
'tipoPaquete',
'tipoPaqueteAltasMasivas',
'tipoPaqueteRecalifica',
'tipoPaqueteInteger',
'tipoProceso',
'tipoRecalificacion',
'tipoScoreUtilizado',
'tipoSolicitud',
'tipoSolicitudAnterior',
'tipoTarjetaAmex',
'tipoTarjetaMaster',
'tipoTarjetaPDV',
'tipoTarjetaVisa',
'tipoTasaPrestamo',
'tipoUniversidadInteger',
'totalLimiteSolicitudVsAsistenciaMaxima',
'visaAMSF',
'visaOfertada',
'zona',
'zonaPDV'
) b as accAMSF,acuerdoOfertado,afectacionCalculada,afectacionFinal,afectacionMinima,afectacionNecesaria,alicuotaIVAPrendario,amexAMSF,amexOfertada,antigVehiculoAPrendar,asistenciaAmex,asistenciaCuenta,asistenciaMaximaASolaFirma,asistenciaMaximaASolaFirmaOfertado,asistenciaMaximaOptimizacion,asistenciaMaximaPorSegmento,asistenciaMaximaSegmento,asistenciaMinimaNecesaria,asistenciaPrestamo,asistenciaVisa,auxiliar,auxiliarInteger,canalOrigen,cantidadDiasAltaPaquete,cesionChequesAMSF,cesionChequesInfyGold,cesionChequesOfertado,cesionChequesPlatinum,cesionChequesUnificada,codigoDecisionSolAnterior,codigoPaquete,codigoPaqueteInteger,codigoProducto,codigoProductoAltasMasivas,codigoProductoSolicitudAnterior,codigoPrograma,codigoPromocion,codigoProvincia,codigoSector,codigoSubzonaInterior,codigoTarjetaAmex,codigoTarjetaAmexInteger,codigoTarjetaAmexMaximo,codigoTarjetaMaster,codigoTarjetaVisa,codigoTarjetaVisaInteger,codigoTarjetaVisaMaximo,cuota36meses,cuotaFinal,cuotaPrestamo,cuotaPuraFinal,cuotaPuraMaximaAFECT,cuotaPuraMaximaCUOV,cuotaPuraMaximaSolicitud,cuotaTotalMaximaAFECT,datosConsistentes,esBancarizado,esContraofertaPrendario,esEmpresaSeleccionada,esMonoproducto,esNTB,esPaquete,esPrendarioPyme,esPreseleccion,esScoreBajo,esUnificacion,excesivoEndeudamiento,fechaEntrada,fechaUltimaSolicitud,fechaVigencia,flujoOMDM,grupoVerazSolicitudAnterior,habilitaMejora,hit,ingresoFinal,ingresoGrupo,ingresoMinimo,ingresoUtilizadoMinimo,limiteACCSolicitado,limiteAcuerdoTRIAD,limiteCompraAmexSolicitado,limiteCompraCuotasAMEX,limiteCompraCuotasMASTER,limiteCompraCuotasVISA,limiteCompraCuotasVISABUSINESS,limiteCompraMasterSolicitado,limiteCompraNuevo,limiteCompraViejo,limiteCompraVisaBusinessSolicitado,limiteCompraVISASolicitado,limitePreAcordadoSolicitado,limiteSolicitudVsCodSeg,limiteUnificadoAMEX,limiteUnificadoMASTER,limiteUnificadoVISA,limiteUnificadoVISABUSINESS,limiteVisaPropuestoFinal,marcaAutoNuevoUsado,marcaBaseDesk,marcaBasePreVigente,marcaCanal,marcaDuplicadaPrograma,marcaLimitesOptimizados,marcaListaVehiculosComerciales,marcaModeloAuto,marcaMontoPrestamoVsDestinoFondos,marcaPAS,marcaPASSolicitudAnterior,marcaProducto1,marcaProductoGarantia,marcaProductoPromocion,marcaProgPASUNI,marcaPrograma,marcaPromo,marcaRegionalizacionIngreso,marcaSolicitudDuplicada,marcaSRI,marcaTarjetaAmex,marcaTarjetaMaster,marcaTarjetaOtorgadaPDV,marcaTarjetaPDVConurbano,marcaTarjetaVisa,marcaTramite,marcaValidacionPromo,masterAMSF,maximaCUOVSolicitud,maximoOfertadoAcuerdo,maximoOfertadoAmex,maximoOfertadoCesionCheques,maximoOfertadoMaster,maximoOfertadoPreacordado,maximoOfertadoSC1conPPP,maximoOfertadoVisa,maximoOfertadoVisaBusiness,mejoraOferta,minimoOfertadoAcuerdo,minimoOfertadoAmex,minimoOfertadoCesionCheques,minimoOfertadoMaster,minimoOfertadoPreacordado,minimoOfertadoSC1conPPP,minimoOfertadoVisa,minimoOfertadoVisaBusiness,modeloPuntuacion,modeloVeraz3SolicitudAnterior,modeloVerazSolicitudAnterior,monoProductoRecalifica,montoCesionValores,montoContraofertaPrendario,montoCUOV,montoEstimadoBienEnGarantia,montoFinal,montoMaximo,montoMaximoAFECT,montoPrestamo,motivoRechazoTriadEstrategias,nuevoPctFinanciacion,numeroSolicitud,numeroSolicitudPDV,numeroSpidAnterior,numeroSucursalPDV,ofertaAmex,ofertaMaster,ofertaMontoPaquete,ofertaPaquete,ofertaPDVAmex,ofertaPDVAmexP1,ofertaPDVMaster,ofertaPDVMasterP1,ofertaPDVVisa,ofertaPDVVisaP1,ofertaPrestamo,ofertaSC1,ofertaVisa,ofertaVisaBusiness,otrosParticipantes,participanteMaximoIndicadorRiesgo,pctFinanciacion,pctMaximaFinanciacion,pctMaximaFinanciacionContraOferta,pctMaximoAfectacion,pctMaximoEndeudamiento,plazoAumentoTransitorio,plazoContraofertaPrendario,plazoFinal,plazoMaximo,plazoPrestamo,politicaPreevaluacion,poseePreevRecaAprobada,poseePreevSimVigente,pppAMSF,preAcordadoOfertado,productosRequeridos,propositoPrestamo,relacionCuotaIngreso,relacionCuotaIngresoBCRA,requiereReevaluar,resolucionPymes,sc1AMSF,scoreUtilizado,segmentoUniveritarioInteger,seguroAutomotorPrendario,seguroVidaPrendario,solicitaRecalificacionCesionCheques,subsanable,sucursal,tablaAfectacion,tablaAMSF,tablaDistribucion,tablaOptimizacion,tablaScoringInteligente,tarjetaAmex,tarjetaAmexP1,tarjetaMaster,tarjetaMasterP1,tarjetaMonoproductoPlatinum,tarjetaVisa,tarjetaVisaP1,tasa12MPren,tasa24MPren,tasa36MPren,tasa48MPren,tasa60MPren,tasaCuotaAUPH,tasaFinal,tasaPrestamo,tipoCambio,tipoIngreso,tipoIngresoDetallado,tipoIngresoUtilizadoMinimo,tipoPaquete,tipoPaqueteAltasMasivas,tipoPaqueteRecalifica,tipoPaqueteInteger,tipoProceso,tipoRecalificacion,tipoScoreUtilizado,tipoSolicitud,tipoSolicitudAnterior,tipoTarjetaAmex,tipoTarjetaMaster,tipoTarjetaPDV,tipoTarjetaVisa,tipoTasaPrestamo,tipoUniversidadInteger,totalLimiteSolicitudVsAsistenciaMaxima,visaAMSF,visaOfertada,zona,zonaPDV
where partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision') }}'
and label='Solicitud';
