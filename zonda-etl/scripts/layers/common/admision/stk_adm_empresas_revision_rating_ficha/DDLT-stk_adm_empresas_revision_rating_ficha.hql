CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_revision_rating_ficha (
    id_adm_ficharevision int,
    id_adm_revisionestado int,
    ds_adm_revisionestado string,
    id_adm_cuestionarioree1 int,
    ds_adm_cuestionarioee1_productodemandamercado string,
    id_adm_tipoproducto int,
    ds_adm_tipoproducto string,
    id_adm_tipodemanda int,
    ds_adm_tipodemanda string,
    id_adm_tipomercado int,
    ds_adm_tipomercado string,
    id_adm_cuestionarioree2 int,
    ds_adm_cuestionarioree2_accionistasgerencia_completo string,
    id_adm_tipoaccionistagerencia1 int,
    ds_adm_identificacionprecisaaccionariado string,
    id_adm_tipoaccionistagerencia2 int,
    ds_adm_valoracionapoyoaccionistas string,
    id_adm_tipoaccionistagerencia3 int,
    ds_adm_valoraciongerencia string,
    id_adm_tipoaccionistagerencia4 int,
    ds_adm_conclusionaccionistagerencia string,
    id_adm_cuestionarioree3 int,
    ds_adm_cuestionarioree3_accesoalcredito_completo string,
    id_adm_tipoaccesoalcredito1 int,
    ds_adm_detalledeudabancaria string,
    id_adm_tipoaccesoalcredito2 int,
    ds_adm_detallerestodeuda string,
    id_adm_tipoaccesoalcredito3 int,
    ds_adm_abanicobancario string,
    id_adm_tipoaccesoalcredito4 int,
    ds_adm_garantias string,
    id_adm_tipoaccesoalcredito5 int,
    ds_adm_conclusionaccesoalcredito string,
    id_adm_cuestionarioree4 int,
    ds_adm_cuestionarioree4_rentabilidadbeneficio_completo string,
    id_adm_tiporentabilidadbeneficio1 int,
    ds_adm_explicacionevolucionventas string,
    id_adm_tiporentabilidadbeneficio2 int,
    ds_adm_recurrenciabeneficio string,
    id_adm_tiporentabilidadbeneficio3 int,
    ds_adm_previsiones string,
    id_adm_tiporentabilidadbeneficio4 int,
    ds_adm_conclusionrentabilidadbeneficios string,
    id_adm_cuestionarioree5 int,
    ds_adm_cuestionarioree5_generacionrecursos_completo string,
    id_adm_tipogeneracionderecursos1 int,
    ds_adm_serviciodeuda string,
    id_adm_tipogeneracionderecursos2 int,
    ds_adm_politicadividendos string,
    id_adm_tipogeneracionderecursos3 int,
    ds_adm_inversionactivosfijos string,
    id_adm_tipogeneracionderecursos4 int,
    ds_adm_conclusiongeneracionfondos string,
    id_adm_cuestionarioree6 int,
    ds_adm_cuestionarioree6_solvencia_completo string,
    id_adm_tiposolvencia1 int,
    ds_adm_adecuacionestructuracapital string,
    id_adm_tiposolvencia2 int,
    ds_adm_adecuacionestructuracirculante string,
    id_adm_tiposolvencia3 int,
    ds_adm_detalleinmueble string,
    id_adm_tiposolvencia4 int,
    ds_adm_verificacionregistrables string,
    id_adm_tiposolvencia5 int,
    ds_adm_conclusionsolvencia string,
    id_adm_tipoproyeccion int,
    ds_adm_tipoproyeccion string,
    id_adm_estadocomentarios int,
    id_adm_seguimiento int,
    ds_adm_seguimiento string,
    id_adm_estadosfinancieros int,
    ds_adm_estadosfinancieros string,
    id_adm_informesectorial int,
    int_adm_informesectorial int,
    id_adm_sistemaopcional int,
    ds_adm_sistemaopcional string,
    id_adm_sistemaopcionalfeve int,
    ds_adm_sistemaopcionalfeve string,
    id_adm_conclusion int,
    ds_adm_conclusion string,
    int_adm_ratingcomentadorta int,
    int_adm_ratingrta int,
    int_adm_calificacioninternarta int,
    int_adm_cuit bigint,
    ds_adm_razonsocial string,
    id_adm_zona string,
    cod_adm_ultimapropuesta bigint,
    cod_adm_sucursal int,
    dec_adm_rating decimal(16,2),
    ds_adm_analista string,
    dec_adm_calificacion decimal(16,2),
    ds_adm_ratingvigente string,
    dec_adm_ratingcomentado decimal(16,2),
    dec_adm_calificacioninterna decimal(16,2),
    ds_adm_regional string,
    ds_adm_cliente string,
    ds_adm_revisorgys string,
    ds_adm_analistarevisor string,
    ds_adm_comentario string,
    ds_adm_comentariocorreccion string,
    cod_per_nup int,
    ts_adm_fechainicio string,
    ts_adm_fechafinalizacion string,
    int_adm_cuestionarioreerta int,
    ds_adm_valoracion string,
    int_adm_ratingresumido int,
    id_adm_zonarating string,
    ds_adm_descripcionzonarating string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_revision_rating_ficha';