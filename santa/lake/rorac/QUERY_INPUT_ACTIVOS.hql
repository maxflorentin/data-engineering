-- RORAC --
SELECT 
-- Unidad
	'ARG' unidad ,
-- FechaDatos
	blce.fec_data fecha_dato ,
-- FinalidadDatos
	null finalidad_dato ,
-- IdContratoMis
	blce.idf_cto_ods id_contrato_mis ,
-- IdAreadeNegociolocal
	id_area_negocio_local ,
-- Divisa
	divisa ,
-- n
	n ,
-- IdDivision
	id_division ,
-- IdProductoMis
	id_producto_mis ,
-- IdRU
	id_ru ,
-- IdClienteMis
	id_cliente_mis ,
-- IdEmpresaBdr
	id_empresa_bdr ,
-- IdClienteBdr
	id_cliente_bdr ,
-- MargenMes
	margen_mes ,
-- MargenYTD
	margen_ytd ,
-- MargenHist
	margen_hist ,
-- SdbSMV
	sdb_smv ,
-- SfbSMV
	sfb_smv ,
-- EADMmff
	ead_mmff ,
-- FlagMmff
	flag_mmff ,
-- FecApertura
	fecha_apertura ,
-- FecVcto
	fecha_vencimiento ,
-- IdEntidadMis
	id_entidad_mis ,
-- IdCentro
	id_centro ,
-- IdSubproductoOperacional
	id_subproducto_operacional ,
-- Zona
	zona ,
-- Territorial
	territorial ,
-- GestorProducto
	gestor_producto ,
-- GestorCliente
	gestor_cliente ,
-- IdContratoBdr
	id_contrato_bdr ,
-- SdbSFM
	sdb_sfm ,
-- SfbSFM
	sfb_sfm ,
-- Intereses
	intereses ,
-- ComFin
	com_fim ,
-- ComNoFin
	com_nofin ,
-- CodVincula
	cod_vinculacion ,
-- Rof
	rof ,
-- Tti
	tti ,
-- MtM
	mtm ,
-- Nocional
	nacional ,
-- CodDivMis
	cod_div_mis ,
-- Flagmoralocal
	flag_mora_local ,
-- NuevaProd
	nueva_prod ,
-- FlagCarterizadas
	flag_carterizadas ,
-- NombreClien
	nombre_cliente ,
-- TipPers
	tipo_persona ,
-- NIFCIF
	nifcif ,
-- Intragrupo
	intra_grupo ,
-- Internegocio
	internegocio ,
-- AreaDeNegocioCorp
	area_negocio_corp ,
-- IdProductoGestion
	id_producto_gestion ,
-- SegmentoClienteMIS
	segmento_cliente_mis ,
-- IdProductoOperacional
	id_producto_operacional ,
-- FicheroMis
	fichero_mis ,
-- FecRenov
	fecha_renovacion ,
-- FecExtrDatosMis
	fecha_extr_datos_mis ,
-- FlagNeteo
	flag_neteo ,
-- Orex
	orex ,
-- Provisión  
	provision ,
-- Stage de Provisión 
	stage_provision ,
-- Coste mensual del contrato informado en MIS (en valor absoluto)
	costo_mensual_contrato_mis ,
-- "nivel al que viene informada la operación. Valores posibles: 0 - Contrato 1 - Cliente  "
	nivel_operacion ,
-- Nombre Ejecutivo encargado de la venta de la operación
	ejecutivo_venta ,
-- Nombre Gestor del cliente
	gestor_cliente ,
-- dotacion del contrato (ifrs9)
	dotacion_contrato_ifrs9
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result blce





