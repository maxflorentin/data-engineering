set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

CREATE TEMPORARY TABLE IF NOT EXISTS generic_result_dia AS
SELECT DISTINCT idf_cto_ods
	, cod_contenido
	, cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, IF(cod_producto = 'null', null, cod_producto) cod_producto
	, IMP_AMRT_PRI_ML imp_amrt_pri_ml
	, IMP_AMRT_PRI_MO imp_amrt_pri_mo
	, TAS_INT tas_int
	, PLZ_CONTRACTUAL plz_contractual
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result_dia
WHERE partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Rentabilidad_Daily') }}' ;

CREATE TEMPORARY TABLE IF NOT EXISTS cliente_dia AS
SELECT DISTINCT
	 cod_per_nup
	,cast(null as string) AS  cod_per_vinculacion
	,cast(null as string) AS  ds_per_vinculacion
	,CONCAT(IF(ds_per_nombre = 'null', '', ds_per_nombre),' ',IF(ds_per_apellido= 'null', '', ds_per_apellido)) nombre_cliente
	,flag_per_empleado
	,cod_per_tipopersona
FROM bi_corp_common.stk_per_personas
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_stk_per_personas', dag_id='LOAD_CMN_Rentabilidad_Daily') }}';

CREATE TEMPORARY TABLE IF NOT EXISTS result_je_dia AS
SELECT
	RE.* ,
	adn.cod_ren_area_negocio_niv_1,
	adn.cod_ren_area_negocio_niv_2,
	adn.cod_ren_area_negocio_niv_3,
	adn.cod_ren_area_negocio_niv_4,
	adn.cod_ren_area_negocio_niv_5,
	adn.cod_ren_area_negocio_niv_6,
	adn.cod_ren_area_negocio_niv_7,
	adn.cod_ren_area_negocio_niv_8,
	adn.cod_ren_area_negocio_niv_9,
	adn.ds_ren_area_negocio_niv_1,
	adn.ds_ren_area_negocio_niv_2,
	adn.ds_ren_area_negocio_niv_3,
	adn.ds_ren_area_negocio_niv_4,
	adn.ds_ren_area_negocio_niv_5,
	adn.ds_ren_area_negocio_niv_6,
	adn.ds_ren_area_negocio_niv_7,
	adn.ds_ren_area_negocio_niv_8,
	adn.ds_ren_area_negocio_niv_9,
	prod.cod_ren_producto_niv_1,
	prod.cod_ren_producto_niv_2,
	prod.cod_ren_producto_niv_3,
	prod.cod_ren_producto_niv_4,
	prod.cod_ren_producto_niv_5,
	prod.cod_ren_producto_niv_6,
	prod.cod_ren_producto_niv_7,
	prod.cod_ren_producto_niv_8,
	prod.cod_ren_producto_niv_9,
	prod.cod_ren_producto_niv_10,
	prod.ds_ren_producto_niv_1,
	prod.ds_ren_producto_niv_2,
	prod.ds_ren_producto_niv_3,
	prod.ds_ren_producto_niv_4,
	prod.ds_ren_producto_niv_5,
	prod.ds_ren_producto_niv_6,
	prod.ds_ren_producto_niv_7,
	prod.ds_ren_producto_niv_8,
	prod.ds_ren_producto_niv_9,
	prod.ds_ren_producto_niv_10,
	prod.ds_ren_producto_signo,
	prod2.cod_ren_producto_niv_7 cod_ren_balance_niv_7,
	prod2.ds_ren_producto_niv_7 ds_ren_balance_niv_7,
	cta.cod_ren_cta_resultados_niv_1,
	cta.cod_ren_cta_resultados_niv_2,
	cta.cod_ren_cta_resultados_niv_3,
	cta.cod_ren_cta_resultados_niv_4,
	cta.cod_ren_cta_resultados_niv_5,
	cta.cod_ren_cta_resultados_niv_6,
	cta.cod_ren_cta_resultados_niv_7,
	cta.cod_ren_cta_resultados_niv_8,
	cta.cod_ren_cta_resultados_niv_9,
	cta.cod_ren_cta_resultados_niv_10,
	cta.cod_ren_cta_resultados_niv_11,
	cta.cod_ren_cta_resultados_niv_12,
	cta.ds_ren_cta_resultados_niv_1,
	cta.ds_ren_cta_resultados_niv_2,
	cta.ds_ren_cta_resultados_niv_3,
	cta.ds_ren_cta_resultados_niv_4,
	cta.ds_ren_cta_resultados_niv_5,
	cta.ds_ren_cta_resultados_niv_6,
	cta.ds_ren_cta_resultados_niv_7,
	cta.ds_ren_cta_resultados_niv_8,
	cta.ds_ren_cta_resultados_niv_9,
	cta.ds_ren_cta_resultados_niv_10,
	cta.ds_ren_cta_resultados_niv_11,
	cta.ds_ren_cta_resultados_niv_12
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result_dia RE
-- Productos de libro de negocio
LEFT JOIN bi_corp_common.dim_ren_productosctrldn prod ON
	RE.cod_producto_gest = prod.cod_ren_producto
-- Productos de balance contable
LEFT JOIN bi_corp_common.dim_ren_productosctrco prod2 ON
	RE.cod_producto_gest = prod2.cod_ren_producto
LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn ON
	RE.cod_area_negocio = adn.cod_ren_area_negocio
LEFT JOIN bi_corp_common.dim_ren_ctaresultadosctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Rentabilidad_Daily') }}' ;

CREATE TEMPORARY TABLE IF NOT EXISTS result_je2_dia AS
SELECT
	RE.* ,
	ent.cod_ren_entidad_gest_niv_1,
	ent.cod_ren_entidad_gest_niv_2,
	ent.cod_ren_entidad_gest_niv_3,
	ent.cod_ren_entidad_gest_niv_4,
	ent.cod_ren_entidad_gest_niv_5,
	ent.cod_ren_entidad_gest_niv_6,
	ent.ds_ren_entidad_gest_niv_1,
	ent.ds_ren_entidad_gest_niv_2,
	ent.ds_ren_entidad_gest_niv_3,
	ent.ds_ren_entidad_gest_niv_4,
	ent.ds_ren_entidad_gest_niv_5,
	ent.ds_ren_entidad_gest_niv_6,
	offi.cod_ren_ofi_comercial_niv_5 ,
	offi.cod_ren_ofi_comercial_niv_6 ,
	offi.cod_ren_ofi_comercial_niv_7 ,
	offi.cod_ren_ofi_comercial_niv_8 ,
	offi.cod_ren_ofi_comercial_niv_9 ,
	offi.cod_ren_ofi_comercial_niv_10 ,
	offi.cod_ren_ofi_comercial_niv_11 ,
	offi.ds_ren_ofi_comercial_niv_5 ,
	offi.ds_ren_ofi_comercial_niv_6 ,
	offi.ds_ren_ofi_comercial_niv_7 ,
	offi.ds_ren_ofi_comercial_niv_8 ,
	offi.ds_ren_ofi_comercial_niv_9 ,
	offi.ds_ren_ofi_comercial_niv_10 ,
	offi.ds_ren_ofi_comercial_niv_11 ,
	ges.cod_ren_gestor_niv_1 ,
	ges.cod_ren_gestor_niv_2 ,
	ges.cod_ren_gestor_niv_3 ,
	ges.cod_ren_gestor_niv_4 ,
	ges.cod_ren_gestor_niv_5 ,
	ges.cod_ren_gestor_niv_6 ,
	ges.cod_ren_gestor_niv_7 ,
	ges.cod_ren_gestor_niv_8 ,
	ges.cod_ren_gestor_niv_9 ,
	ges.cod_ren_gestor_niv_10 ,
	ges.cod_ren_gestor_niv_11 ,
	ges.ds_ren_gestor_niv_1 ,
	ges.ds_ren_gestor_niv_2 ,
	ges.ds_ren_gestor_niv_3 ,
	ges.ds_ren_gestor_niv_4 ,
	ges.ds_ren_gestor_niv_5 ,
	ges.ds_ren_gestor_niv_6 ,
	ges.ds_ren_gestor_niv_7 ,
	ges.ds_ren_gestor_niv_8 ,
	ges.ds_ren_gestor_niv_9 ,
	ges.ds_ren_gestor_niv_10 ,
	ges.ds_ren_gestor_niv_11
FROM result_je_dia RE
JOIN bi_corp_common.dim_ren_entidadjuridica ent ON
	RE.cod_entidad_espana = ent.cod_ren_entidad_gest
LEFT JOIN bi_corp_common.dim_ren_oficinas offi ON
	RE.cod_ofi_comercial = offi.cod_ren_ofi_comercial
LEFT JOIN bi_corp_common.dim_ren_gestor ges ON
	RE.cod_gestor = ges.cod_ren_gestor	;

CREATE TEMPORARY TABLE IF NOT EXISTS result_g_dia AS
SELECT RE.*
	, gen.cod_producto
	, gen.fec_alta_cto
	, gen.fec_ven
	, gen.fec_reestruc
	, gen.imp_amrt_pri_ml
	, gen.imp_amrt_pri_mo
	, gen.tas_int
	, gen.plz_contractual
FROM result_je2_dia RE
JOIN generic_result_dia gen ON
	gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
	AND RE.idf_cto_ods = gen.idf_cto_ods
	AND RE.cod_contenido = gen.cod_contenido
WHERE gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
AND RE.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
UNION ALL
SELECT RE.*
	, cast(null as string) AS cod_producto
	, cast(null as string) AS  fec_alta_cto
	, cast(null as string) AS  fec_ven
	, cast(null as string) AS  fec_reestruc
	, cast(null as string) AS  imp_amrt_pri_ml
	, cast(null as string) AS  imp_amrt_pri_mo
	, cast(null as string) AS  tas_int
	, cast(null as string) AS  plz_contractual
FROM result_je2_dia RE
WHERE RE.cod_contenido IN ('GAS','RSM','CTB','CCL');

CREATE TEMPORARY TABLE IF NOT EXISTS result_gc_dia AS
SELECT RE.*
	, cli.cod_per_vinculacion
	, cli.ds_per_vinculacion
	, cli.nombre_cliente
	, cli.flag_per_empleado
	, cli.cod_per_tipopersona
FROM result_g_dia RE
JOIN cliente_dia cli ON
	cli.cod_per_nup = CAST(SUBSTR(re.idf_pers_ods, 6, 8) AS INT)
WHERE re.idf_pers_ods not in (-99100, -99101)
UNION ALL
SELECT RE.*
	, cast(null as string) AS  cod_per_vinculacion
	, cast(null as string) AS  ds_per_vinculacion
	, cast(null as string) AS  nombre_cliente
	, cast(null as string) AS  flag_per_empleado
	, cast(null as string) AS  cod_per_tipopersona
FROM result_g_dia RE
WHERE re.idf_pers_ods in (-99100, -99101);

DROP TABLE IF EXISTS generic_result_dia ;
DROP TABLE IF EXISTS cliente_dia ;
DROP TABLE IF EXISTS result_g_dia ;

INSERT OVERWRITE TABLE bi_corp_common.bt_ren_result_dia
PARTITION(partition_date)
SELECT RE.idf_cto_ods cod_ren_contrato,
	RE.fec_alta_cto dt_ren_altacontrato,
	RE.fec_ven dt_ren_vencontrato,
	RE.fec_reestruc dt_ren_reestruccontrato,
	COALESCE(RE.tas_int,0) fc_ren_tasint,
	COALESCE(RE.plz_contractual, 0) fc_ren_plazcontractual,
	RE.cod_producto cod_ren_producto_generic,
	RE.cod_contenido cod_ren_contenido,
	from_unixtime(unix_timestamp(RE.fec_data , 'yyyyMMdd'), 'yyyy-MM-dd') dt_ren_fechadata,
	RE.cod_pais cod_ren_pais,
	RE.idf_pers_ods cod_ren_pers_ods,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8)AS INT) cod_per_nup,
	RE.cod_per_vinculacion cod_ren_vinculacion,
	RE.ds_per_vinculacion ds_per_vinculacion,
	RE.cod_per_tipopersona cod_per_tipopersona,
	RE.nombre_cliente ds_per_nombre_apellido,
	RE.flag_per_empleado  flag_per_empleado,
	RE.cod_entidad_espana cod_ren_entidad_espana,
	RE.cod_producto_gest cod_ren_producto_gest,
	RE.cod_divisa cod_ren_divisa,
	IF(RE.cod_reajuste = 'null', null, RE.cod_reajuste) cod_ren_reajuste,
	RE.cod_agrp_func cod_ren_agrp_func,
	RE.cod_est_sdo cod_ren_est_sdo,
	RE.cod_cta_cont_gestion cod_ren_cont_gestion,
	RE.cod_area_negocio cod_ren_area_negocio,
	RE.cod_tip_ajuste cod_ren_tipo_ajuste,
	RE.cod_centro_cont cod_ren_centro_cont,
	RE.cod_ofi_comercial cod_ren_ofi_comercial,
	RE.ind_conciliacion cod_ren_conciliacion,
	IF(RE.ind_pool = 'S',1,0) flag_ren_ind_pool,
	RE.cod_segmento_gest cod_ren_segmento_gest,
	RE.cod_gestor cod_ren_gestor,
	IF(RE.cod_gestor_prod = 'null', null, RE.cod_gestor_prod) cod_ren_gestor_prod,
	RE.cod_origen_inf cod_ren_origen_inf,
	RE.cod_ren_producto_niv_1 ,
	RE.cod_ren_producto_niv_2 ,
	RE.cod_ren_producto_niv_3 ,
	RE.cod_ren_producto_niv_4 ,
	RE.cod_ren_producto_niv_5 ,
	RE.cod_ren_producto_niv_6 ,
	RE.cod_ren_producto_niv_7 ,
	RE.cod_ren_producto_niv_8 ,
	RE.cod_ren_producto_niv_9 ,
	RE.cod_ren_producto_niv_10 ,
	RE.ds_ren_producto_niv_1 ,
	RE.ds_ren_producto_niv_2 ,
	RE.ds_ren_producto_niv_3 ,
	RE.ds_ren_producto_niv_4 ,
	RE.ds_ren_producto_niv_5 ,
	RE.ds_ren_producto_niv_6 ,
	RE.ds_ren_producto_niv_7 ,
	RE.ds_ren_producto_niv_8 ,
	RE.ds_ren_producto_niv_9 ,
	RE.ds_ren_producto_niv_10 ,
	RE.cod_ren_balance_niv_7,
	RE.ds_ren_balance_niv_7,
	RE.cod_ren_cta_resultados_niv_1 ,
	RE.cod_ren_cta_resultados_niv_2 ,
	RE.cod_ren_cta_resultados_niv_3 ,
	RE.cod_ren_cta_resultados_niv_4 ,
	RE.cod_ren_cta_resultados_niv_5 ,
	RE.cod_ren_cta_resultados_niv_6 ,
	RE.cod_ren_cta_resultados_niv_7 ,
	RE.cod_ren_cta_resultados_niv_8 ,
	RE.cod_ren_cta_resultados_niv_9 ,
	RE.cod_ren_cta_resultados_niv_10 ,
	RE.cod_ren_cta_resultados_niv_11 ,
	RE.cod_ren_cta_resultados_niv_12 ,
	RE.ds_ren_cta_resultados_niv_1 ,
	RE.ds_ren_cta_resultados_niv_2 ,
	RE.ds_ren_cta_resultados_niv_3 ,
	RE.ds_ren_cta_resultados_niv_4 ,
	RE.ds_ren_cta_resultados_niv_5 ,
	RE.ds_ren_cta_resultados_niv_6 ,
	RE.ds_ren_cta_resultados_niv_7 ,
	RE.ds_ren_cta_resultados_niv_8 ,
	RE.ds_ren_cta_resultados_niv_9 ,
	RE.ds_ren_cta_resultados_niv_10 ,
	RE.ds_ren_cta_resultados_niv_11 ,
	RE.ds_ren_cta_resultados_niv_12 ,
	RE.cod_ren_area_negocio_niv_1 ,
	RE.cod_ren_area_negocio_niv_2 ,
	RE.cod_ren_area_negocio_niv_3 ,
	RE.cod_ren_area_negocio_niv_4 ,
	RE.cod_ren_area_negocio_niv_5 ,
	RE.cod_ren_area_negocio_niv_6 ,
	RE.cod_ren_area_negocio_niv_7 ,
	RE.cod_ren_area_negocio_niv_8 ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.ds_ren_area_negocio_niv_1 ,
	RE.ds_ren_area_negocio_niv_2 ,
	RE.ds_ren_area_negocio_niv_3 ,
	RE.ds_ren_area_negocio_niv_4 ,
	RE.ds_ren_area_negocio_niv_5 ,
	RE.ds_ren_area_negocio_niv_6 ,
	RE.ds_ren_area_negocio_niv_7 ,
	RE.ds_ren_area_negocio_niv_8 ,
	RE.ds_ren_area_negocio_niv_9 ,
	RE.cod_ren_entidad_gest_niv_1 ,
	RE.cod_ren_entidad_gest_niv_2 ,
	RE.cod_ren_entidad_gest_niv_3 ,
	RE.cod_ren_entidad_gest_niv_4 ,
	RE.cod_ren_entidad_gest_niv_5 ,
	RE.cod_ren_entidad_gest_niv_6 ,
	RE.ds_ren_entidad_gest_niv_1 ,
	RE.ds_ren_entidad_gest_niv_2 ,
	RE.ds_ren_entidad_gest_niv_3 ,
	RE.ds_ren_entidad_gest_niv_4 ,
	RE.ds_ren_entidad_gest_niv_5 ,
	RE.ds_ren_entidad_gest_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_5 ,
	RE.cod_ren_ofi_comercial_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_7 ,
	RE.cod_ren_ofi_comercial_niv_8 ,
	RE.cod_ren_ofi_comercial_niv_9 ,
	RE.cod_ren_ofi_comercial_niv_10 ,
	RE.cod_ren_ofi_comercial_niv_11 ,
	RE.ds_ren_ofi_comercial_niv_5 ,
	RE.ds_ren_ofi_comercial_niv_6 ,
	RE.ds_ren_ofi_comercial_niv_7 ,
	RE.ds_ren_ofi_comercial_niv_8 ,
	RE.ds_ren_ofi_comercial_niv_9 ,
	RE.ds_ren_ofi_comercial_niv_10 ,
	RE.ds_ren_ofi_comercial_niv_11 ,
	RE.cod_ren_gestor_niv_1 ,
	RE.cod_ren_gestor_niv_2 ,
	RE.cod_ren_gestor_niv_3 ,
	RE.cod_ren_gestor_niv_4 ,
	RE.cod_ren_gestor_niv_5 ,
	RE.cod_ren_gestor_niv_6 ,
	RE.cod_ren_gestor_niv_7 ,
	RE.cod_ren_gestor_niv_8 ,
	RE.cod_ren_gestor_niv_9 ,
	RE.cod_ren_gestor_niv_10 ,
	RE.cod_ren_gestor_niv_11 ,
	RE.ds_ren_gestor_niv_1 ,
	RE.ds_ren_gestor_niv_2 ,
	RE.ds_ren_gestor_niv_3 ,
	RE.ds_ren_gestor_niv_4 ,
	RE.ds_ren_gestor_niv_5 ,
	RE.ds_ren_gestor_niv_6 ,
	RE.ds_ren_gestor_niv_7 ,
	RE.ds_ren_gestor_niv_8 ,
	RE.ds_ren_gestor_niv_9 ,
	RE.ds_ren_gestor_niv_10 ,
	RE.ds_ren_gestor_niv_11 ,
	sum(COALESCE(RE.imp_amrt_pri_ml, 0)) fc_ren_amrt_pri_ml,
	sum(COALESCE(RE.imp_amrt_pri_mo, 0)) fc_ren_amrt_pri_mo,
	COALESCE(RE.out_tti, 0) fc_ren_out_tti,
	sum(COALESCE(RE.imp_ing_per_ml, 0)) fc_ren_ing_per_ml,
	sum(COALESCE(RE.imp_ing_per_mo, 0)) fc_ren_ing_per_mo,
	sum(COALESCE(RE.imp_ing_cap_ml, 0)) fc_ren_ing_cap_ml,
	sum(COALESCE(RE.imp_ing_cap_mo, 0)) fc_ren_ing_cap_mo,
	sum(COALESCE(RE.imp_ing_reaj_ml, 0)) fc_ren_ing_reaj_ml,
	sum(COALESCE(RE.imp_egr_per_ml, 0)) fc_ren_egr_per_ml,
	sum(COALESCE(RE.imp_egr_cap_ml, 0)) fc_ren_egr_cap_ml,
	sum(COALESCE(RE.imp_egr_reaj_ml, 0)) fc_ren_egr_reaj_ml,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_ml, 0)) fc_ren_ajtti_egr_tb_cap_ml,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_ml, 0)) fc_ren_ajtti_egr_sl_cap_ml,
	sum(COALESCE(RE.imp_ajtti_egr_per_ml, 0)) fc_ren_ajtti_egr_per_ml,
	sum(COALESCE(RE.imp_ajtti_egr_reajuste_ml, 0))  fc_ren_ajtti_egr_reajuste_ml,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_ml, 0))  fc_ren_ajtti_ing_tb_cap_ml,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_ml, 0))  fc_ren_ajtti_ing_sl_cap_ml,
	sum(COALESCE(RE.imp_ajtti_ing_per_ml, 0)) fc_ren_ajtti_ing_per_ml,
	sum(COALESCE(RE.imp_ajtti_ing_reajuste_ml, 0))  fc_ren_ajtti_ing_reajuste_ml,
	sum(COALESCE(RE.imp_efec_enc_ml, 0))  fc_ren_efec_enc_ml,
	sum(COALESCE(RE.imp_ing_reaj_mo, 0))  fc_ren_ing_reaj_mo,
	sum(COALESCE(RE.imp_egr_per_mo, 0)) fc_ren_egr_per_mo,
	sum(COALESCE(RE.imp_egr_cap_mo, 0))  fc_ren_egr_cap_mo,
	sum(COALESCE(RE.imp_egr_reaj_mo, 0)) fc_ren_egr_reaj_mo,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_mo, 0))  fc_ren_ajtti_egr_tb_cap_mo,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_mo, 0)) fc_ren_ajtti_egr_sl_cap_mo,
	sum(COALESCE(RE.imp_ajtti_egr_per_mo, 0))  fc_ren_ajtti_egr_per_mo,
	sum(COALESCE(RE.imp_ajtti_egr_reajuste_mo, 0))  fc_ren_ajtti_egr_reajuste_mo,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_mo, 0))  fc_ren_ajtti_ing_tb_cap_mo,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_mo, 0))  fc_ren_ajtti_ing_sl_cap_mo,
	sum(COALESCE(RE.imp_ajtti_ing_per_mo, 0)) fc_ren_ajtti_ing_per_mo,
	sum(COALESCE(RE.imp_ajtti_ing_reajuste_mo, 0))  fc_ren_ajtti_ing_reajuste_mo,
	sum(COALESCE(RE.imp_efec_enc_mo, 0))  fc_ren_efec_enc_mo,
	sum(COALESCE(RE.imp_sdo_cap_med_ml, 0))  * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_cap_ml,
	sum(COALESCE(RE.imp_sdo_med_int_ml, 0))  * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_int_ml,
	sum(COALESCE(RE.imp_sdo_med_reajuste_ml, 0))  * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_reajuste_ml,
	sum(COALESCE(RE.imp_sdo_cap_med_mo, 0))  * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_cap_mo,
	sum(COALESCE(RE.imp_sdo_med_int_mo, 0))  * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_int_mo,
	sum(COALESCE(RE.imp_sdo_med_reajuste_mo, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_med_reajuste_mo,
	sum(COALESCE(RE.imp_sdo_cap_ml, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_cap_ml ,
	sum(COALESCE(RE.imp_sdo_cap_mo, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_cap_mo ,
	sum(COALESCE(RE.imp_sdo_int_ml, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_int_ml ,
	sum(COALESCE(RE.imp_sdo_int_mo, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_int_mo ,
	sum(COALESCE(RE.imp_sdo_reajuste_ml, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_reajuste_ml ,
	sum(COALESCE(RE.imp_sdo_reajuste_mo, 0)) * (RE.signo * RE.ds_ren_producto_signo) fc_ren_sdo_reajuste_mo ,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_sea_ml, 0)) fc_ren_ajtti_ing_tb_cap_sea_ml ,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_sea_mo, 0)) fc_ren_ajtti_ing_tb_cap_sea_mo ,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_sea_ml, 0)) fc_ren_ajtti_ing_sl_cap_sea_ml,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_sea_mo, 0)) fc_ren_ajtti_ing_sl_cap_sea_mo,
	sum(COALESCE(RE.imp_ajtti_ing_per_sea_ml, 0)) fc_ren_ajtti_ing_per_sea_ml,
	sum(COALESCE(RE.imp_ajtti_ing_per_sea_mo, 0)) fc_ren_ajtti_ing_per_sea_mo,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_sea_ml, 0)) fc_ren_ajtti_egr_tb_cap_sea_ml,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_sea_mo, 0)) fc_ren_ajtti_egr_tb_cap_sea_mo,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_sea_ml, 0)) fc_ren_ajtti_egr_sl_cap_sea_ml,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_sea_mo, 0)) fc_ren_ajtti_egr_sl_cap_sea_mo,
	sum(COALESCE(RE.imp_ajtti_egr_per_sea_ml, 0)) fc_ren_ajtti_egr_per_sea_ml,
	sum(COALESCE(RE.imp_ajtti_egr_per_sea_mo, 0)) fc_ren_ajtti_egr_per_sea_mo,
	sum(COALESCE(RE.imp_efec_enc_sea_ml, 0)) fc_ren_efec_enc_sea_ml,
	sum(COALESCE(RE.imp_efec_enc_sea_mo, 0)) fc_ren_efec_enc_sea_mo,
	((sum(COALESCE(RE.IMP_SDO_CAP_ML, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_INT_ML, 0))  * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0)) * (RE.signo * RE.ds_ren_producto_signo))) AS fc_ren_saldo_puntual_ml,
    ((sum(COALESCE(RE.IMP_SDO_CAP_MO, 0))  * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0)) * (RE.signo * RE.ds_ren_producto_signo))) AS fc_ren_saldo_puntual_mo,
    ((sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) * (RE.signo * RE.ds_ren_producto_signo))) AS fc_ren_saldo_medio_ml,
    ((sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) * (RE.signo * RE.ds_ren_producto_signo))
			+ (sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) * (RE.signo * RE.ds_ren_producto_signo))) AS fc_ren_saldo_medio_mo,
    (sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0))
            + sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0))) AS fc_ren_ing_total_ficticio_ml,
    (sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0))
            + sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0))) AS fc_ren_ing_total_ficticio_mo,
    (sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0))) AS fc_ren_egr_total_ficticio_ml,
    (sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0))
            + sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0))) AS fc_ren_egr_total_ficticio_mo,
    (sum(COALESCE(RE.IMP_ING_PER_ML, 0))
            + sum(COALESCE(RE.IMP_ING_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_ING_REAJ_ML, 0))) AS fc_ren_ing_total_real_ml,
    (sum(COALESCE(RE.IMP_ING_PER_MO, 0))
            + sum(COALESCE(RE.IMP_ING_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_ING_REAJ_MO, 0))) AS fc_ren_ing_total_real_mo,
    (sum(COALESCE(RE.IMP_EGR_PER_ML,0))
            + sum(COALESCE(RE.IMP_EGR_CAP_ML, 0))
            + sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0))) AS fc_ren_egr_total_real_ml,
    (sum(COALESCE(RE.IMP_EGR_PER_MO,0))
            + sum(COALESCE(RE.IMP_EGR_CAP_MO, 0))
            + sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0))) AS fc_ren_egr_total_real_mo,
	RE.signo cod_ren_signo,
	RE.cod_sis_origen cod_ren_sis_origen,
	RE.partition_date
FROM result_gc_dia RE
GROUP BY RE.idf_cto_ods ,
	RE.fec_alta_cto ,
	RE.fec_ven ,
	RE.fec_reestruc ,
	COALESCE(RE.tas_int,0),
	COALESCE(RE.plz_contractual, 0),
	RE.cod_producto ,
	RE.cod_contenido ,
	RE.fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8)AS INT) ,
	RE.cod_per_vinculacion,
	RE.ds_per_vinculacion,
	RE.cod_per_tipopersona,
	RE.nombre_cliente,
	RE.flag_per_empleado,
	RE.cod_entidad_espana,
	RE.cod_producto_gest,
	RE.cod_divisa,
	IF(RE.cod_reajuste = 'null', null, RE.cod_reajuste),
	RE.cod_agrp_func ,
	RE.cod_est_sdo ,
	RE.cod_cta_cont_gestion ,
	RE.cod_area_negocio ,
	RE.cod_tip_ajuste ,
	RE.cod_centro_cont ,
	RE.cod_ofi_comercial ,
	RE.ind_conciliacion ,
	IF(RE.ind_pool = 'S',1,0) ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	IF(RE.cod_gestor_prod = 'null', null, RE.cod_gestor_prod) ,
	RE.cod_origen_inf ,
	RE.cod_ren_producto_niv_1 ,
	RE.cod_ren_producto_niv_2 ,
	RE.cod_ren_producto_niv_3 ,
	RE.cod_ren_producto_niv_4 ,
	RE.cod_ren_producto_niv_5 ,
	RE.cod_ren_producto_niv_6 ,
	RE.cod_ren_producto_niv_7 ,
	RE.cod_ren_producto_niv_8 ,
	RE.cod_ren_producto_niv_9 ,
	RE.cod_ren_producto_niv_10 ,
	RE.ds_ren_producto_niv_1 ,
	RE.ds_ren_producto_niv_2 ,
	RE.ds_ren_producto_niv_3 ,
	RE.ds_ren_producto_niv_4 ,
	RE.ds_ren_producto_niv_5 ,
	RE.ds_ren_producto_niv_6 ,
	RE.ds_ren_producto_niv_7 ,
	RE.ds_ren_producto_niv_8 ,
	RE.ds_ren_producto_niv_9 ,
	RE.ds_ren_producto_niv_10 ,
	RE.cod_ren_balance_niv_7,
	RE.ds_ren_balance_niv_7,
	RE.cod_ren_cta_resultados_niv_1 ,
	RE.cod_ren_cta_resultados_niv_2 ,
	RE.cod_ren_cta_resultados_niv_3 ,
	RE.cod_ren_cta_resultados_niv_4 ,
	RE.cod_ren_cta_resultados_niv_5 ,
	RE.cod_ren_cta_resultados_niv_6 ,
	RE.cod_ren_cta_resultados_niv_7 ,
	RE.cod_ren_cta_resultados_niv_8 ,
	RE.cod_ren_cta_resultados_niv_9 ,
	RE.cod_ren_cta_resultados_niv_10 ,
	RE.cod_ren_cta_resultados_niv_11 ,
	RE.cod_ren_cta_resultados_niv_12 ,
	RE.ds_ren_cta_resultados_niv_1 ,
	RE.ds_ren_cta_resultados_niv_2 ,
	RE.ds_ren_cta_resultados_niv_3 ,
	RE.ds_ren_cta_resultados_niv_4 ,
	RE.ds_ren_cta_resultados_niv_5 ,
	RE.ds_ren_cta_resultados_niv_6 ,
	RE.ds_ren_cta_resultados_niv_7 ,
	RE.ds_ren_cta_resultados_niv_8 ,
	RE.ds_ren_cta_resultados_niv_9 ,
	RE.ds_ren_cta_resultados_niv_10 ,
	RE.ds_ren_cta_resultados_niv_11 ,
	RE.ds_ren_cta_resultados_niv_12 ,
	RE.cod_ren_area_negocio_niv_1 ,
	RE.cod_ren_area_negocio_niv_2 ,
	RE.cod_ren_area_negocio_niv_3 ,
	RE.cod_ren_area_negocio_niv_4 ,
	RE.cod_ren_area_negocio_niv_5 ,
	RE.cod_ren_area_negocio_niv_6 ,
	RE.cod_ren_area_negocio_niv_7 ,
	RE.cod_ren_area_negocio_niv_8 ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.ds_ren_area_negocio_niv_1 ,
	RE.ds_ren_area_negocio_niv_2 ,
	RE.ds_ren_area_negocio_niv_3 ,
	RE.ds_ren_area_negocio_niv_4 ,
	RE.ds_ren_area_negocio_niv_5 ,
	RE.ds_ren_area_negocio_niv_6 ,
	RE.ds_ren_area_negocio_niv_7 ,
	RE.ds_ren_area_negocio_niv_8 ,
	RE.ds_ren_area_negocio_niv_9 ,
	RE.cod_ren_entidad_gest_niv_1 ,
	RE.cod_ren_entidad_gest_niv_2 ,
	RE.cod_ren_entidad_gest_niv_3 ,
	RE.cod_ren_entidad_gest_niv_4 ,
	RE.cod_ren_entidad_gest_niv_5 ,
	RE.cod_ren_entidad_gest_niv_6 ,
	RE.ds_ren_entidad_gest_niv_1 ,
	RE.ds_ren_entidad_gest_niv_2 ,
	RE.ds_ren_entidad_gest_niv_3 ,
	RE.ds_ren_entidad_gest_niv_4 ,
	RE.ds_ren_entidad_gest_niv_5 ,
	RE.ds_ren_entidad_gest_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_5 ,
	RE.cod_ren_ofi_comercial_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_7 ,
	RE.cod_ren_ofi_comercial_niv_8 ,
	RE.cod_ren_ofi_comercial_niv_9 ,
	RE.cod_ren_ofi_comercial_niv_10 ,
	RE.cod_ren_ofi_comercial_niv_11 ,
	RE.ds_ren_ofi_comercial_niv_5 ,
	RE.ds_ren_ofi_comercial_niv_6 ,
	RE.ds_ren_ofi_comercial_niv_7 ,
	RE.ds_ren_ofi_comercial_niv_8 ,
	RE.ds_ren_ofi_comercial_niv_9 ,
	RE.ds_ren_ofi_comercial_niv_10 ,
	RE.ds_ren_ofi_comercial_niv_11 ,
	RE.cod_ren_gestor_niv_1 ,
	RE.cod_ren_gestor_niv_2 ,
	RE.cod_ren_gestor_niv_3 ,
	RE.cod_ren_gestor_niv_4 ,
	RE.cod_ren_gestor_niv_5 ,
	RE.cod_ren_gestor_niv_6 ,
	RE.cod_ren_gestor_niv_7 ,
	RE.cod_ren_gestor_niv_8 ,
	RE.cod_ren_gestor_niv_9 ,
	RE.cod_ren_gestor_niv_10 ,
	RE.cod_ren_gestor_niv_11 ,
	RE.ds_ren_gestor_niv_1 ,
	RE.ds_ren_gestor_niv_2 ,
	RE.ds_ren_gestor_niv_3 ,
	RE.ds_ren_gestor_niv_4 ,
	RE.ds_ren_gestor_niv_5 ,
	RE.ds_ren_gestor_niv_6 ,
	RE.ds_ren_gestor_niv_7 ,
	RE.ds_ren_gestor_niv_8 ,
	RE.ds_ren_gestor_niv_9 ,
	RE.ds_ren_gestor_niv_10 ,
	RE.ds_ren_gestor_niv_11 ,
	COALESCE(RE.out_tti, 0),
	RE.signo ,
	RE.cod_sis_origen ,
	RE.partition_date,
	RE.ds_ren_producto_signo ;