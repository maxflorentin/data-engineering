"
SET hive.execution.engine=spark;
SET mapred.job.queue.name=root.dataeng ;
----------------------------------------
SELECT 
	RE.cod_ren_unidad unidad ,
	RE.dt_ren_data fechadatos ,
	RE.cod_ren_finalidaddatos finalidaddatos ,
	RE.cod_ren_contrato_rorac idcontratomis ,
	RE.cod_ren_areanegocio idareadenegociolocal ,
	RE.cod_ren_divisa divisa ,
	RE.cod_ren_division iddivision ,
	RE.cod_ren_producto idproductomis ,
	-- '' idru , -- devolucion_españa
	RE.cod_ren_pers_ods idclientemis ,
	-- 23100 idempresabdr , -- devolucion_españa
	-- LPAD(CAST(SUBSTRING(cod_ren_pers_ods,6,8) AS INT),9,'0') idclientebdr , -- devolucion_españa
	SUM(RE.fc_ren_margenmes) margenmes ,
	SUM(RE.fc_ren_sdbsmv) sdbsmv ,
	SUM(RE.fc_ren_sfbsmv) sfbsmv ,
	-- '' eadmmff , -- devolucion_españa
	-- '' flagmmff , -- devolucion_españa
	IF(RE.dt_ren_fec_altacto IS NULL, '2020-06-30', RE.dt_ren_fec_altacto) fecapertura ,
	IF(RE.dt_ren_fec_ven IS NULL, '9999-01-01',RE.dt_ren_fec_ven) fecvcto ,
	RE.cod_ren_entidad_espana identidadmis ,
	RE.cod_ren_centrocont idcentro ,
	RE.cod_ren_subprodu idsubproductooperacional ,
	'' zona ,
	'' territorial ,
	IF(RE.cod_ren_gestorprod = 'N/A', cast(NULL as string), RE.cod_ren_gestorprod) gestorproducto ,
	IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) gestorcliente ,
	-- id_cto_bdr , -- devolucion_españa
	SUM(RE.fc_ren_sdbsfm) sdbsfm ,
	'' sfbsfm ,
	SUM(RE.fc_ren_interes) intereses ,
	SUM(RE.fc_ren_comfin) comfin ,
	SUM(RE.fc_ren_comnofin) comnofin ,
	IF(RE.cod_ren_vincula = 'null', cast(NULL as string), RE.cod_ren_vincula) codvincula ,
	SUM(RE.fc_ren_rof) rof ,
	MAX(RE.fc_ren_tti) tti ,
	'' mtm ,
	'' nocional ,
	RE.cod_ren_divisa_mis coddivmis ,
	RE.flag_ren_moralocal flagmoralocal ,
	RE.flag_ren_carterizadas flagcarterizadas ,
	RE.ds_ren_nombrecliente nombreclien ,
	COALESCE(RE.cod_per_tipopersona, 'F') tippers , -- devolucion_españa
	'' nifcif ,
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL', 1, 0) intragrupo , -- devolucion_españa
	RE.cod_ren_internegocio internegocio ,
	RE.cod_ren_areanegociocorp areadenegociocorp ,
	RE.cod_productogest idproductogestion ,
	RE.cod_segmentogest segmentoclientemis ,
	RE.cod_producto_operacional idproductooperacional ,
	RE.ds_ren_ficheromis ficheromis ,
	RE.dt_ren_fec_reestruc fecrenov ,
	RE.dt_ren_fec_extrdatos fecextrdatosmis ,
	RE.flag_ren_neteo flagneteo ,
	-- fc_ifrs_provision provision , -- devolucion_españa
	-- '' stagedeprovision , -- devolucion_españa
	SUM(RE.fc_ren_orex) orex ,
	'' costesmis ,
	RE.cod_nivel_operacion niveloperacion ,
	'' nombregestorproducto ,
	'' nombregestorcliente 
	-- '' dotacion  -- devolucion_españa
FROM bi_corp_common.bt_ror_input_pasivo RE
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='RORAC_EXPORT_Tables') }}'
GROUP BY RE.cod_ren_unidad ,--unidad ,
	RE.dt_ren_data ,-- fechadatos ,
	RE.cod_ren_finalidaddatos ,-- finalidaddatos ,
	RE.cod_ren_contrato_rorac ,-- idcontratomis ,
	RE.cod_ren_areanegocio ,-- idareadenegociolocal ,
	RE.cod_ren_divisa ,-- divisa ,
	RE.cod_ren_division ,-- iddivision ,
	RE.cod_ren_producto ,-- idproductomis ,
	RE.cod_ren_pers_ods ,-- idclientemis ,
	RE.dt_ren_fec_altacto ,-- fecapertura ,
	IF(RE.dt_ren_fec_ven IS NULL, '9999-01-01',RE.dt_ren_fec_ven) ,-- fecvcto ,
	RE.cod_ren_entidad_espana ,-- identidadmis ,
	RE.cod_ren_centrocont ,-- idcentro ,
	RE.cod_ren_subprodu ,-- idsubproductooperacional ,
	IF(RE.cod_ren_gestorprod = 'N/A', cast(NULL as string), RE.cod_ren_gestorprod) ,-- gestorproducto ,
	IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) ,-- gestorcliente ,
	IF(RE.cod_ren_vincula = 'null', cast(NULL as string), RE.cod_ren_vincula) ,-- codvincula ,
	RE.cod_ren_divisa_mis ,-- coddivmis ,
	RE.flag_ren_moralocal ,-- flagmoralocal ,
	RE.flag_ren_carterizadas ,-- flagcarterizadas ,
	RE.ds_ren_nombrecliente ,-- nombreclien ,
	COALESCE(RE.cod_per_tipopersona, 'F') ,-- tippers , -- devolucion_españa
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL', 1, 0) ,-- intragrupo , -- devolucion_españa
	RE.cod_ren_internegocio ,-- internegocio ,
	RE.cod_ren_areanegociocorp ,-- areadenegociocorp ,
	RE.cod_productogest ,-- idproductogestion ,
	RE.cod_segmentogest ,-- segmentoclientemis ,
	RE.cod_producto_operacional ,-- idproductooperacional ,
	RE.ds_ren_ficheromis ,-- ficheromis ,
	RE.dt_ren_fec_reestruc ,-- fecrenov ,
	RE.dt_ren_fec_extrdatos ,-- fecextrdatosmis ,
	RE.flag_ren_neteo ,-- flagneteo ,
	RE.cod_nivel_operacion ;
"