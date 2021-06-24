 set hive.merge.mapfiles=true;
 set hive.merge.mapredfiles=true;
 set hive.merge.size.per.task=4000000;
 set hive.merge.smallfiles.avgsize=16000000;
 set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

 ------------------------------------------------------------------------------------------------------------------------
 --GARANTIAS ESPECIFICAS + HIPOTECAS UVA

--Paso1:
    -- Reuno los datos de las garantias especificas y tambien convierto a pesos el limite de la garantia.
    -- nivelar particiones de ABKT y GTDTCOD anteriores a 2020-04-30

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_especificas_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
 SELECT
	  MAESTRO.gttcmae_mae_cod_entidad as cod_entidad
	, rgo1.id_cto_div			as id_cto_div
 	, CAST(MAESTRO.gttcmae_mae_num_garantia	as string) as num_garantia
 	, MAESTRO.gttcmae_mae_cod_estado as cod_estado
 	, MAESTRO.gttcmae_mae_fec_alta	as fec_alta
 	, MAESTRO.gttcmae_mae_fec_vcto	as fec_vcto
 	, MAESTRO.gttcmae_mae_cod_garantia	as cod_garantia
 	, MAESTRO.gttcmae_mae_tip_cobertur		as tip_cobertur
 	, cast(null as string)		as num_bien
 	, MAESTRO.gttcgar_gar_cla_garantia as cla_garantia
	, cast(null as string) as tip_instrumentacion
	, cast(null as string) as pignoracion
	, cast(null as string) as tip_aval
 	, MAESTRO.gttcmae_mae_cod_divisa	 as cod_divisa
 	, MAESTRO.gttcmae_mae_imp_limite	as imp_limite
 	, hip.gttcvab_vab_tip_valor		as tip_valor
 	, hip.gttcvab_vab_imp_total		as imp_total
 	, CASE
		WHEN (MAESTRO.gttcgar_gar_cla_garantia = '001' and hip.gttcvab_vab_tip_valor ='015')
			THEN hip.gttcvab_vab_imp_total
			ELSE MAESTRO.gttcmae_mae_imp_limite
			END
	  as imp_total_garantia
 	, CAST(
 		CASE WHEN (MAESTRO.gttcgar_gar_cla_garantia = '001' and hip.gttcvab_vab_tip_valor ='015')
 	    	THEN IF (MAESTRO.gttcmae_mae_cod_divisa !='ARS' , (hip.gttcvab_vab_imp_total * cotizaciones.imp_cambio_fijo_vigente) ,  hip.gttcvab_vab_imp_total)
         	ELSE
				IF (MAESTRO.gttcmae_mae_cod_divisa !='ARS' , (MAESTRO.gttcmae_mae_imp_limite * cotizaciones.imp_cambio_fijo_vigente) , MAESTRO.gttcmae_mae_imp_limite)
       	END as DECIMAL(19,4)
	  ) as imp_total_garantia_pesos
	, IF (MAESTRO.gttcmae_mae_cod_divisa !='ARS' ,cotizaciones.imp_cambio_fijo_vigente,1)
	  as imp_cambio_fijo_vigente
	, rgo1.fec_bajrelac	as fec_bajrelac
	, rgo1.ind_principa as ind_principa
	, cast(null as string) as orden
	, cast(null as string) as tip_gara_bdr
    , cast(null as string) as cod_garantia_bdr
    	, cast(null as string) as num_persona_garante
    	, cast(null as string) as pais_iso_garante
    	, cast(null as string) as cod_bien
    	, cast(null as string) as cod_postal
 FROM
 	(SELECT
 		  mae.partition_date
		, mae.gttcmae_mae_cod_entidad
 		, mae.gttcmae_mae_num_garantia
 		, mae.gttcmae_mae_cod_estado
 		, mae.gttcmae_mae_fec_alta
 		, mae.gttcmae_mae_fec_vcto
 		, mae.gttcmae_mae_cod_divisa
 		, mae.gttcmae_mae_imp_limite
 		, mae.gttcmae_mae_cod_garantia
 		, mae.gttcmae_mae_tip_cobertur
 		, gar.gttcgar_gar_cla_garantia
 	FROM 	bi_corp_staging.gtdtmae mae
 		LEFT OUTER JOIN bi_corp_staging.gtdtgar gar
 		ON (mae.partition_date = gar.partition_date and mae.gttcmae_mae_cod_garantia = gar.gttcgar_gar_cod_garantia and gar.gttcgar_gar_cla_garantia = '001')
 	WHERE 1=1
 	    and mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
 	    and mae.gttcmae_mae_cod_estado = '020'
        and mae.gttcmae_mae_cod_garantia != '019'
 	)  MAESTRO
	INNER JOIN
				(SELECT
						rgo.partition_date
						, rgo.gttcrgo_rgo_num_garantia as num_garantia
						, concat_ws('_' , rgo.gttcrgo_rgo_contrato_rgo_cod_entcont, rgo.gttcrgo_rgo_contrato_rgo_cod_oficont, rgo.gttcrgo_rgo_contrato_rgo_num_ctacont, rgo.gttcrgo_rgo_contrato_rgo_cod_producto
									, rgo.gttcrgo_rgo_contrato_rgo_cod_subprod--, rgo.gttcrgo_rgo_contrato_rgo_cod_divcont
									) as id_cto_div
						, rgo.gttcrgo_rgo_fec_altrelac as fec_altrelac
						, rgo.gttcrgo_rgo_fec_bajrelac as fec_bajrelac
						, rgo.gttcrgo_rgo_tip_cobertur as tip_cobertur
						, rgo.gttcrgo_rgo_ind_principa as ind_principa
					FROM
						 bi_corp_staging.gtdtrgo  rgo
					WHERE
						rgo.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
						and (rgo.gttcrgo_rgo_fec_altrelac <='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' and rgo.gttcrgo_rgo_fec_bajrelac >='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}' )
						and rgo.gttcrgo_rgo_tip_cobertur = '001' --> COBERTURA ESPECIFICA
				) rgo1
					ON (rgo1.num_garantia = MAESTRO.gttcmae_mae_num_garantia and rgo1.partition_date = MAESTRO.partition_date)
 	LEFT OUTER JOIN
 				(SELECT
 					 rgb.partition_date
 					,rgb.gttcrgb_rgb_cod_entidad
 					,rgb.gttcrgb_rgb_num_garantia
 					,rgb.gttcrgb_rgb_num_bien
 					,vab.gttcvab_vab_tip_valor
 					,vab.gttcvab_vab_imp_total
 				FROM
 					 bi_corp_staging.gtdtrgb rgb
 					,bi_corp_staging.gtdtvab vab
 				WHERE 1=1
 					and rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
 					and rgb.partition_date = vab.partition_date
 					and rgb.gttcrgb_rgb_cod_entidad = vab.gttcvab_vab_cod_entidad
 					and rgb.gttcrgb_rgb_num_bien = vab.gttcvab_vab_num_bien
 					--and vab.gttcvab_vab_cod_divisa != 'ARS'
 					--and rgb.gttcrgb_rgb_num_garantia IN (787198,687133)
 					and vab.gttcvab_vab_tip_valor = '015'  -- '015' VALOR_TASACION PRESTAMOS HIP.UVA
 				) HIP
 			ON (MAESTRO.gttcmae_mae_num_garantia = HIP.gttcrgb_rgb_num_garantia and MAESTRO.partition_date = HIP.partition_date
 				and MAESTRO.gttcgar_gar_cla_garantia = '001')
    LEFT OUTER JOIN
			(
				SELECT cod_divisa, data_date_part, (imp_cambio_fijo/100) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
				FROM santander_business_risk_arda.cotizacion
				--WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','-','')
				WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
				  and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
				  --and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','-','')
                  and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
			)
			cotizaciones
			--ON (cotizaciones.data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','-','') and cotizaciones.cod_divisa = MAESTRO.gttcmae_mae_cod_divisa)
			ON (cotizaciones.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}' and cotizaciones.cod_divisa = MAESTRO.gttcmae_mae_cod_divisa)
 WHERE 1=1
 	and MAESTRO.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
 	--and MAESTRO.gttcmae_mae_num_garantia ='467065'
UNION ALL ----------------------- todos los datos provenientes de ABKT
	 SELECT
		  CONTRAG.cod_entidad
		, CONTRAG.id_cto as id_cto_div
	 	, CONTRAG.num_garantia
	 	, CONTRAG.cod_estado
	 	, CONTRAG.fec_alta
	 	, CONTRAG.fec_vcto
	 	, CONTRAG.cod_garantia
	 	, CONTRAG.tip_cobertur
	 	, cast(null as string) as num_bien
	 	, CONTRAG.cla_garantia
		, cast(null as string) as tip_instrumentacion
		, cast(null as string) as pignoracion
		, cast(null as string) as tip_aval
	 	, CONTRAG.cod_divisa
	 	, CONTRAG.imp_limite
	 	, cast(null as string) as tip_valor
	 	, cast(null as string) as imp_total
	 	, NULL as imp_total_garantia
	 	, NULL as imp_total_garantia_pesos
		, cotizaciones.imp_cambio_fijo_vigente
		, cast(null as string) as fec_bajrelac
		, 'N' as ind_principa
		, cast(null as string) as orden
		, cast(null as string) as tip_gara_bdr
		, cast(null as string) as cod_garantia_bdr
	    	, cast(null as string) as num_persona_garante
	    	, cast(null as string) as pais_iso_garante
	    	, cast(null as string) as cod_bien
	    	, cast(null as string) as cod_postal
	 FROM
		(	SELECT
				  cgr.partition_date
				, '0072' as cod_entidad
				, cgr.cgr_id_contrato as id_cto
				, cgr.cgr_garantia_nro as num_garantia
				, '020' as cod_estado
				, cgr.cgr_fecha_emision as fec_alta
				, cgr.cgr_fecha_vto as fec_vcto
				, SUBSTRING(TRIM(cgr.cgr_moneda),1,3) as cod_divisa
				, cgr.cgr_saldo_pesos as imp_limite
				, '009' as cod_garantia
				, '001' as tip_cobertur
				, '003' as cla_garantia
			FROM 	bi_corp_staging.abkt_contragarantias cgr
			WHERE (cgr.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	 	)  CONTRAG
	    LEFT OUTER JOIN
				(
					SELECT cod_divisa, data_date_part, (imp_cambio_fijo/100) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
					FROM santander_business_risk_arda.cotizacion
					--WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','-','')
					WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
							and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
							and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
				)
				cotizaciones
				ON (cotizaciones.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}' and cotizaciones.cod_divisa = CONTRAG.cod_divisa  )
	 WHERE 1=1
	 	and CONTRAG.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
	;



--Paso2:
    -- Asigno un orden a las garantias de cada contrato en base a ind_principa y el num_garantia

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_especificas_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
 SELECT
	 ge.cod_entidad
	,ge.id_cto_div
	,ge.num_garantia
	,ge.cod_estado
	,ge.fec_alta
	,ge.fec_vcto
	,ge.cod_garantia
	,ge.tip_cobertur
	,bien.num_bien
	,ge.cla_garantia
	,ins.tip_instrumentacion
	,pig.pignoracion
	,avl.tip_aval
	,ge.cod_divisa
	,ge.imp_limite
	,ge.tip_valor
	,ge.imp_total
	,ge.imp_total_garantia
	,ge.imp_total_garantia_pesos
	,ge.imp_cambio_fijo_vigente
	,ge.fec_bajrelac
	,ge.ind_principa
	,CAST( ROW_NUMBER() OVER(PARTITION BY  ge.id_cto_div ORDER BY ge.ind_principa desc, ge.num_garantia asc) as INT)
	    as ordena
	, tip.tip_gara_bdr
	, gara.cod_garantia_bdr
    	, bien.num_persona_garante
    	, rtg_pais.codigo_iso_pais as pais_iso_garante
    	, bien.cod_bien
    	, bien.cod_postal
FROM bi_corp_bdr.aux_garant_garantias_especificas_div ge
	LEFT OUTER JOIN
			( SELECT cod_cod_codigo, cod_cod_entidad, substr(cod_des_textolar,1,1) as pignoracion,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='72')  pig
			ON (ge.cod_garantia = pig.cod_cod_codigo and ge.cod_entidad = pig.cod_cod_entidad
			    and pig.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	LEFT OUTER JOIN
			( SELECT cod_cod_codigo, cod_cod_entidad, substr(cod_des_textolar,1,1) as tip_instrumentacion,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='71')  ins
			ON (ge.cod_garantia = ins.cod_cod_codigo and ge.cod_entidad = ins.cod_cod_entidad
			 and ins.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	LEFT OUTER JOIN
    			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as tip_aval,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='73')  avl
    			ON (ge.cod_garantia = avl.cod_cod_codigo and ge.cod_entidad = avl.cod_cod_entidad
    			and avl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
     LEFT OUTER JOIN
     			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as tip_gara_bdr,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='74')  tip
     			ON (ge.cod_garantia = tip.cod_cod_codigo and ge.cod_entidad = tip.cod_cod_entidad
     			and tip.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
    LEFT OUTER JOIN
     			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as cod_garantia_bdr,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='75')  gara
     			ON (ge.cod_garantia = gara.cod_cod_codigo and ge.cod_entidad = gara.cod_cod_entidad
     			and gara.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
    LEFT OUTER JOIN
                (
                			    SELECT rgb.gttcrgb_rgb_num_garantia as num_garantia,rgb.partition_date,rgb.gttcrgb_rgb_num_bien as num_bien, rbc1.gttcrbc_rbc_num_persona as num_persona_garante
                					,gttcbie_bie_cod_bien as cod_bien, gttchip_hip_cod_postal as cod_postal
                					FROM
                						bi_corp_staging.gtdtrgb rgb
                					LEFT OUTER JOIN
                		                	(
												SELECT * FROM (
													SELECT t.partition_date,t.gttcrbc_rbc_num_bien,t.gttcrbc_rbc_cod_relacion, t.gttcrbc_rbc_num_secclien,t.gttcrbc_rbc_num_persona,t.gttcrbc_rbc_fec_inivali, t.gttcrbc_rbc_fec_finvali, t.estado
														, rank() over (partition by t.gttcrbc_rbc_num_bien order by t.prioridad asc) as ranking
															FROM   (
																			SELECT * FROM (
											                	                	SELECT  rbc.partition_date,rbc.gttcrbc_rbc_num_bien,gttcrbc_rbc_cod_relacion, rbc.gttcrbc_rbc_num_secclien,gttcrbc_rbc_num_persona
											                	                	, rank() over (partition by rbc.gttcrbc_rbc_num_bien order by gttcrbc_rbc_cod_relacion asc, rbc.gttcrbc_rbc_num_secclien asc) as ranking
											                	                	, gttcrbc_rbc_fec_inivali, gttcrbc_rbc_fec_finvali, 'vigentes' as estado, 1 as prioridad
											                	                	FROM bi_corp_staging.gtdtrbc rbc -- RELACION_BIEN_CLIENTE VIGENTES
											                	                	WHERE 1=1
											                	                		--and rbc.gttcrbc_rbc_num_bien IN (340062)--,514300,484966,411935)
											                	                		--and gttcrbc_rbc_num_persona = '07364390'
											                	                		--and rbc.gttcrbc_rbc_cod_relacion = '001'
											                	                		and rbc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
											                	                		and rbc.gttcrbc_rbc_fec_finvali >= rbc.partition_date
											                	                		and rbc.gttcrbc_rbc_fec_inivali <= rbc.partition_date
											                						) rbc2 WHERE rbc2.ranking = 1
											                				UNION ALL
																				SELECT * FROM (
											                	                	SELECT  rbc.partition_date,rbc.gttcrbc_rbc_num_bien,gttcrbc_rbc_cod_relacion, rbc.gttcrbc_rbc_num_secclien,gttcrbc_rbc_num_persona
											                	                	, rank() over (partition by rbc.gttcrbc_rbc_num_bien order by gttcrbc_rbc_cod_relacion asc, rbc.gttcrbc_rbc_num_secclien asc) as ranking
											                	                	, gttcrbc_rbc_fec_inivali, gttcrbc_rbc_fec_finvali, 'vencidos' as estado, 2 as prioridad
											                	                	FROM bi_corp_staging.gtdtrbc rbc -- RELACION_BIEN_CLIENTE VENCIDAS
											                	                	WHERE 1=1
											                	                		--and rbc.gttcrbc_rbc_num_bien IN (340062)--,514300,484966,411935)
											                	                		--and gttcrbc_rbc_num_persona = '07364390'
											                	                		--and rbc.gttcrbc_rbc_cod_relacion = '001'
											                	                		and rbc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
											                	                		and rbc.gttcrbc_rbc_fec_finvali <= rbc.partition_date -- ya vencidos
											                	                		and rbc.gttcrbc_rbc_fec_inivali <= rbc.partition_date
											                						) rbc2 WHERE rbc2.ranking = 1
																) t
															) t1
												WHERE t1.ranking = 1
                                            ) rbc1
                                        ON (rbc1.partition_date = rgb.partition_date and rbc1.gttcrbc_rbc_num_bien = rgb.gttcrgb_rgb_num_bien)
                					LEFT OUTER JOIN bi_corp_staging.gtdtbie bie
                						ON (bie.partition_date = rgb.partition_date and bie.gttcbie_bie_num_bien = rgb.gttcrgb_rgb_num_bien )
                					LEFT OUTER JOIN  bi_corp_staging.gtdthip hip
                						ON (hip.partition_date = rgb.partition_date and hip.gttchip_hip_num_bien = rgb.gttcrgb_rgb_num_bien )
                				WHERE rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
				) bien
			ON (bien.partition_date = ge.partition_date and bien.num_garantia = ge.num_garantia )
	LEFT OUTER JOIN
		(
			SELECT P01.partition_date,P01.pecdgent, P01.penumper, P01.penumdoc, P01.PEPAIRES
				,aux_garant_calif_pais.codigo_iso_pais,aux_garant_calif_pais.cal_unif_pais
			FROM bi_corp_staging.malpe_pedt001 P01
			INNER JOIN  bi_corp_bdr.aux_garant_calif_pais
			ON  (P01.partition_date = aux_garant_calif_pais.partition_date and p01.pepaires = aux_garant_calif_pais.pais)
		) rtg_pais
				ON (rtg_pais.partition_date =ge.partition_date and bien.num_persona_garante = rtg_pais.penumper)
WHERE ge.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
--and ge.num_garantia ='467065'
--order by ge.id_cto_div,ordena asc
;