set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
set mapred.job.queue.name=root.dataeng;

------------------------------------------------------------------------------------------------------------------------
-- UNIVERSO DE GARANTIAS GENERICAS

--Paso1:
    -- Reuno los datos de las garantias genericas, obtengo los rating (pais y empresa) y convierto a pesos el limite de la garantia.

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_genericas PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT
	mae.gttcmae_mae_cod_entidad,
	rgc.gttcrgc_rgc_num_persona as num_persona,
	mae.gttcmae_mae_num_garantia as num_garantia,
	mae.gttcmae_mae_cod_estado as cod_estado,
	mae.gttcmae_mae_fec_alta as fec_alta,
	mae.gttcmae_mae_fec_vcto as fec_vcto,
	cast(null as string) as fec_bajrelac,
	mae.gttcmae_mae_cod_garantia as cod_garantia,
	mae.gttcmae_mae_tip_cobertur as tip_cobertur,
	rgb.gttcrgb_rgb_num_bien as num_bien,
	ins.tip_instrumentacion,
	pig.pignoracion,
	avl.tip_aval,
	mae.gttcmae_mae_cod_divisa as cod_divisa,
	mae.gttcmae_mae_imp_limite as imp_limite,
	CASE
	    WHEN mae.gttcmae_mae_cod_divisa !='ARS'
	        THEN (mae.gttcmae_mae_imp_limite * cotizaciones.imp_cambio_fijo_vigente)
	    ELSE mae.gttcmae_mae_imp_limite
    END as imp_limite_pesos,
	IF (mae.gttcmae_mae_cod_divisa !='ARS' ,cotizaciones.imp_cambio_fijo_vigente,1) as imp_cambio_fijo_vigente,
	bien.gttcrbc_rbc_num_persona as num_persona_garante,
	rtg_pais.cal_unif_pais,
	rtg_empresa.cal_unif_empresa as RW_GARANTE,
	CASE
        WHEN mae.gttcmae_mae_cod_garantia IN ('006','007')	-- Fianzas y Avales
            THEN NVL (rtg_empresa.cal_unif_empresa, IF( (rtg_pais.cal_unif_pais < 4), 4 , rtg_pais.cal_unif_pais) )     -- Altera la calificacion del pais para salir minimamente con 4.
        WHEN mae.gttcmae_mae_cod_garantia IN ('005','009','021','022') -- El resto de las Fianzas y Avales
            THEN NVL (rtg_empresa.cal_unif_empresa, rtg_pais.cal_unif_pais)
        ELSE  NULL
    END as RW_GARANTIA,
    CASE
        WHEN mae.gttcmae_mae_cod_garantia IN ('005','006','007','009','021','022')
            THEN 1          -- Fianzas y Avales
        WHEN mae.gttcmae_mae_cod_garantia IN ('056','058','072','076','078','080')
            THEN 2          -- Garantias Hipotecarias Comerciales (No individuos)
        ELSE    3
    END as orden_cod_garantia,
	cast(null as string) as orden,
	tip.tip_gara_bdr,
	gara.cod_garantia_bdr,
	rtg_pais.codigo_iso_pais as pais_iso_garante,
	bien.COD_BIEN,
	bien.COD_POSTAL
FROM
	bi_corp_staging.gtdtmae mae
	INNER JOIN bi_corp_staging.gtdtrgc rgc
		ON (    rgc.gttcrgc_rgc_num_garantia = mae.gttcmae_mae_num_garantia
			--and rgc.partition_date = mae.partition_date
			--agrego partition date a la tabla
			and rgc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
			and rgc.gttcrgc_rgc_fec_inivali <= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
			and rgc.gttcrgc_rgc_fec_finvali >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
			)
	LEFT OUTER JOIN
	    (SELECT * FROM bi_corp_staging.gtdtrgb WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
        			and gttcrgb_rgb_fec_inivali <= partition_date
        			and gttcrgb_rgb_fec_finvali >= partition_date
        )rgb
		ON (	mae.gttcmae_mae_num_garantia = rgb.gttcrgb_rgb_num_garantia
			and rgb.partition_date = mae.partition_date
			and rgb.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
			)
	LEFT OUTER JOIN
			    (
			    SELECT bie.partition_date,bie.gttcbie_bie_num_bien, rbc1.gttcrbc_rbc_num_secclien, rbc1.gttcrbc_rbc_num_persona,  gttcbie_bie_cod_bien as cod_bien, gttchip_hip_cod_postal as cod_postal
                                FROM
                                	bi_corp_staging.gtdtbie bie
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
                                		ON (bie.partition_date = rbc1.partition_date and bie.gttcbie_bie_num_bien = rbc1.gttcrbc_rbc_num_bien )
                                	LEFT OUTER JOIN  bi_corp_staging.gtdthip hip
                                		ON (hip.partition_date = bie.partition_date and hip.gttchip_hip_num_bien = bie.gttcbie_bie_num_bien )
				) bien
			ON (bien.partition_date = mae.partition_date and bien.gttcbie_bie_num_bien = rgb.gttcrgb_rgb_num_bien)
	LEFT OUTER JOIN
	(
		SELECT P01.partition_date,P01.pecdgent, P01.penumper, P01.penumdoc, P01.PEPAIRES
			,aux_garant_calif_pais.codigo_iso_pais,aux_garant_calif_pais.cal_unif_pais
		FROM bi_corp_staging.malpe_pedt001 P01
		INNER JOIN  bi_corp_bdr.aux_garant_calif_pais
		ON  (P01.partition_date = aux_garant_calif_pais.partition_date and p01.pepaires = aux_garant_calif_pais.pais)
	) rtg_pais
			ON (rtg_pais.partition_date =mae.partition_date and bien.gttcrbc_rbc_num_persona = rtg_pais.penumper)
    LEFT OUTER JOIN
	(
    	SELECT cod_divisa, data_date_part, (imp_cambio_fijo/100) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
    	FROM santander_business_risk_arda.cotizacion
    	WHERE data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
    			and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
    			and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}'
	)
	cotizaciones
	ON (cotizaciones.data_date_part = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day_arda', dag_id='BDR_LOAD_Tables-Monthly') }}' and cotizaciones.cod_divisa = mae.gttcmae_mae_cod_divisa)
	LEFT OUTER JOIN
	     bi_corp_bdr.aux_garant_calif_empresa rtg_empresa
	     ON (bien.gttcrbc_rbc_num_persona = rtg_empresa.alias_nup and rtg_empresa.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}' )
	LEFT OUTER JOIN
			( SELECT cod_cod_codigo, cod_cod_entidad, substr(cod_des_textolar,1,1) as pignoracion,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='72')  pig
			ON (mae.gttcmae_mae_cod_garantia = pig.cod_cod_codigo and mae.gttcmae_mae_cod_entidad = pig.cod_cod_entidad --and mae.partition_date = pig.partition_date )
			and pig.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	LEFT OUTER JOIN
			( SELECT cod_cod_codigo, cod_cod_entidad, substr(cod_des_textolar,1,1) as tip_instrumentacion,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='71')  ins
			ON (mae.gttcmae_mae_cod_garantia = ins.cod_cod_codigo and mae.gttcmae_mae_cod_entidad = ins.cod_cod_entidad --and mae.partition_date = ins.partition_date )
			and ins.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	LEFT OUTER JOIN
    			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as tip_aval,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='73')  avl
    			ON (mae.gttcmae_mae_cod_garantia = avl.cod_cod_codigo and mae.gttcmae_mae_cod_entidad = avl.cod_cod_entidad --and mae.partition_date = avl.partition_date )
    			and avl.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
     LEFT OUTER JOIN
     			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as tip_gara_bdr,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='74')  tip
     			ON (mae.gttcmae_mae_cod_garantia = tip.cod_cod_codigo and mae.gttcmae_mae_cod_entidad = tip.cod_cod_entidad -- and mae.partition_date = tip.partition_date)
     			and tip.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
    LEFT OUTER JOIN
     			( SELECT cod_cod_codigo, cod_cod_entidad, cod_des_textored as cod_garantia_bdr,partition_date from bi_corp_staging.gtdtcod WHERE COD_COD_TIPO='75')  gara
     			ON (mae.gttcmae_mae_cod_garantia = gara.cod_cod_codigo  and mae.gttcmae_mae_cod_entidad = gara.cod_cod_entidad --and mae.partition_date = gara.partition_date)
                and gara.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
WHERE 1=1
	and mae.gttcmae_mae_cod_estado = '020' -- ACTIVAS
	and mae.gttcmae_mae_tip_cobertur = '002' -- GENERICAS
	and mae.gttcmae_mae_fec_vcto >= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
	and mae.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
    and mae.gttcmae_mae_cod_garantia != '019'
	--and mae.gttcmae_mae_num_garantia ='729632'
	--and rgc.gttcrgc_rgc_num_persona = '03413204'
;


--Paso2:
    -- Asigno un orden a las garantias de cada "num_persona" seg?n: RW_GARANTIA asc, fec_vcto desc, imp_limite_pesos desc

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_genericas PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT
	cod_entidad,
	num_persona,
	num_garantia,
	cod_estado,
	fec_alta,
	fec_vcto,
	fec_bajrelac,
	cod_garantia,
	tip_cobertur,
	num_bien,
	tip_instrumentacion,
	pignoracion,
	tip_aval,
	cod_divisa,
	imp_limite,
	imp_limite_pesos,
	imp_cambio_fijo_vigente,
	num_persona_garante,
	cal_unif_pais,
	RW_GARANTE,
	RW_GARANTIA,
	orden_cod_garantia,
	CAST( row_number() OVER(PARTITION BY  num_persona order by orden_cod_garantia asc, RW_GARANTIA asc, fec_vcto desc, imp_limite_pesos desc) as INT) as orden,
        tip_gara_bdr,
        cod_garantia_bdr,
        	pais_iso_garante,
        	COD_BIEN,
        	COD_POSTAL
FROM bi_corp_bdr.aux_garant_garantias_genericas where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}';