set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- STAGE2: calcular el nominal_cobertura_actual en base al importe de deuda y los nominal_cobertura (anteriores)
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT 
	cod_entidad,num_persona,id_cto_div,orden_contrato,imp_deuda,apr_sin_mitigar,orden_garantia,cla_garantia,num_garantia,tip_cobertur,cod_garantia,fec_alta,fec_bajrelac,fec_vcto,
	imp_total_garantia,imp_total_garantia_pesos,orden_prelacion,CAST(nominal_cobertura_actual as decimal(19,4)) as nominal_cobertura_actual,porc_cobertura_actual,porc_reparto,nominal_cobertura_inicial,porc_cobertura_inicial,
	imp_deuda_remanente,imp_total_garantia_pesos_remantente,stage,tip_instrumentacion,pignoracion,gar_cod_estado,gar_cod_divisa,tip_aval,tip_gara_bdr,cod_garantia_bdr,id_cto_bdr,
	cod_entidad_bdr
	FROM bi_corp_bdr.aux_garant_garantias_contratos_div  cto_ant 
	WHERE partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
	--and num_persona ='02765272'
UNION ALL
SELECT
      gc1.cod_entidad
    , gc1.num_persona
    , gc1.id_cto_div
    , gc1.orden_contrato
    , gc1.imp_deuda
    , gc1.apr_sin_mitigar
    --, cast (row_number() OVER(PARTITION BY  gc1.num_persona, gc1.id_cto_div order by gc1.tip_cobertur, gc1.orden_garantia1 asc) as INT) as orden_garantia
    , gc1.orden_garantia1 as orden_garantia
    , gc1.cla_garantia
    , cast(gc1.num_garantia as string) as num_garantia
    , gc1.tip_cobertur
    , gc1.cod_garantia
    , gc1.fec_alta
    , gc1.fec_bajrelac
    , gc1.fec_vcto
    , gc1.imp_total_garantia
    , gc1.imp_total_garantia_pesos
    , CAST (row_number() OVER(PARTITION BY  gc1.num_persona, gc1.num_garantia order by gc1.orden_contrato asc) as INT) as orden_prelacion
    , CAST(CASE
        WHEN
            --((gc1.deuda_rema - NVL(SUM(gc1.gara_rema) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0  )
            ((gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0  )
			 --remanente_deuda mayor a cero
        THEN (
            CASE
                WHEN         
                    ((gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > gc1.deuda_rema)
                    --si el remanente de deuda es mayor a la garantia: entonces cubriremos el total de la garantia
                    THEN gc1.deuda_rema
                ELSE
                    -- si el remanente de deuda es menor que la garantia: entonces cubrimos con la garantia solo ese remanente
                    (gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0))
            END
             )
        ELSE 0 -- si no queda remanente de deuda anterior, entonces no uso la garantia
    END as decimal (19,4)) as nominal_cobertura_actual
    , cast(null as string) as porc_cobertura_actual
    , cast(null as string) as porc_reparto
    , cast(null as string) as nominal_cobertura_inicial
    , cast(null as string) as porc_cobertura_inicial
	, CAST(CASE
        WHEN
			((gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0)) > 0  )
			 --SI la GARANTIA es mayor a todas las DEUDAS aplicadas: 
	        THEN 0      	
    	ELSE 
		    (CASE  --SI la GARANTIA es menor a todas las DEUDAS aplicadas: Cobertura_Parcial o Cobertura_Nula
	        	WHEN -- GARANTIA es mayor a las DEUDAS anteriores: Cobertura_Parcial
	        		( (gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0)
	        		THEN 
	        			(gc1.deuda_rema - (gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) )
	        	ELSE 
	        		gc1.deuda_rema -- Cobertura_Nula
	        END)
    END as decimal (19,4)) as imp_deuda_remanente
	, CAST(CASE
        WHEN
			(   (gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0)) > 0  )
			 --SI la GARANTIA es mayor a todas las DEUDAS aplicadas: 
	        THEN 
	        	(gc1.gara_rema - NVL(SUM(gc1.deuda_rema) OVER (PARTITION BY num_persona,num_garantia ORDER BY orden_contrato asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
        ELSE 0 
    END as decimal (19,4)) as imp_total_garantia_pesos_remantente
    , gc1.stage
	, gc1.tip_instrumentacion
	, gc1.pignoracion
	, gc1.gar_cod_estado
	, gc1.gar_cod_divisa
	, gc1.tip_aval
	, gc1.tip_gara_bdr
	, gc1.cod_garantia_bdr
	, gc1.id_cto_bdr
	, gc1.cod_entidad_bdr
FROM
(
	-- garantias genericas
        SELECT
              cto3.cod_entidad
            , cto3.num_persona
            , cto3.id_cto_div
            , cast(cto3.orden_contrato as int) orden_contrato
            , cto3.imp_deuda
            , cto3.apr_sin_mitigar
            , gen.orden_garantia as orden_garantia1
            , cast(null as string) as cla_garantia
            , CAST(gen.num_garantia as string)  as num_garantia
            , gen.tip_cobertur
            , gen.cod_garantia
            , gen.fec_alta
            , gen.fec_bajrelac
            , gen.fec_vcto
            , gen.imp_total_garantia
            , gen.imp_total_garantia_pesos
            , cast(null as string) as orden_prelacion
            , cast(null as string) as nominal_cobertura_actual
            , cast(null as string) as porc_cobertura_actual
            , cast(null as string) as porc_reparto
            , cast(null as string) as nominal_cobertura_inicial
            , cast(null as string) as porc_cobertura_inicial
            , cast(null as string) as imp_deuda_remanente
            , cast(null as string) as imp_total_garantia_pesos_remantente
            , CAST((gen.max_stage + 1) as INT) as stage
			, gen.tip_instrumentacion
			, gen.pignoracion
			, gen.gar_cod_estado
			, gen.gar_cod_divisa
			, gen.tip_aval
        	, gen.tip_gara_bdr
			, gen.cod_garantia_bdr
            , cto3.id_cto_bdr
            , cto3.cod_entidad_bdr
                	,NVL(cto3.deuda_remanente, cto3.imp_deuda) deuda_rema
                	,gen.imp_total_garantia_pesos_remantente as gara_rema
        FROM
            (
				--PROXIMOS CONTRATOS AUN POR CUBRIR
				SELECT cast(CTO_REMA.deuda_remanente as DECIMAL(19,4)) as  deuda_remanente, cto.cod_entidad, cto.num_persona, cto.id_cto_div, cto.orden_contrato, CAST(cto.imp_deuda as decimal(19,4)) as imp_deuda
            		, cto.apr_sin_mitigar, cto.id_cto_bdr, cto.cod_entidad_bdr , cto.partition_date 
				FROM bi_corp_bdr.aux_garant_contratos_div  cto 
				LEFT OUTER JOIN 
						(
						SELECT partition_date,num_persona ,id_cto_div,max(orden_contrato ) as max_orden, MIN(imp_deuda_remanente) as deuda_remanente
							FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc 
							WHERE partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
							--and num_persona ='02765272'
							group by partition_date,num_persona,id_cto_div --having deuda_remanente > 0 			
							) CTO_REMA -- siguiente orden_cto del ultimo cargado con garantias_genericas
						ON (cto.num_persona = CTO_REMA.num_persona and cto.id_cto_div = CTO_REMA.id_cto_div)
				WHERE cto.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
				and NVL(CTO_REMA.deuda_remanente,1) > 0
				--and cto.num_persona ='02765272'			
            ) cto3
            INNER JOIN
        		(
					--GARANTIAS AUN REMANENTES : la primer garantia disponible de cada persona
					SELECT * FROM 
						(
						SELECT  gen_rema.*, rank() over (partition by num_persona order by orden_garantia asc) as ranking
						FROM (
							SELECT partition_date,stage,orden_contrato ,num_persona ,orden_garantia,num_garantia,tip_cobertur,cod_garantia, fec_alta, fec_bajrelac,fec_vcto,imp_total_garantia, CAST(imp_total_garantia_pesos as decimal(19,4)) as imp_total_garantia_pesos
								, tip_instrumentacion,pignoracion,gar_cod_estado
										, gar_cod_divisa
										, tip_aval
							        	, tip_gara_bdr
										, cod_garantia_bdr
										, CAST(gc.imp_total_garantia_pesos_remantente as decimal(19,4)) as imp_total_garantia_pesos_remantente
										, max(orden_contrato ) over (partition by num_persona, num_garantia ) as max_orden_contrato
										, max(stage) over (PARTITION by partition_date ) as max_stage
							FROM bi_corp_bdr.aux_garant_garantias_contratos_div gc 
							WHERE partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
							and tip_cobertur ='002' --and imp_total_garantia_pesos_remantente >0
							--and num_persona = '02765272'			
							) gen_rema
						WHERE 1=1
							and imp_total_garantia_pesos_remantente >0
							and  max_orden_contrato = orden_contrato 
						) prox_gara
						WHERE ranking = 1
        		) gen
        			ON (cto3.num_persona = gen.num_persona)
) gc1
ORDER BY gc1.num_persona, gc1.orden_contrato asc, orden_garantia asc;

