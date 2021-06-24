set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;


-- STAGE1: producto vectorial de todos los contratos contra sus garantias genericas UNION los contratos contra sus garantias especificas
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
SELECT
      gc1.cod_entidad
    , gc1.num_persona
    , gc1.id_cto_div
    , gc1.orden_contrato
    , gc1.imp_deuda
    , gc1.apr_sin_mitigar
    , cast (row_number() OVER(PARTITION BY  gc1.num_persona, gc1.id_cto_div order by gc1.tip_cobertur, gc1.orden_garantia1 asc) as INT) as orden_garantia
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
            ((imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0  )
			 --remanente_deuda mayor a cero
        THEN (
            CASE
                WHEN (
                    (imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0))
                    > imp_total_garantia_pesos
                    ) --si el remanente de deuda es mayor a la garantia: entonces cubriremos el total de la garantia
                    THEN imp_total_garantia_pesos
                ELSE
                    -- si el remanente de deuda es menor que la garantia: entonces cubrimos con la garantia solo ese remanente
                    (imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0))
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
            ((imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0)) > 0  )
			 --SI la DEUDA es mayor a todas las garantias aplicadas: 
        THEN (
         	(imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
         	)
        ELSE 0 --SI la DEUDA era menor a todas las garantias aplicadas: 
    END as decimal (19,4)) as imp_deuda_remanente
    , CAST(CASE
        WHEN
            ((imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0  )
			 --remanente_deuda mayor a cero (deuda menos todas la garantias aplicadas antes a esta)
        THEN (
            CASE
                WHEN (
                    (imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0))
                    > imp_total_garantia_pesos
                    ) --si el remanente de deuda es mayor a la garantia: entonces cubriremos el total de la garantia y no queda remanente de garantia
                    THEN 0
                ELSE
                    -- si el remanente de deuda es menor que la garantia: entonces el remanente de garantia sera la garantia menos lo cubierto antes
                 (imp_total_garantia_pesos - (imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY gc1.tip_cobertur, gc1.orden_garantia1 asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)))
            END
             )
        ELSE imp_total_garantia_pesos -- si no quedaba remanente de deuda anterior, entonces no uso la garantia y queda toda como remanente
    END as decimal (19,4)) as imp_total_garantia_pesos_remantente
    , 1 as stage
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
        SELECT
              cto.cod_entidad
            , cto.num_persona
            , cto.id_cto_div
            , cast(cto.orden_contrato as INT) as orden_contrato
            , cto.imp_deuda
            , cto.apr_sin_mitigar
            , cast (ge.orden as int) as orden_garantia1
            , ge.cla_garantia
            , ge.num_garantia
            , ge.tip_cobertur
            , ge.cod_garantia
            , ge.fec_alta
            , ge.fec_bajrelac
            , ge.fec_vcto
            , cast(ge.imp_total_garantia as decimal(19,4)) as imp_total_garantia
            , cast(ge.imp_total_garantia_pesos as decimal(19,4)) as imp_total_garantia_pesos
            , cast(null as string) as orden_prelacion
            , cast(null as string) as nominal_cobertura_actual
            , cast(null as string) as porc_cobertura_actual
            , cast(null as string) as porc_reparto
            , cast(null as string) as nominal_cobertura_inicial
            , cast(null as string) as porc_cobertura_inicial
            , cast(null as string) as imp_deuda_remanente
            , cast(null as string) as imp_total_garantia_pesos_remantente
            , cast(null as string) as stage
			, ge.tip_instrumentacion
			, ge.pignoracion
			, ge.cod_estado as gar_cod_estado
			, ge.cod_divisa as gar_cod_divisa
			, ge.tip_aval
            	, ge.tip_gara_bdr
				, ge.cod_garantia_bdr
            	, cto.id_cto_bdr
            	, cto.cod_entidad_bdr
        FROM
            bi_corp_bdr.aux_garant_contratos_div cto
            INNER JOIN
                (SELECT * FROM bi_corp_bdr.aux_garant_garantias_especificas_div 
                	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                ) ge
                	ON (ge.partition_date = cto.partition_date and ge.id_cto_div = cto.id_cto_div)
        where 1=1
            and cto.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            --and cto.num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
			--and cto.num_persona = '10315791'
            --and cto.id_cto ='0072_0493_039100025848_39_0037_ARS'
        UNION ALL -- garantias especificas de abkt
		SELECT
              cto2.cod_entidad
            , cto2.num_persona
            , cto2.id_cto_div
            , cast(cto2.orden_contrato as INT) as orden_contrato
            , cto2.imp_deuda
            , cto2.apr_sin_mitigar
            , cast (geb.orden as int) as orden_garantia1
            , geb.cla_garantia
            , geb.num_garantia
            , geb.tip_cobertur
            , geb.cod_garantia
            , geb.fec_alta
            , geb.fec_bajrelac
            , geb.fec_vcto
            , cast(geb.imp_total_garantia as decimal(19,4)) as imp_total_garantia
            , cast(geb.imp_total_garantia_pesos as decimal(19,4)) as imp_total_garantia_pesos
            , cast(null as string) as orden_prelacion
            , cast(null as string) as nominal_cobertura_actual
            , cast(null as string) as porc_cobertura_actual
            , cast(null as string) as porc_reparto
            , cast(null as string) as nominal_cobertura_inicial
            , cast(null as string) as porc_cobertura_inicial
            , cast(null as string) as imp_deuda_remanente
            , cast(null as string) as imp_total_garantia_pesos_remantente
            , cast(null as string) as stage
			, geb.tip_instrumentacion
			, geb.pignoracion
			, geb.cod_estado as gar_cod_estado
			, geb.cod_divisa as gar_cod_divisa
			, geb.tip_aval
            	, geb.tip_gara_bdr
				, geb.cod_garantia_bdr
            	, cto2.id_cto_bdr
            	, cto2.cod_entidad_bdr
        FROM
            (SELECT * FROM bi_corp_bdr.aux_garant_contratos_div 
            	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            	and substring(id_cto_div, 24, 2) ='50' --and num_persona = '10315791'
            )cto2
            INNER JOIN
                (SELECT * FROM bi_corp_bdr.aux_garant_garantias_especificas_div 
                	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
                ) geb
                	ON (geb.partition_date = cto2.partition_date and geb.id_cto_div = SUBSTRING(cto2.id_cto_div, 11, 12)  )
        UNION ALL -- garantias genericas
        SELECT
              cto3.cod_entidad
            , cto3.num_persona
            , cto3.id_cto_div
            , cast(cto3.orden_contrato as int) orden_contrato
            , cto3.imp_deuda
            , cto3.apr_sin_mitigar
            , cast(gen.orden as int) as orden_garantia1
            , cast(null as string) as cla_garantia
            , gen.num_garantia
            , gen.tip_cobertur
            , gen.cod_garantia
            , gen.fec_alta
            , gen.fec_bajrelac
            , gen.fec_vcto
            , cast(gen.imp_limite as decimal(19,4)) as imp_total_garantia
            , cast(gen.imp_limite_pesos as decimal(19,4)) as imp_total_garantia_pesos
            , cast(null as string) as orden_prelacion
            , cast(null as string) as nominal_cobertura_actual
            , cast(null as string) as porc_cobertura_actual
            , cast(null as string) as porc_reparto
            , cast(null as string) as nominal_cobertura_inicial
            , cast(null as string) as porc_cobertura_inicial
            , cast(null as string) as imp_deuda_remanente
            , cast(null as string) as imp_total_garantia_pesos_remantente
            , cast(null as string) as stage
			, gen.tip_instrumentacion
			, gen.pignoracion
			, gen.cod_estado as gar_cod_estado
			, gen.cod_divisa as gar_cod_divisa
			, gen.tip_aval
            	, gen.tip_gara_bdr
				, gen.cod_garantia_bdr
                , cto3.id_cto_bdr
                , cto3.cod_entidad_bdr
        FROM
            (SELECT * FROM bi_corp_bdr.aux_garant_contratos_div  
            	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
            	and orden_contrato = 1 --and num_persona = '10315791'
            ) cto3
            INNER JOIN
        		(SELECT * FROM bi_corp_bdr.aux_garant_garantias_genericas 
        			WHERE partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}' 
        		) gen
        			ON (cto3.num_persona = gen.num_persona and cto3.partition_date = gen.partition_date)
) gc1
ORDER BY gc1.num_persona, gc1.orden_contrato asc, orden_garantia asc;