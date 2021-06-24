set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;


-- STAGE1: producto vectorial de todos los contratos contra sus garantias genericas UNION los contratos contra sus garantias especificas

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT
      gc1.cod_entidad
    , gc1.num_persona
    , gc1.id_cto_div
    , gc1.orden_contrato
    , gc1.imp_deuda
    , gc1.apr_sin_mitigar
    , cast (row_number() OVER(PARTITION BY  gc1.num_persona, gc1.id_cto_div order by gc1.tip_cobertur, gc1.orden_garantia1 asc) as INT) as orden_garantia
    , gc1.cla_garantia
    , gc1.num_garantia
    , gc1.tip_cobertur
    , gc1.cod_garantia
    , gc1.fec_alta
    , gc1.fec_bajrelac
    , gc1.fec_vcto
    , gc1.imp_total_garantia
    , gc1.imp_total_garantia_pesos
    , CAST (row_number() OVER(PARTITION BY  gc1.num_persona, gc1.num_garantia order by gc1.orden_contrato asc) as INT) as orden_prelacion
    , cast(null as string) as nominal_cobertura_actual
    , cast(null as string) as porc_cobertura_actual
    , cast(null as string) as porc_reparto
    , cast(null as string) as nominal_cobertura_inicial
    , cast(null as string) as porc_cobertura_inicial
    , cast(null as string) as imp_deuda_remanente
    , cast(null as string) as imp_total_garantia_pesos_remantente
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
                bi_corp_bdr.aux_garant_garantias_especificas_div ge
                ON (ge.partition_date = cto.partition_date and ge.id_cto_div = cto.id_cto_div)
        where 1=1
            and cto.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}'
            --and cto.num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
			--and cto.num_persona = '00026343'
            --and cto.id_cto ='0072_0493_039100025848_39_0037_ARS'
        UNION ALL -- garantias especificas de abkt
		SELECT
              cto2.cod_entidad
            , cto2.num_persona
            , cto2.id_cto_div
            , cast(cto2.orden_contrato as INT) as orden_contrato
            , cto2.imp_deuda
            , cto2.apr_sin_mitigar
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
            	, cto2.id_cto_bdr
            	, cto2.cod_entidad_bdr
        FROM
            bi_corp_bdr.aux_garant_contratos_div cto2
            INNER JOIN
                bi_corp_bdr.aux_garant_garantias_especificas_div ge
                ON (ge.partition_date = cto2.partition_date and ge.id_cto_div = SUBSTRING(cto2.id_cto_div, 11, 12)  )
        where 1=1
            and substring(cto2.id_cto_div, 24, 2) ='50'
        	and cto2.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}'
            --and cto2.num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
			--and cto2.num_persona = '00026343'
            --and cto2.id_cto ='0072_0493_039100025848_39_0037_ARS'
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
            bi_corp_bdr.aux_garant_contratos_div cto3
            INNER JOIN
        		bi_corp_bdr.aux_garant_garantias_genericas gen
        			ON (cto3.num_persona = gen.num_persona and cto3.partition_date = gen.partition_date)
        WHERE cto3.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}'
        	--and cto3.num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
			--and cto3.num_persona = '00026343'
        	--and cto3.id_cto ='0072_0493_039100025848_39_0037_ARS'
) gc1
ORDER BY gc1.num_persona, gc1.orden_contrato asc, orden_garantia asc;


-- STAGE2: calcular el nominal_cobertura_actual en base al importe de deuda y los nominal_cobertura (anteriores)

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT
     cod_entidad
    , num_persona
    , id_cto_div
    , orden_contrato
    , imp_deuda
    , apr_sin_mitigar
    , orden_garantia
	, cla_garantia
    , num_garantia
    , tip_cobertur
	, cod_garantia
	, fec_alta
	, fec_bajrelac
	, fec_vcto
	, imp_total_garantia
    , imp_total_garantia_pesos
	, orden_prelacion
    , CASE
        WHEN
            (
				((imp_deuda - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0  )
				AND
				((imp_total_garantia_pesos - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,gc.num_garantia ORDER BY orden_contrato asc,orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)) > 0)
			) --remanente_deuda mayor a cero y remanente_garantia mayor a cero
        THEN (
            CASE
                WHEN (
                    (imp_deuda -
                        (NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)
                            - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,gc.num_garantia ORDER BY orden_contrato asc, orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)
                        )--remanente_garantia
                    ) > imp_total_garantia_pesos)
                    --si el remanente de deuda es mayor a la garantia: entonces cubriremos el total de la garantia
                    THEN imp_total_garantia_pesos
                ELSE
                    -- si el remanente de deuda es menor que la garantia: entonces cubrimos con la garantia solo ese remanente
                    (imp_deuda -
                        (NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,id_cto_div ORDER BY orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0) --remante_deuda
                        - NVL(SUM(imp_total_garantia_pesos) OVER (PARTITION BY num_persona,gc.num_garantia ORDER BY orden_contrato asc, orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)
                        )--remanente_garantia
                    )
            END
             )
        ELSE 0 -- si no queda remanente de deuda anterior, entonces no uso la garantia
    END as nominal_cobertura_actual
	, cast(null as string) as porc_cobertura_actual
	, cast(null as string) as porc_reparto
	, cast(null as string) as nominal_cobertura_inicial
	, cast(null as string) as porc_cobertura_inicial
	, cast(null as string) as imp_deuda_remanente
	, cast(null as string) as imp_total_garantia_pesos_remantente
	, 2 as stage
	, tip_instrumentacion
	, pignoracion
	, gar_cod_estado
	, gar_cod_divisa
	, tip_aval
    , tip_gara_bdr
	, cod_garantia_bdr
    , id_cto_bdr
    , cod_entidad_bdr
FROM
    bi_corp_bdr.aux_garant_garantias_contratos_div gc
WHERE
	gc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}'
	 --and gc.num_persona = '00026343'
	   --and gc.id_cto IN ('0072_0493_039100025848_39_0037_ARS','0072_0493_322140501061_21_0012_ARS')
	    --and gc.tip_cobertur = '001'
ORDER BY num_persona asc, orden_contrato asc, orden_garantia asc;


-- STAGE3: en base al nominal_cobertura calculado en stage2, se procede a calcular los porcentajes y remanentes.

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_garantias_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
SELECT
      gc.cod_entidad
    , gc.num_persona
    , gc.id_cto_div
    , gc.orden_contrato
    , gc.imp_deuda
    , gc.apr_sin_mitigar
    , gc.orden_garantia
	, gc.cla_garantia
    , gc.num_garantia
    , gc.tip_cobertur
	, gc.cod_garantia
	, gc.fec_alta
	, gc.fec_bajrelac
	, gc.fec_vcto
	, gc.imp_total_garantia
    , gc.imp_total_garantia_pesos
	, gc.orden_prelacion
    , gc.nominal_cobertura_actual
	, (gc.nominal_cobertura_actual / gc.imp_deuda * 100)  as porc_cobertura_actual
	, (gc.nominal_cobertura_actual / gc.imp_total_garantia_pesos * 100) as porc_reparto
	, NVL( ini.nominal_cobertura_actual, gc.nominal_cobertura_actual)
	  as nominal_cobertura_inicial
	, NVL( ini.porc_cobertura_actual, (gc.nominal_cobertura_actual / gc.imp_deuda * 100))
	  as porc_cobertura_inicial
    , (gc.imp_deuda -
        (NVL(SUM(gc.nominal_cobertura_actual) OVER (PARTITION BY gc.num_persona,gc.id_cto_div ORDER BY gc.orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
      ) as imp_deuda_remanente
    , (gc.imp_total_garantia_pesos -
        (NVL(SUM(gc.nominal_cobertura_actual) OVER (PARTITION BY gc.num_persona, gc.num_garantia ORDER BY gc.orden_contrato,gc.orden_garantia asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),0))
      ) as imp_total_garantia_pesos_remantente
    , 3 as stage
	, gc.tip_instrumentacion
	, gc.pignoracion
	, gc.gar_cod_estado
	, gc.gar_cod_divisa
	, gc.tip_aval
    , gc.tip_gara_bdr
	, gc.cod_garantia_bdr
	, gc.id_cto_bdr
    , gc.cod_entidad_bdr
FROM
    bi_corp_bdr.aux_garant_garantias_contratos_div gc
	LEFT OUTER JOIN
		bi_corp_bdr.aux_garant_garantias_contratos_inicial_div ini
		ON (ini.cod_entidad = gc.cod_entidad and ini.num_persona = gc.num_persona and ini.id_cto_div = gc.id_cto_div and ini.num_garantia = gc.num_garantia
		    and ini.partition_date < '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
WHERE
	gc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}'
	 --and gc.num_persona = '00026343'
	   --and gc.id_cto IN ('0072_0493_039100025848_39_0037_ARS','0072_0493_322140501061_21_0012_ARS')
	    --and gc.tip_cobertur = '001'
ORDER BY gc.num_persona, gc.orden_contrato asc, gc.orden_garantia asc;