set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

------------------------------------------------------------------------------------------------------------------------
-- UNIVERSO DE CONTRATOS
-- Pendiente de hacer dinamico el ORDEN_PRODUCTO: Ver si existe una jerarquia de Rubros Cargabal

-- PASO1:
    --Busco los contratos de "jm_contr_bis" los paso por "normalizacion_id_contratos" y me traigo la deuda/cargabal de "jm_posic_contr"
    --Busco en "arda.contratos" el nup y entidad local del contrato
    --Busco los ratings en "aux_garant_calif_empresa" y "aux_garant_calif_pais".

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
	  ctob.g4095_s1emp as cod_entidad_bdr
	, ctoa.g4128_idnumcli as nup_cto_bdr
	, ctob.g4095_contra1 as id_cto_bdr
	, '0072' as cod_entidad
	, SUBSTRING(ctoa.g4128_idnumcli,2,8) as num_persona
	, nor.id_cto_source as id_cto_div
	, CASE substr(posic.pos_balance ,1,1) WHEN '1' then 'ON' WHEN '3' then 'OFF' ELSE NULL END as pos_balance
	, orden_producto as orden_x_producto
	, rtg_empresa.cal_unif_empresa as Rating_cliente
	, pers.cal_unif_pais as Rating_Pais_cliente
	, pers.PESUBSEG as segmento_cliente
	, cast(cast( as  as string)string) as facturacion_total_cliente
	, sum(posic.imp_deuda) over (partition by ctoa.g4128_idnumcli) as deuda_total_cliente
	, cast(cast( as  as string)string) as RW_contrato
    , CASE
		WHEN (pers.PESUBSEG  in ('COR', 'EMP','PYM')) THEN
			CASE
				WHEN (pers.PESUBSEG = ('PYM') and (sum(posic.imp_deuda) over (partition by ctoa.g4128_idnumcli) <= (1000000 * cotizaciones.imp_cambio_fijo_vigente)) )
					THEN 0.75 --lo trato como MINORISTA si la deuda total es menor a 1M euros
			ELSE
			    CASE                                        -- SI TIENE RATING_CLIENTE
				    WHEN (rtg_empresa.cal_unif_empresa = 1) THEN    0.2
				    WHEN (rtg_empresa.cal_unif_empresa = 2) THEN    0.5
				    WHEN (rtg_empresa.cal_unif_empresa = 3) THEN    1
				    WHEN (rtg_empresa.cal_unif_empresa = 4) THEN    1
				    WHEN (rtg_empresa.cal_unif_empresa = 5) THEN    1.5
				    WHEN (rtg_empresa.cal_unif_empresa = 6) THEN    1.5
				    ELSE                                    -- SI NO TIENE RATING_CLIENTE
					    CASE
                            -- WHEN (pers.cal_unif_pais = 1) THEN    0    -- lo pisa luego
                            -- WHEN (pers.cal_unif_pais = 2) THEN    0.2  -- lo pisa luego
                            -- WHEN (pers.cal_unif_pais = 3) THEN    0.5  -- lo pisa luego
                            -- WHEN (pers.cal_unif_pais = 4) THEN    1
                            -- WHEN (pers.cal_unif_pais = 5) THEN    1.5
                            WHEN ( NVL(pers.cal_unif_pais,1) < 5) THEN 1
                            ELSE 1.5
					    END
				END
			END
		WHEN (pers.PESUBSEG in ('IND')) THEN 0.75
		ELSE 1
	  END as RW_cliente
	, cast(cast( as  as string)string) as RW_contraparte
	, cast(cast( as  as string)string) as factor_reductor
	, cast(cast( as  as string)string) as APR_SIN_MITIGAR
	, posic.imp_deuda
	, cast(cast( as  as string)string) as orden_x_contrato
	, cast(cast( as  as string)string) as orden_contrato
FROM
    bi_corp_bdr.jm_contr_bis ctob
    INNER JOIN bi_corp_bdr.normalizacion_id_contratos nor
        ON ('{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' = nor.partition_date and ctob.g4095_contra1 = nor.id_cto_bdr and nor.`source` ='credito')
    INNER JOIN
        (SELECT pos.partition_date, pos.e0621_contra1,pos.pos_balance, pos.orden_producto, SUM(pos.imp_deuda) as imp_deuda
            FROM
            (SELECT partition_date, e0621_contra1
            ,( cast(E0621_IMPORTH as DECIMAL (19,4))/-100) as  imp_deuda -- Se le invierte el signo y corremos 2 lugares por decimales
            ,case
                when (E0621_CTACGBAL is cast( or  as string)trim(E0621_CTACGBAL)='') then '1'
                else substr(trim(E0621_CTACGBAL),1,1)
            end as pos_balance
            ,case
                when trim(E0621_CTACGBAL) in ('300001','300011','300021','3000313') then 1
                when trim(E0621_CTACGBAL) in ('300002','300012','300022','3000323') then 2
                when trim(E0621_CTACGBAL) in ('300101','300111','300121','3001313','30300','30301','30302','30303') then 3
                when trim(E0621_CTACGBAL) in ('300100','300110','300120','300130') then 4
            end as orden_producto
            FROM bi_corp_bdr.jm_posic_contr
            WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
            ) pos
            GROUP BY partition_date, e0621_contra1,pos_balance,orden_producto
            HAVING imp_deuda <> 0
        ) posic
        ON (posic.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' and ctob.g4095_contra1 = posic.e0621_contra1)
    LEFT OUTER JOIN
        (
        SELECT c.g4128_s1emp, c.g4128_idnumcli, c.g4128_contra1 FROM bi_corp_bdr.jm_interv_cto c where c.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' AND c.g4128_tipintev='00001'
        ) ctoa
        ON (ctob.g4095_contra1 = ctoa.g4128_contra1)
	LEFT OUTER JOIN
    	(
		SELECT P01.partition_date,P01.pecdgent, P01.penumper, P01.penumdoc, P01.PEPAIRES,p01.PESUBSEG
			,aux_garant_calif_pais.codigo_iso_pais,aux_garant_calif_pais.cal_unif_pais
		FROM bi_corp_staging.malpe_pedt001 P01
		    LEFT OUTER JOIN  bi_corp_bdr.aux_garant_calif_pais
		    ON  ('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' = aux_garant_calif_pais.partition_date and p01.pepaires = aux_garant_calif_pais.pais)
    	where P01.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
    	) pers
	    ON (SUBSTRING(ctoa.g4128_idnumcli,2,8) = pers.penumper)
	LEFT OUTER JOIN
	     bi_corp_bdr.aux_garant_calif_empresa rtg_empresa
	     ON (SUBSTRING(ctoa.g4128_idnumcli,2,8) = rtg_empresa.alias_nup and rtg_empresa.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}' )
	LEFT OUTER JOIN
			(
				SELECT cod_divisa, data_date_part, (imp_cambio_fijo/100) as imp_cambio_fijo_vigente, concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) as fec_cambio_yyyymmdd
				FROM santander_business_risk_arda.cotizacion
				WHERE data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
						and ind_divisa = 'D' and ind_cotizacion = 'S' and cod_segmento = ''
						and concat(substr(fec_cambio, 1,4), substr(fec_cambio, 6,2), substr(fec_cambio, 9,2)) = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','')
						and cod_divisa = 'EUR'
				limit 1
			)
			cotizaciones
			ON (cotizaciones.data_date_part = translate('{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}','-','') )
WHERE ctob.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}'
;

-- PASO2: --Calculo el RW_contraparte, APR_SIN_MITIGAR y con ellos el "orden_x_contrato"
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
	  cod_entidad_bdr
    , nup_cto_bdr
    , id_cto_bdr
    , cod_entidad
    , num_persona
    , id_cto_div
    , pos_balance
    , orden_x_producto
    , Rating_Cliente
    , Rating_Pais_Cliente
    , segmento_cliente
    , facturacion_total_cliente
    , deuda_total_cliente
    , RW_contrato
    , RW_cliente
	, NVL (RW_contrato, RW_cliente ) as RW_contraparte
    , factor_reductor
    , (NVL (RW_contrato, RW_cliente ) * imp_deuda) as APR_SIN_MITIGAR
    , imp_deuda
	, CASE
	    WHEN pos_balance = 'OFF'
	        THEN CAST(row_number() OVER(PARTITION BY  num_persona,pos_balance order by orden_x_producto asc, (NVL (RW_contrato, RW_cliente ) * imp_deuda) desc) as int)
	    WHEN pos_balance = 'ON'
	        THEN CAST (row_number() OVER(PARTITION BY  num_persona,pos_balance order by NVL (RW_contrato, RW_cliente ) asc, (NVL (RW_contrato, RW_cliente ) * imp_deuda) desc) as int)
	  END as orden_x_contrato
	, cast(null as string) as orden_contrato
FROM bi_corp_bdr.aux_garant_contratos_div where (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
-- and num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
;


-- PASO3:
    --Calculo el "orden_contrato" en base a "pos_balance y orden_x_contrato"

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_contratos_div PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
SELECT
	  cod_entidad_bdr
    , nup_cto_bdr
    , id_cto_bdr
    , cod_entidad
    , num_persona
    , id_cto_div
    , pos_balance
    , orden_x_producto
    , Rating_Cliente
    , Rating_Pais_Cliente
    , segmento_cliente
    , facturacion_total_cliente
    , deuda_total_cliente
    , RW_contrato
    , RW_cliente
	, RW_contraparte
    , factor_reductor
    , APR_SIN_MITIGAR
    , imp_deuda
	, orden_x_contrato
	, cast(row_number()  OVER(PARTITION BY num_persona ORDER BY pos_balance desc, orden_x_contrato asc) as int) as orden_contrato
FROM bi_corp_bdr.aux_garant_contratos_div where (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201912_202103') }}')
-- and num_persona IN ('60577655','01154402','06317987','01149182','00388449','00026343')
;