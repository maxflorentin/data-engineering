-----------------------------------------------------------------------------------
--    _____                                                _                       
--   |  __ \                                              (_)                      
--   | |__) |___ _ __  _   _ _ __ ___   ___ _ __ __ _  ___ _  ___  _ __   ___  ___ 
--   |  _  // _ \ '_ \| | | | '_ ` _ \ / _ \ '__/ _` |/ __| |/ _ \| '_ \ / _ \/ __|
--   | | \ \  __/ | | | |_| | | | | | |  __/ | | (_| | (__| | (_) | | | |  __/\__ \
--   |_|  \_\___|_| |_|\__,_|_| |_| |_|\___|_|  \__,_|\___|_|\___/|_| |_|\___||___/
--
------------------------------------------------------------------------------------

-- hist tables: 
	--	2019-12-16 > bi_corp_staging.malbgc_zbdtmig
	--  2019-01-31 > bi_corp_staging.malpe_pedt008
	
-- clasificar cuenta destino = 70
-- clasificar cuenta destino != 70
	
-- step1:

------------------------------------------------
set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE renumeraciones ;
CREATE TEMPORARY TABLE renumeraciones AS
WITH 

	cuentas_previousdate AS (

		SELECT mig_new_cent_alta
			, mig_new_cuenta
			, mig_new_divisa
		FROM bi_corp_staging.malbgc_zbdtmig
		WHERE partition_date = '2020-08-09'	

	)
	
	, cuentas_thisdate AS (
	
		SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			T.partition_date
		FROM bi_corp_staging.malbgc_zbdtmig T
		LEFT JOIN cuentas_previousdate P
			ON T.mig_new_cent_alta = P.mig_new_cent_alta
			AND T.mig_new_cuenta = P.mig_new_cuenta
			AND T.mig_new_divisa = P.mig_new_divisa 
		WHERE partition_date = '2020-08-10'	
			AND P.mig_new_cuenta IS NULL
	)

	, new_con AS (
	
		SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			P8.new_penumper ,
			P8.new_pecodpro ,
			P8.new_pecodsub ,
			P8.new_penumcon ,
			P8.new_sucursal ,
			P8.new_pefecalt ,
			P8.new_pefecbrb ,
			T.partition_date
		FROM cuentas_thisdate T  	
		LEFT JOIN (
					SELECT
						penumper new_penumper ,
						pecodpro new_pecodpro ,
						pecodsub new_pecodsub ,
						partition_date ,
						CAST(penumcon AS bigint) new_penumcon ,
						pecodofi new_sucursal , 
						pefecalt new_pefecalt ,
						pefecbrb new_pefecbrb 
					FROM
						bi_corp_staging.malpe_pedt008
					WHERE partition_date = '2020-08-10'
						AND pecalpar = 'TI' 
				) P8 
				ON  P8.new_penumcon = CAST(substring(T.mig_new_cuenta, 4, 9) AS bigint)
				AND P8.new_penumcon = T.mig_new_cent_alta
			WHERE (SUBSTRING(T.mig_new_cuenta, 2, 2) = P8.new_pecodpro 
					OR P8.new_pecodpro = '70')
		
	)	
	
	SELECT T.mig_new_cent_alta ,
			T.mig_new_cuenta ,
			T.mig_new_divisa ,
			T.mig_old_cent_alta ,
			T.mig_old_cuenta ,
			T.mig_old_divisa ,
			T.mig_old_indesta ,
			T.mig_old_motiv_baja ,
			T.mig_old_motiv_migr ,
			T.new_penumper ,
			T.new_pecodpro ,
			T.new_pecodsub ,
			T.new_penumcon ,
			T.new_sucursal ,
			T.new_pefecalt ,
			T.new_pefecbrb ,	
			P8b.old_penumper ,
			P8b.old_pecodpro ,
			P8b.old_pecodsub ,
			P8b.old_penumcon ,
			P8b.old_sucursal ,
			P8b.old_pefecalt ,
			P8b.old_pefecbrb ,
			T.partition_date
	FROM  new_con T			
	LEFT JOIN (
				SELECT
					penumper old_penumper ,
					pecodpro old_pecodpro ,
					pecodsub old_pecodsub ,
					partition_date ,
					CAST(penumcon AS bigint) old_penumcon , 
					pecodofi old_sucursal ,
					pefecalt old_pefecalt ,
					pefecbrb old_pefecbrb
				FROM
					bi_corp_staging.malpe_pedt008
				WHERE partition_date = '2020-08-10'
					AND pecalpar = 'TI' 
				) P8b 
			ON  P8b.old_penumcon = CAST(substring(T.mig_old_cuenta, 4, 9) AS bigint)
			AND P8b.old_sucursal = T.mig_old_cent_alta
		WHERE (SUBSTRING(T.mig_old_cuenta, 2, 2) = P8b.old_pecodpro 
			OR P8b.old_pecodpro = '70')  ;


SELECT * FROM renumeraciones WHERE new_penumper = '01316873' ; --'01316873' ; '06242016'

	



SELECT
	q.*,
	p.*
FROM
	(
	
	SELECT
		h.mig_new_cent_alta ,
		h.mig_new_cuenta ,
		h.mig_new_divisa ,
		h.mig_old_cent_alta ,
		h.mig_old_cuenta ,
		h.mig_old_divisa ,
		h.mig_old_indesta ,
		h.mig_old_motiv_baja ,
		h.mig_old_motiv_migr ,
		h.partition_date
	FROM
		bi_corp_staging.malbgc_zbdtmig h
	WHERE
		h.partition_date = '2020-08-10'
		--'2019-12-17' --limit 10
		AND NOT EXISTS (
							SELECT
								1
							FROM
								bi_corp_staging.malbgc_zbdtmig a
							WHERE
								a.partition_date = '2020-08-09' --'2019-12-16'
								AND h.mig_new_cent_alta = a.mig_new_cent_alta
								AND h.mig_new_cuenta = a.mig_new_cuenta
								AND h.mig_new_divisa = a.mig_new_divisa 
						) 
	) q
LEFT JOIN (
				SELECT
					penumper ,
					pecodpro ,
					pecodsub ,
					partition_date ,
					--cast(waguxdex_num_contrato as bigint)
			 		CAST (penumcon AS bigint) penumcon,
					--cast(waguxdex_cod_centro as int)
			 		pecodofi sucursal ,
					pefecalt ,
					pefecbrb
				FROM
					bi_corp_staging.malpe_pedt008
				WHERE
					pecalpar = 'TI' 
		) p ON
		p.partition_date = q.partition_date
		AND p.penumcon = CAST(substring(q.mig_new_cuenta, 4, 9) AS bigint)
		AND p.sucursal = q.mig_new_cent_alta
		AND (SUBSTRING(Q.MIG_NEW_CUENTA, 2, 2) = pecodpro OR pecodpro = '70')
LEFT JOIN (
				SELECT
					penumper ,
					pecodpro ,
					pecodsub ,
					partition_date ,
					--cast(waguxdex_num_contrato as bigint)
			 		CAST (penumcon AS bigint) penumcon ,
					--cast(waguxdex_cod_centro as int)
			 		pecodofi sucursal ,
					pefecalt ,
					pefecbrb
				FROM
					bi_corp_staging.malpe_pedt008
				WHERE
					pecalpar = 'TI' 
			) p2 ON
		p2.partition_date = q.partition_date
		AND p2.penumcon = CAST(substring(q.mig_old_cuenta, 4, 9) AS bigint)
		AND p2.sucursal = q.mig_old_cent_alta
		AND (SUBSTRING(Q.MIG_old_CUENTA, 2, 2) = p2.pecodpro OR p2.pecodpro = '70')
WHERE
	-- mig_new_cuenta='007003618066'
 	p.penumper = '01316873'
	--'06242016' 
 	

	
-- ##################################################################################################


SELECT * --mig_new_cent_alta
	--, mig_new_cuenta
	--, mig_new_divisa
FROM bi_corp_staging.malbgc_zbdtmig
WHERE partition_date = '2021-03-04'	
AND mig_new_cuenta = '007000605762' ;
