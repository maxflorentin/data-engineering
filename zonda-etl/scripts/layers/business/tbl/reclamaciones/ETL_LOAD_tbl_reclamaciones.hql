SET hive.merge.mapfiles=TRUE;
SET hive.merge.mapredfiles=TRUE;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

ALTER TABLE bi_corp_business.agg_rec_reclamaciones ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}');

INSERT OVERWRITE TABLE bi_corp_business.agg_rec_reclamaciones PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}')
SELECT periodo as dt_rec_periodo, estados.ide_gestion_nro as id_rec_ide_gestion_nro,cod_est_gest as cod_rec_cod_est_gest,fec_est_gest as dt_rec_fec_est_gest,
cod_bandeja as cod_rec_cod_bandeja,ide_gestion_sector as id_rec_ide_gestion_sector,tpo_medio as cod_rec_tpo_medio, orden_estado as cod_rec_orden_estado,c.ide_circuito as id_rec_ide_circuito,
C.tpo_gestion as cod_rec_tpo_gestion,fec_gestion_alta as dt_rec_fec_gestion_alta, cod_tipo_favorabilidad as cod_rec_cod_tipo_favorabilidad,cod_cpto as cod_rec_cod_cpto,
    if(tpo_medio = '8',1,0) as dec_rec_NP_20067,
    if(tpo_medio != '8',1,0) as dec_rec_NP_20069,
    cast(null as decimal(38,21)) as dec_rec_NP_20074,
    cast(null as decimal(38,21)) as dec_rec_NP_20076,
    cast(null as decimal(38,21)) as dec_rec_NP_20078,
    cast(null as decimal(38,21)) as dec_rec_NP_20080,
    cast(null as decimal(38,21)) as dec_rec_NP_20083,
    cast(null as decimal(38,21)) as dec_rec_NP_20106,
    cast(null as decimal(38,21)) as dec_rec_NP_20128,
    cast(null as decimal(38,21)) as dec_rec_NP_20141,
    cast(null as decimal(38,21)) as dec_rec_NP_20143,
    cast(null as decimal(38,21)) as dec_rec_NP_20145
    FROM
    (
		SELECT * FROM
		(
			SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado,
			rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) asc) as ranking
			FROM bi_corp_staging.rio56_gestion_estados
		    order by cast(orden_estado as int) asc
		) ulti
			WHERE ranking = 1 -- me quedo con el primer estado ordenado por orden_estado descendente
			and upper(cod_bandeja) = 'ALTA'
			and SUBSTRING(fec_est_gest, 1,7)  = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
			and ide_gestion_sector IN ('SCLI')
			--and tpo_medio = '8'
	) ESTADOS
	INNER JOIN
	(
		SELECT DISTINCT ide_gestion_nro,ide_circuito,fec_gestion_alta, nvl(cod_tipo_favorabilidad, 'NULL') as cod_tipo_favorabilidad, partition_date
        FROM bi_corp_staging.rio56_gestiones
        where partition_date = '2021-06-09' -- PARA REPROCESO 2020
		--FROM bi_corp_staging.rio56_gestiones GS
		--inner join
        --(
        --      select max(partition_date) as max_partition_date
        --      from bi_corp_staging.rio56_gestiones
        --     where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}',7))
        --      and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}'
        -- ) GS2 on GS.partition_date = GS2.max_partition_date
	) G
	ON (ESTADOS.ide_gestion_nro = G.ide_gestion_nro)
	INNER JOIN
	(
		SELECT * FROM (
		SELECT partition_date, ide_circuito,tpo_gestion,cod_cpto, cod_subcpto, rank() over (partition by ide_circuito order by partition_date desc) as ranking
		FROM bi_corp_staging.rio56_circuito c  WHERE tpo_gestion in ('1','2')) rr where ranking = 1
	) C
	on (G.ide_circuito = c.ide_circuito)
	order by id_rec_ide_gestion_nro asc
UNION ALL
SELECT periodo as dt_rec_periodo, estados.ide_gestion_nro as id_rec_ide_gestion_nro,cod_est_gest as cod_rec_cod_est_gest,fec_est_gest as dt_rec_fec_est_gest,
cod_bandeja as cod_rec_cod_bandeja,ide_gestion_sector as id_rec_ide_gestion_sector,tpo_medio as cod_rec_tpo_medio, orden_estado as cod_rec_orden_estado,c.ide_circuito as id_rec_ide_circuito,
C.tpo_gestion as cod_rec_tpo_gestion,fec_gestion_alta as dt_rec_fec_gestion_alta, cod_tipo_favorabilidad as cod_rec_cod_tipo_favorabilidad,cod_cpto as cod_rec_cod_cpto,
    cast(null as decimal(38,21)) as dec_rec_NP_20067,
    cast(null as decimal(38,21)) as dec_rec_NP_20069,
    nvl(1,0.0) as dec_rec_NP_20074,
    if(cod_tipo_favorabilidad IN('2','NULL'), 1,0) as dec_rec_NP_20076,
    if(cod_tipo_favorabilidad IN('1','3'), 1,0) as dec_rec_NP_20078,
    cast(null as decimal(38,21)) as dec_rec_NP_20080,
    cast(null as decimal(38,21)) as dec_rec_NP_20083,
    cast(null as decimal(38,21)) as dec_rec_NP_20106,
    cast(null as decimal(38,21)) as dec_rec_NP_20128,
    cast(null as decimal(38,21)) as dec_rec_NP_20141,
    cast(null as decimal(38,21)) as dec_rec_NP_20143,
    cast(null as decimal(38,21)) as dec_rec_NP_20145
    FROM
    (
		SELECT * FROM
		(
			SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado,
			rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) desc) as ranking
			FROM bi_corp_staging.rio56_gestion_estados
		    order by cast(orden_estado as int) desc
		) ulti
			WHERE ranking = 1 -- me quedo con el ultimo estado ordenado por orden_estado descendente
			and upper(cod_bandeja) = 'CERRADO'
			and SUBSTRING(fec_est_gest, 1,7)  = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
			and ide_gestion_sector IN ('SCLI')
			and tpo_medio NOT IN('40','42')
	) ESTADOS
	INNER JOIN
	(
		SELECT DISTINCT ide_gestion_nro,ide_circuito,fec_gestion_alta, nvl(cod_tipo_favorabilidad, 'NULL') as cod_tipo_favorabilidad, partition_date
        FROM bi_corp_staging.rio56_gestiones
        where partition_date = '2021-06-09' -- PARA REPROCESO 2020
		--FROM bi_corp_staging.rio56_gestiones GS
		--inner join
        --(
        --      select max(partition_date) as max_partition_date
        --      from bi_corp_staging.rio56_gestiones
        --      where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}',7))
        --      and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}'
        -- ) GS2 on GS.partition_date = GS2.max_partition_date
	) G
	ON (ESTADOS.ide_gestion_nro = G.ide_gestion_nro)
	INNER JOIN
	(
		SELECT * FROM (
		SELECT partition_date, ide_circuito,tpo_gestion, cod_cpto, cod_subcpto, rank() over (partition by ide_circuito order by partition_date desc) as ranking
		FROM bi_corp_staging.rio56_circuito c  WHERE tpo_gestion in ('1','2')) rr where ranking = 1
	) C
	on (G.ide_circuito = c.ide_circuito)
	order by id_rec_ide_gestion_nro asc
UNION ALL
SELECT periodo as dt_rec_periodo, estados.ide_gestion_nro as id_rec_ide_gestion_nro, cod_est_gest as cod_rec_cod_est_gest, fec_est_gest as dt_rec_fec_est_gest,
cod_bandeja as cod_rec_cod_bandeja, ide_gestion_sector as id_rec_ide_gestion_sector,tpo_medio as cod_rec_tpo_medio, orden_estado as cod_rec_orden_estado, c.ide_circuito as id_rec_ide_circuito,
C.tpo_gestion as cod_rec_tpo_gestion, fec_gestion_alta as dt_rec_fec_gestion_alta, cod_tipo_favorabilidad as cod_rec_cod_tipo_favorabilidad,cod_cpto as cod_rec_cod_cpto,
        cast(null as decimal(38,21)) as dec_rec_NP_20067,
        cast(null as decimal(38,21)) as dec_rec_NP_20069,
        cast(null as decimal(38,21)) as dec_rec_NP_20074,
        cast(null as decimal(38,21)) as dec_rec_NP_20076,
        cast(null as decimal(38,21)) as dec_rec_NP_20078,
        nvl(valor_economico,0.0) as dec_rec_NP_20080,
        cast(null as decimal(38,21)) as dec_rec_NP_20083,
        cast(null as decimal(38,21)) as dec_rec_NP_20106,
        cast(null as decimal(38,21)) as dec_rec_NP_20128,
        cast(null as decimal(38,21)) as dec_rec_NP_20141,
        cast(null as decimal(38,21)) as dec_rec_NP_20143,
        cast(null as decimal(38,21)) as dec_rec_NP_20145
        FROM
        (
			SELECT * FROM
		(
			SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado,
			rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) desc) as ranking
			FROM bi_corp_staging.rio56_gestion_estados
		    order by cast(orden_estado as int) desc
		) ulti
			WHERE ranking = 1 -- me quedo con el ultimo estado ordenado por orden_estado descendente
			and upper(cod_bandeja) = 'CERRADO'
			and SUBSTRING(fec_est_gest, 1,7)  = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
			and ide_gestion_sector IN ('SCLI')
			and tpo_medio NOT IN('40','42')
		) ESTADOS
		INNER JOIN
		(
			SELECT DISTINCT ide_gestion_nro,ide_circuito, fec_gestion_alta, nvl(cod_tipo_favorabilidad, 'NULL') as cod_tipo_favorabilidad, partition_date
            FROM bi_corp_staging.rio56_gestiones
            where partition_date = '2021-06-09' -- PARA REPROCESO 2020
            and cod_tipo_favorabilidad IN('1','3')
			--FROM bi_corp_staging.rio56_gestiones GS
			--inner join
			--(
			--	  select max(partition_date) as max_partition_date
			--	  from bi_corp_staging.rio56_gestiones
			--	  where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}',7))
			--	  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}'
			--	  and cod_tipo_favorabilidad IN('1','3')
			-- ) GS2 on GS.partition_date = GS2.max_partition_date
		) G
		ON (ESTADOS.ide_gestion_nro = G.ide_gestion_nro)
		INNER JOIN
		(
			SELECT * FROM (
			SELECT partition_date, ide_circuito,tpo_gestion, cod_cpto, cod_subcpto, rank() over (partition by ide_circuito order by partition_date desc) as ranking
			FROM bi_corp_staging.rio56_circuito c  WHERE tpo_gestion in ('1','2')) rr where ranking = 1
		) C
		on (G.ide_circuito = c.ide_circuito)
		left JOIN
		(
			select ide_gestion_nro, sum(valor_economico) as valor_economico
			from
			(
				select ide_gestion_nro, cod_info_doc_reque, CAST(dato_info_doc_reque AS DECIMAL(19,2)) as valor_economico, fec_inf_adjunta
				from bi_corp_staging.rio56_informacion_adjunta
				where substring(fec_inf_adjunta,1,7) = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
				and cod_info_doc_reque IN('538','2739','2756','2726')
			) a
			GROUP BY ide_gestion_nro
		) AD
		ON (G.ide_gestion_nro = AD.ide_gestion_nro)
		order by id_rec_ide_gestion_nro asc
UNION ALL
SELECT periodo as dt_rec_periodo, ide_gestion_nro as id_rec_ide_gestion_nro, cod_est_gest as cod_rec_cod_est_gest, fec_est_gest as dt_rec_fec_est_gest,
cod_bandeja as cod_rec_cod_bandeja, ide_gestion_sector as id_rec_ide_gestion_sector,tpo_medio as cod_rec_tpo_medio, orden_estado as cod_rec_orden_estado, ide_circuito as id_rec_ide_circuito,
tpo_gestion as cod_rec_tpo_gestion, fec_gestion_alta as dt_rec_fec_gestion_alta, cod_tipo_favorabilidad as cod_rec_cod_tipo_favorabilidad,cod_cpto as cod_rec_cod_cpto,
        cast(null as decimal(38,21)) as dec_rec_NP_20067,
        cast(null as decimal(38,21)) as dec_rec_NP_20069,
        cast(null as decimal(38,21)) as dec_rec_NP_20074,
        cast(null as decimal(38,21)) as dec_rec_NP_20076,
        cast(null as decimal(38,21)) as dec_rec_NP_20078,
        cast(null as decimal(38,21)) as dec_rec_NP_20080,
        nvl(1,0.0) as dec_rec_NP_20083,
        cast(null as decimal(38,21)) as dec_rec_NP_20106,
        cast(null as decimal(38,21)) as dec_rec_NP_20128,
        cast(null as decimal(38,21)) as dec_rec_NP_20141,
        cast(null as decimal(38,21)) as dec_rec_NP_20143,
        cast(null as decimal(38,21)) as dec_rec_NP_20145
        FROM
        (
        SELECT periodo, estados.ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, orden_estado, g.ide_circuito, C.tpo_gestion, C.cod_cpto, fec_gestion_alta, cod_tipo_favorabilidad, sum(flag_laborable) as cant_DHAB
		FROM
		(
			SELECT periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector, tpo_medio, orden_estado
			FROM
			(
				SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado
					, rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) desc) as ranking
				FROM bi_corp_staging.rio56_gestion_estados
			) ulti
			WHERE ranking = 1
			and upper(cod_bandeja) = 'CERRADO'
			and tpo_medio = '8'
		UNION ALL
			SELECT rdesc.periodo, rdesc.ide_gestion_nro, rdesc.cod_est_gest, rdesc.fec_est_gest, rdesc.cod_bandeja, rdesc.ide_gestion_sector, prim.tpo_medio, prim.orden_estado
			FROM
			(
				SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja,ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado,
				rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) asc) as ranking
				FROM bi_corp_staging.rio56_gestion_estados
			) prim
			join
			(
				SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest , cod_bandeja,ide_gestion_sector, tpo_medio, orden_estado,
				rank() over (partition by cast(ide_gestion_nro as int) order by cast(orden_estado as int) desc) as rankt
				FROM bi_corp_staging.rio56_gestion_estados
			) rdesc
			on rdesc.rankt = 1 and rdesc.ide_gestion_nro = prim.ide_gestion_nro and upper(rdesc.cod_bandeja) = 'CERRADO' and rdesc.tpo_medio != '8' and rdesc.ide_gestion_sector = 'SCLI'
			WHERE ranking = 1
			and upper(prim.cod_bandeja) = 'ALTA'
			and prim.tpo_medio = '8'
		) ESTADOS
		LEFT OUTER JOIN
			(
				SELECT DISTINCT ide_gestion_nro,ide_circuito,fec_gestion_alta, nvl(cod_tipo_favorabilidad, 'NULL') as cod_tipo_favorabilidad, partition_date
				FROM bi_corp_staging.rio56_gestiones
                where partition_date = '2021-06-09' -- PARA REPROCESO 2020
				--FROM bi_corp_staging.rio56_gestiones GS
				--inner join
				--(
				--  select max(partition_date) as max_partition_date
				--  from bi_corp_staging.rio56_gestiones
				--  where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}',7))
				--  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}'
				--) GS2 on GS.partition_date = GS2.max_partition_date
			) G
			ON (ESTADOS.ide_gestion_nro = G.ide_gestion_nro)
		JOIN
			(
				SELECT * FROM
				(
				SELECT partition_date, ide_circuito,tpo_gestion, cod_cpto, cod_subcpto, rank() over (partition by ide_circuito order by partition_date desc) as ranking
				FROM bi_corp_staging.rio56_circuito c  WHERE tpo_gestion in ('1','2')
				) rr where ranking = 1
			) c
			on (G.ide_circuito = c.ide_circuito)
		INNER JOIN
		(
			SELECT fec_yyyy_mm_dd, flag_laborable, data_date_part
			FROM santander_business_risk_arda.calendario
			WHERE data_date_part = '20210609'
		) cal
			ON G.partition_date = '2021-06-09' --cal.data_date_part x REPRO 2020
            where substring(ESTADOS.periodo,1,7) = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
            and cal.fec_yyyy_mm_dd >= substring(G.fec_gestion_alta,1,10) and cal.fec_yyyy_mm_dd <= substring(ESTADOS.fec_est_gest,1,10)
            GROUP BY periodo, estados.ide_gestion_nro, cod_est_gest, estados.fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, tpo_gestion, c.cod_cpto, orden_estado, g.ide_circuito, G.fec_gestion_alta, cod_tipo_favorabilidad
            order by ESTADOS.ide_gestion_nro asc
	) M20083
	where cant_DHAB <= 10
UNION ALL
SELECT periodo as dt_rec_periodo, estados.ide_gestion_nro as id_rec_ide_gestion_nro, cod_est_gest as cod_rec_cod_est_gest, fec_est_gest as dt_rec_fec_est_gest,
cod_bandeja as cod_rec_cod_bandeja, ide_gestion_sector as id_rec_ide_gestion_sector,tpo_medio as cod_rec_tpo_medio, orden_estado as cod_rec_orden_estado, c.ide_circuito as id_rec_ide_circuito,
C.tpo_gestion as cod_rec_tpo_gestion, fec_gestion_alta as dt_rec_fec_gestion_alta, cod_tipo_favorabilidad as cod_rec_cod_tipo_favorabilidad,cod_cpto as cod_rec_cod_cpto,
        cast(null as decimal(38,21)) as dec_rec_NP_20067,
        cast(null as decimal(38,21)) as dec_rec_NP_20069,
        cast(null as decimal(38,21)) as dec_rec_NP_20074,
        cast(null as decimal(38,21)) as dec_rec_NP_20076,
        cast(null as decimal(38,21)) as dec_rec_NP_20078,
        cast(null as decimal(38,21)) as dec_rec_NP_20080,
        cast(null as decimal(38,21)) as dec_rec_NP_20083,
        IF(tpo_medio = '16',1,0) as dec_rec_NP_20106,
        if(ide_gestion_sector  = 'SCLI' and cod_subcpto = '654',1,0) as dec_rec_NP_20128, --INSATISFACCION POR INCLUSION EN CAMPAÑA
        if(ide_gestion_sector  = 'SCLI' and cod_subcpto = '630',1,0) as dec_rec_NP_20141, --SOLICITUD DE INFORMACION
        if(ide_gestion_sector  = 'SCLI' and cod_subcpto IN('1976' ,'1977'),1,0) as dec_rec_NP_20143, -- SOLICITADO DESDE AREA CENTRAL (ex RECTIFICACION/REGULARIZACION)
        if(ide_gestion_sector  = 'SCLI' and cod_subcpto = '581',1,0) as dec_rec_NP_20145 --SOLICITUD DE ELIMINACION DE DATOS
        FROM
        (
            SELECT * FROM
        		(
        			SELECT SUBSTRING(fec_est_gest, 1,7)as periodo, ide_gestion_nro, cod_est_gest, fec_est_gest, cod_bandeja, ide_gestion_sector,tpo_medio, cast(orden_estado as int) as orden_estado,
        			rank() over (partition by ide_gestion_nro order by cast(orden_estado as int) asc) as ranking
        			FROM bi_corp_staging.rio56_gestion_estados
        		    order by cast(orden_estado as int) asc
        		) ulti
        			WHERE ranking = 1 -- me quedo con el primer estado ordenado por orden_estado descendente
        			and upper(cod_bandeja) = 'ALTA'
        			and SUBSTRING(fec_est_gest, 1,7)  = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}', 1,7)
        ) ESTADOS
        INNER JOIN
        	(
        		SELECT DISTINCT ide_gestion_nro,ide_circuito, fec_gestion_alta, nvl(cod_tipo_favorabilidad, 'NULL') as cod_tipo_favorabilidad, partition_date
                FROM bi_corp_staging.rio56_gestiones
                where partition_date = '2021-06-09' -- PARA REPROCESO 2020
                --FROM bi_corp_staging.rio56_gestiones GS
				--inner join
				--(
				--  select max(partition_date) as max_partition_date
				--  from bi_corp_staging.rio56_gestiones
				--  where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}',7))
				--  and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonReclamaciones-Daily') }}'
				--) GS2 on GS.partition_date = GS2.max_partition_date
        	) G
             ON (ESTADOS.ide_gestion_nro = G.ide_gestion_nro)
        INNER JOIN
        	(
        	    SELECT * FROM
            	(
            		SELECT partition_date, ide_circuito,tpo_gestion, cod_cpto, cod_subcpto, rank() over (partition by ide_circuito order by partition_date desc) as ranking
            		FROM bi_corp_staging.rio56_circuito c  WHERE tpo_gestion in ('1','2')
            	) rr
            		where ranking = 1
        	) C
        	on (G.ide_circuito = c.ide_circuito)
        	order by id_rec_ide_gestion_nro asc