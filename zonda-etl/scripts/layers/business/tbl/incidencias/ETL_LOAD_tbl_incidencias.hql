SET hive.merge.mapfiles=TRUE;
SET hive.merge.mapredfiles=TRUE;
SET hive.merge.size.per.task=4000000;
SET hive.merge.smallfiles.avgsize=16000000;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

ALTER TABLE bi_corp_business.agg_rec_incidencias ADD IF NOT EXISTS PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonIncidencias-Daily') }}');

INSERT OVERWRITE TABLE bi_corp_business.agg_rec_incidencias PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonIncidencias-Daily') }}')
SELECT periodo as dt_rec_periodo, estados.ide_gestion_nro as id_rec_ide_gestion_nro,cod_est_gest as cod_rec_cod_est_gest,
fec_est_gest as dt_rec_fec_est_gest, cod_bandeja as cod_rec_cod_bandeja,ide_gestion_sector as id_rec_ide_gestion_sector,
tpo_medio as cod_rec_tpo_medio,orden_estado as cod_rec_orden_estado,c.ide_circuito as id_rec_ide_circuito,
C.tpo_gestion as cod_rec_tpo_gestion,C.cod_cpto as cod_rec_cod_cpto, C.cod_subcpto as cod_rec_cod_subcpto,
    if(ide_gestion_sector NOT IN('SCLI','TRAC'),1,0.0) as dec_rec_NP_20107,
        if(ide_gestion_sector != 'SCLI' and cod_subcpto = '630',1,0) as dec_rec_NP_20140, -- SOLICITUD DE INFORMACION
        if(ide_gestion_sector != 'SCLI' and cod_subcpto IN('1976' ,'1977'),1,0) as dec_rec_NP_20142, -- SOLICITADO DESDE AREA CENTRAL(ex RECTIFICACION/REGULARIZACION)
        if(ide_gestion_sector != 'SCLI' and cod_subcpto = '581',1,0) as dec_rec_NP_20144, -- SOLICITUD DE ELIMINACION DE DATOS
        if(ide_gestion_sector != 'SCLI' and cod_subcpto = '654',1,0) as dec_rec_NP_20150 -- INSATISFACCION POR INCLUSION EN CAMPAÑA
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
			and SUBSTRING(fec_est_gest, 1,7)  = SUBSTRING('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonIncidencias-Daily') }}', 1,7)
	) ESTADOS
	INNER JOIN
	(
		SELECT DISTINCT ide_gestion_nro,ide_circuito,partition_date
		FROM bi_corp_staging.rio56_gestiones
		where partition_date = '2021-06-09' -- PARA REPROCESO 2020
		--FROM bi_corp_staging.rio56_gestiones GS
		--inner join
        --(
        --     select max(partition_date) as max_partition_date
        --      from bi_corp_staging.rio56_gestiones
        --      where partition_date > to_date(date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonIncidencias-Daily') }}',7))
        --      and partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_BSN_REC_RDA_TablonIncidencias-Daily') }}'
        -- ) GS2 on GS.partition_date = GS2.max_partition_date
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