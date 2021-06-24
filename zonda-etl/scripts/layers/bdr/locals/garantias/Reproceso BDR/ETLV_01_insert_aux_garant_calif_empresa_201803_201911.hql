set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- calificacion_empresas
-- Paso1: Tomo las 3 calificaciones posibles en un vector para todos los ALIAS_NUPS/SHORTNAME disponibles en MDR_contrapartes

-- Pendientes:
    -- Hacer dinamicas las fechas para:
        -- MDR_contrapartes: disponible a partir de 2020-04-17
        -- AQUA_rating_empresas: mensuales cargados a demanda.. pendiente automatizar la recepcion.
        -- matriz_rating_calificacion: Incremental total

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_empresa PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
	SELECT mdr.alias_nup,mdr.shortname,rte1.FITCH_SU,rte1.MOODYS_LT_RT, rte1.SP_LT_FCUR_CREDIT_RT,
		mrc_fitch.calificacion_empresa as ce_fitch,
		mrc_moodys.calificacion_empresa as ce_moodys,
		mrc_sp.calificacion_empresa as ce_sp,
		sort_array(array(mrc_fitch.calificacion_empresa,mrc_moodys.calificacion_empresa,mrc_sp.calificacion_empresa)) as listado_ordenado,
		cast(null as string) as calificacion_unificada
	FROM
		(SELECT shortname, razon_social, alias_nup
			FROM bi_corp_staging.mdr_contrapartes
			WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
			and (TRIM(alias_nup) !='<NOT FOUND>' and length(TRIM(alias_nup))=8)
		) mdr
		INNER JOIN
			(SELECT unidad_operativa
				, upper(rte.MOODYS_LT_RT) as MOODYS_LT_RT
				, upper(rte.SP_LT_FCUR_CREDIT_RT) as SP_LT_FCUR_CREDIT_RT
				, upper(rte.FITCH_SU) as FITCH_SU
			from bi_corp_staging.aqua_rating_empresas rte
			where rte.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}'
				and coalesce(rte.MOODYS_LT_RT,rte.SP_LT_FCUR_CREDIT_RT,rte.FITCH_SU) is not null
			) rte1
			ON (mdr.shortname = rte1.unidad_operativa)
		LEFT OUTER JOIN
			(SELECT mrc.moodys,mrc.calificacion_empresa from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_moodys
			ON ( mrc_moodys.moodys = rte1.MOODYS_LT_RT )
		LEFT OUTER JOIN
			(SELECT mrc.sp,mrc.calificacion_empresa from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_sp
			ON ( mrc_sp.sp = rte1.SP_LT_FCUR_CREDIT_RT )
		LEFT OUTER JOIN
			(SELECT mrc.fitch,mrc.calificacion_empresa from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_fitch
			ON ( mrc_fitch.fitch = rte1.FITCH_SU );

-- Paso2: aplico la logica para seleccionar del vector una de las tres calificaciones:

INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_empresa PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}')
	SELECT alias_nup,shortname,FITCH_SU,MOODYS_LT_RT,SP_LT_FCUR_CREDIT_RT,ce_fitch, ce_moodys, ce_sp,listado_ordenado,
        coalesce(listado_ordenado[1],listado_ordenado[2]) as calificacion_unificada
    FROM bi_corp_bdr.aux_garant_calif_empresa
    WHERE (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Reproceso_BDR_Monthly_201803_201911') }}') ;
