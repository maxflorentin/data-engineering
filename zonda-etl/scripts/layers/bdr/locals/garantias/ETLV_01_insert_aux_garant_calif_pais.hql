set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

-- CALIFICACION_PAIS
-- Paso1: Tomo las 3 calificaciones posibles en un vector para todos los paises disponibles en la TCDTGEN (0112).

-- Pendientes:
    -- Hacer dinamicas las fechas para:
        -- AQUA_rating_paises: mensuales cargados a demanda.. pendiente automatizar la recepcion.
        -- matriz_rating_calificacion: Incremental total
        
--INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_pais PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_pais PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	SELECT trim(gen.gen_clave) as pais,rtp1.codigo_iso_pais,rtp1.FITCH_LT_F,rtp1.MDYS_LT_FC, rtp1.SP_LT_FC,
        mrc_fitch.calificacion_pais as cp_fitch,
        mrc_moodys.calificacion_pais as cp_moodys,
        mrc_sp.calificacion_pais as cp_sp,
        sort_array(array(mrc_moodys.calificacion_pais,mrc_sp.calificacion_pais,mrc_fitch.calificacion_pais)) as listado_ordenado,
        cast(null as string) as calificacion_unificada
    FROM 
            (SELECT 
                    tc.partition_date,
                    trim(tc.gen_clave) as gen_clave,
                    substr(gen_datos,001,040) as 	TCMPAIS , 	--	NOMBRE PAIS  
                    substr(gen_datos,047,002) as 	TCCPAIS4 	--	CODIGO ISO B.E.   
                FROM bi_corp_staging.tcdtgen tc
                WHERE gentabla ='0112'
                --PDC cambio partition_date por last_working_date
                and partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
                and tc.gen_idioma = 'E'
            ) gen
        LEFT OUTER JOIN 
            (SELECT codigo_iso_pais
                , upper(rtp.MDYS_LT_FC) as MDYS_LT_FC
                , upper(rtp.SP_LT_FC) as SP_LT_FC
                , upper(rtp.FITCH_LT_F) as FITCH_LT_F 
                from bi_corp_staging.aqua_rating_paises rtp
               --where rtp.partition_date = IF(to_Date(add_months(trunc('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','MM'),1)) < '2020-04-01','2020-04-01' ,to_Date(add_months(trunc('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}','MM'),1)))
               --PDC cambio partition_date por last_working_date
               where rtp.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}'
                ) rtp1
            ON (gen.TCCPAIS4 = rtp1.codigo_iso_pais )
		LEFT OUTER JOIN 
            (SELECT mrc.moodys,mrc.calificacion_pais from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_moodys
            ON ( mrc_moodys.moodys = rtp1.MDYS_LT_FC )
        LEFT OUTER JOIN 
            (SELECT mrc.sp,mrc.calificacion_pais from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_sp
            ON ( mrc_sp.sp = rtp1.SP_LT_FC )
        LEFT OUTER JOIN 
            (SELECT mrc.fitch,mrc.calificacion_pais from bi_corp_bdr.matriz_rating_calificacion mrc where mrc.partition_date= '2020-03-30') mrc_fitch
            ON ( mrc_fitch.fitch = rtp1.FITCH_LT_F )
     ;
     
-- Paso2: aplico la logica para seleccionar del vector una de las tres calificaciones:

--INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_pais PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='BDR_LOAD_Tables-Monthly') }}')
INSERT OVERWRITE TABLE bi_corp_bdr.aux_garant_calif_pais PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}')
	SELECT pais,codigo_iso_pais,FITCH_LT_F,MDYS_LT_FC,SP_LT_FC, cp_fitch,cp_moodys, cp_sp,listado_ordenado,
        coalesce(listado_ordenado[1],listado_ordenado[2]) as calificacion_unificada
    FROM bi_corp_bdr.aux_garant_calif_pais
    --PDC cambio partition_date por last_working_date
    WHERE (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly') }}') ;

