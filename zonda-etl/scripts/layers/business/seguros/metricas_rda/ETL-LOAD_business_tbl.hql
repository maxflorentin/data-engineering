set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Calculo la particion mas reciente de bi_corp_staging.rio6_cart_certificados
CREATE TEMPORARY TABLE tmp_maxpart_cace AS
select max(partition_date) AS partition_date
  from bi_corp_staging.rio6_cart_certificados;

--armo tabla temporal TEMP_CONTRATACIONES_4MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CONTRATACIONES_4MESES as
select count(1) fc_contrataciones_4meses_30042
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1001
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0;

--armo tabla temporal TEMP_CONTRATACIONES_15MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CONTRATACIONES_15MESES as
select count(1) fc_contrataciones_15meses_30045
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1001
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0;

--armo tabla temporal TEMP_CANCELACIONES_4MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CANCELACIONES_4MESES as
SELECT count(1) fc_cancelaciones_4meses_30043
FROM
(select CAST(CACE.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
CAST(CACE.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
CACE.cace_fe_completada,
CACE.cace_fe_desde,
CAOD.partition_date
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40
   and TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1003
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0) A
    INNER join (select 	distinct CAST(CACEA.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
						CAST(CACEA.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
						CACEA.cace_fe_completada,
						CACEA.cace_fe_desde
				from bi_corp_staging.rio6_cart_certificados CACEA
				INNER JOIN bi_corp_staging.rio6_cart_operaciones_diarias CAODA
				ON (CAODA.caod_casu_cd_sucursal    = CACEA.cace_casu_cd_sucursal and
				    CAODA.caod_carp_cd_ramo        = CACEA.cace_carp_cd_ramo and
				    CAODA.caod_capo_nu_poliza      = CACEA.cace_capo_nu_poliza and
				    CAODA.caod_cace_nu_certificado = CACEA.cace_nu_certificado and
				    CACEA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
				    )
				where CAST(CAODA.caod_caop_cd_operacion AS BIGINT) = 1001
				  and CAST(CAODA.caod_cd_sub_operacion AS BIGINT)   = 0
				  and CAODA.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4))
			) B
	on (A.cace_carp_cd_ramo = B.cace_carp_cd_ramo and
		A.cace_nu_poliza_original = B.cace_nu_poliza_original
	   )
	where (A.cace_fe_completada = B.cace_fe_completada or A.cace_fe_desde = B.cace_fe_desde);

--armo tabla temporal TEMP_CANCELACIONES_15MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CANCELACIONES_15MESES as
SELECT count(1) fc_cancelaciones_15meses_30046
FROM
(select CAST(CACE.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
CAST(CACE.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
CACE.cace_fe_completada,
CACE.cace_fe_desde,
CAOD.partition_date
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE CAST(CACE.cace_carp_cd_ramo AS BIGINT) between 30 and 40
   and TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1003
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0) A
    INNER join (select 	distinct CAST(CACEA.cace_carp_cd_ramo AS BIGINT) cace_carp_cd_ramo,
						CAST(CACEA.cace_nu_poliza_original AS BIGINT) cace_nu_poliza_original,
						CACEA.cace_fe_completada,
						CACEA.cace_fe_desde
				from bi_corp_staging.rio6_cart_certificados CACEA
				INNER JOIN bi_corp_staging.rio6_cart_operaciones_diarias CAODA
				ON (CAODA.caod_casu_cd_sucursal    = CACEA.cace_casu_cd_sucursal and
				    CAODA.caod_carp_cd_ramo        = CACEA.cace_carp_cd_ramo and
				    CAODA.caod_capo_nu_poliza      = CACEA.cace_capo_nu_poliza and
				    CAODA.caod_cace_nu_certificado = CACEA.cace_nu_certificado and
				    CACEA.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
				    )
				where CAST(CAODA.caod_caop_cd_operacion AS BIGINT) = 1001
				  and CAST(CAODA.caod_cd_sub_operacion AS BIGINT)   = 0
				  and CAODA.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15))
			) B
	on (A.cace_carp_cd_ramo = B.cace_carp_cd_ramo and
		A.cace_nu_poliza_original = B.cace_nu_poliza_original
	   )
	where (A.cace_fe_completada = B.cace_fe_completada or A.cace_fe_desde = B.cace_fe_desde);

--armo tabla temporal TEMP_CONTRATOS_VIVOS_4MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CONTRATOS_VIVOS_4MESES as
select count(1) contratos_vivos_4meses_30041
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-4))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1001
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0
    and exists (select 1
    			from bi_corp_staging.rio6_cart_certificados CACE1,
    			     tmp_maxpart_cace tmp_cace
    			where CAST(CACE1.cace_st_certificado AS BIGINT) in (1, 30)
			 	and CAST(CACE1.cace_nu_poliza_original AS BIGINT) = CAST(CACE.cace_nu_poliza_original AS BIGINT)
    			and CACE1.partition_date = tmp_cace.partition_date
               );

--armo tabla temporal TEMP_CONTRATOS_VIVOS_15MESES
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_CONTRATOS_VIVOS_15MESES as
select count(1) contratos_vivos_15meses_30044
	from bi_corp_staging.rio6_cart_operaciones_diarias CAOD
	INNER JOIN bi_corp_staging.rio6_cart_certificados CACE
        ON (CAOD.caod_casu_cd_sucursal    = CACE.cace_casu_cd_sucursal and
            CAOD.caod_carp_cd_ramo        = CACE.cace_carp_cd_ramo and
            CAOD.caod_capo_nu_poliza      = CACE.cace_capo_nu_poliza and
            CAOD.caod_cace_nu_certificado = CACE.cace_nu_certificado and
            CACE.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
           )
	LEFT JOIN  bi_corp_staging.rio6_cart_productos CAPU
				ON (CACE.cace_carp_cd_ramo        = CAPU.capu_carp_cd_ramo and
						CACE.cace_capu_cd_producto    = CAPU.capu_cd_producto and
						CAPU.partition_date           ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
						)
	LEFT JOIN bi_corp_staging.rio6_cart_prodplanes CAPB
				ON (CACE.cace_carp_cd_ramo     = CAPB.capb_carp_cd_ramo and
            CACE.cace_capu_cd_producto = CAPB.capb_capu_cd_producto and
            CACE.cace_capb_cd_plan     = CAPB.capb_cd_plan and
            CAPB.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}'
            )
	WHERE TRIM(CAPU.capu_cacy_cd_clase) not in ('HI','PE','PR','SD','TC','TR') --esto es solo om
   and not (
            (CAST(CAPU.capu_carp_cd_ramo AS BIGINT) = 18 and CASE WHEN TRIM(CAPU.capu_cacy_cd_clase) IS NULL THEN 'OM' ELSE TRIM(CAPU.capu_cacy_cd_clase) END in ('IP','PP') and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END = 'REL')
            or
            (CAST(CAPB.capb_carp_cd_ramo AS BIGINT) = 19 and CASE WHEN TRIM(CAPB.capb_cd_clasificacion) IS NULL THEN 'OM' ELSE TRIM(CAPB.capb_cd_clasificacion) END in ('PLT', 'BLK'))
            or
            (CAST(CACE.cace_carp_cd_ramo AS BIGINT) in (60, 62, 64, 66, 68, 70, 72, 74) and CAST(cace_nu_certificado AS BIGINT) > 1000)
           )
	and CAOD.partition_date between  date_format(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15),'YYYY-MM-01')  and last_day(add_months('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}',-15))
	and CAST(CAOD.caod_caop_cd_operacion AS BIGINT) = 1001
    and CAST(CAOD.caod_cd_sub_operacion AS BIGINT)   = 0
    and exists (select 1
    			from bi_corp_staging.rio6_cart_certificados CACE1,
    			     tmp_maxpart_cace tmp_cace
    			where CAST(CACE1.cace_st_certificado AS BIGINT) in (1, 30)
			 	and CAST(CACE1.cace_nu_poliza_original AS BIGINT) = CAST(CACE.cace_nu_poliza_original AS BIGINT)
    			and CACE1.partition_date = tmp_cace.partition_date
               );

-- Inserto en la tabla bi_corp_business.tbl_metricas_rda
INSERT OVERWRITE TABLE bi_corp_business.tbl_metricas_rda
partition(partition_date)
select  a.fc_contrataciones_4meses_30042,
        b.fc_contrataciones_15meses_30045,
        c.fc_cancelaciones_4meses_30043,
        d.fc_cancelaciones_15meses_30046,
        e.contratos_vivos_4meses_30041,
        f.contratos_vivos_15meses_30044,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BSN_MetricasRDA_Monthly') }}' partition_date
from (select fc_contrataciones_4meses_30042
        from TEMP_CONTRATACIONES_4MESES) a,
     (select fc_contrataciones_15meses_30045
        from TEMP_CONTRATACIONES_15MESES) b,
     (select fc_cancelaciones_4meses_30043
        from TEMP_CANCELACIONES_4MESES) c,
     (select fc_cancelaciones_15meses_30046
        from TEMP_CANCELACIONES_15MESES) d,
     (select contratos_vivos_4meses_30041
        from TEMP_CONTRATOS_VIVOS_4MESES) e,
     (select contratos_vivos_15meses_30044
        from TEMP_CONTRATOS_VIVOS_15MESES) f;