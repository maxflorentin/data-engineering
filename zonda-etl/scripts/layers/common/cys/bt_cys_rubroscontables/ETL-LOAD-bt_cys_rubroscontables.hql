set hive.exec.dynamic.partition.mode = nonstrict ;
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.bt_cys_rubroscontables
PARTITION(partition_date) 

	
SELECT TRIM(a9600.entidad) cod_cys_entidad
		, from_unixtime(unix_timestamp(a9600.fecha_informacion ,'dd-mm-yyyy'),'yyyy-mm-dd') dt_cys_fechainformacion
		, TRIM(a9600.rubro_altair) cod_cys_rubroaltair
		, TRIM(a9600.nombre_cuenta) ds_cys_nombrecuenta
		, TRIM(a9600.rubro_bcra) cod_cys_rubrobcra
		, TRIM(a9600.cargabal) cod_cys_cargabal
		, TRIM(a9600.pdn) cod_cys_plandenegocios
		, TRIM(a9600.em) cod_cys_em
		, TRIM(a9600.cuenta_ant) cod_nro_cuentaanterior
		, TRIM(a9600.rubro_niif) cod_cys_rubroniif
		, IF(SUBSTRING(TRIM(a9600.rubro_bcra),3,1) IN ('5','6'), 'USD', 'ARS') cod_div_divisa
		, NULL cod_nro_cuentaregularizadora
		, TRIM(REGEXP_REPLACE(h7770.saldo_mes, ",", ".")) fc_cys_saldomes
		, TRIM(REGEXP_REPLACE(h7770.saldo_promedio_mes, ",", ".")) fc_cys_saldopromediomes
		, TRIM(REGEXP_REPLACE(a9600.enero, ",", ".")) fc_cys_enero
		, TRIM(REGEXP_REPLACE(a9600.febrero, ",", ".")) fc_cys_febrero
		, TRIM(REGEXP_REPLACE(a9600.marzo, ",", ".")) fc_cys_marzo
		, TRIM(REGEXP_REPLACE(a9600.abril, ",", ".")) fc_cys_abril
		, TRIM(REGEXP_REPLACE(a9600.mayo, ",", ".")) fc_cys_mayo
		, TRIM(REGEXP_REPLACE(a9600.junio, ",", ".")) fc_cys_junio
		, TRIM(REGEXP_REPLACE(a9600.julio, ",", ".")) fc_cys_julio
		, TRIM(REGEXP_REPLACE(a9600.agosto, ",", ".")) fc_cys_agosto
		, TRIM(REGEXP_REPLACE(a9600.septiembre, ",", ".")) fc_cys_septiembre
		, TRIM(REGEXP_REPLACE(a9600.octubre, ",", ".")) fc_cys_octubre
		, TRIM(REGEXP_REPLACE(a9600.noviembre, ",", ".")) fc_cys_noviembre
		, TRIM(REGEXP_REPLACE(a9600.diciembre, ",", ".")) fc_cys_diciembre
		, NULL fc_cys_cierreanioanteriorajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.enero, ",", ".")) fc_cys_eneroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.febrero, ",", ".")) fc_cys_febreroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.marzo, ",", ".")) fc_cys_marzoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.abril, ",", ".")) fc_cys_abrilajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.mayo, ",", ".")) fc_cys_mayoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.junio, ",", ".")) fc_cys_junioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.julio, ",", ".")) fc_cys_julioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.agosto, ",", ".")) fc_cys_agostoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.septiembre, ",", ".")) fc_cys_septiembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.octubre, ",", ".")) fc_cys_octubreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.noviembre, ",", ".")) fc_cys_noviembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.diciembre, ",", ".")) fc_cys_diciembreajustinflacion
		, a9600.partition_date
	FROM bi_corp_staging.malha_alha9600 a9600
	
	LEFT JOIN bi_corp_staging.malha_alha9601 a9601
		ON TRIM(a9601.rubro_altair) = TRIM(a9600.rubro_altair)
		AND a9601.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}'
	
	LEFT JOIN bi_corp_staging.malha_hals7770 h7770 
		ON TRIM(a9600.rubro_altair) = TRIM(h7770.cuenta_contable_pl1)
		AND h7770.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}'
	
	WHERE a9600.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CyS') }}' ;