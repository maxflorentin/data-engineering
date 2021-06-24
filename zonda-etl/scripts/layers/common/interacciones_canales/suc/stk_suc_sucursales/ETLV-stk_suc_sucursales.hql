SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

WITH servicios AS (

		SELECT sus.sucu_id
			, concat_ws(', ',collect_list(DISTINCT ser.servicio_nombre)) as servicios
			, concat_ws(', ',collect_list(DISTINCT CONCAT(osu.hora_desde, '-', osu.hora_hasta))) as horario
		FROM bi_corp_staging.rio350_sucursal_x_serv sus 
		LEFT JOIN bi_corp_staging.rio350_servicios ser 
			ON ser.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
			AND sus.servicio_id = ser.servicio_id
		LEFT JOIN bi_corp_staging.rio350_sucursal_x_opening_hour_sucu hsu
			ON hsu.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
			AND sus.sucu_id = sus.sucu_id
		LEFT JOIN bi_corp_staging.rio350_opening_hour_sucu osu
			ON osu.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
			AND hsu.opening_hour_sucu_id = osu.opening_hour_sucu_id
			AND hsu.sucu_id = sus.sucu_id
		GROUP BY sus.sucu_id

)

INSERT 	overwrite TABLE bi_corp_common.stk_suc_sucursales 
PARTITION (partition_date)
SELECT CAST(suc.ref_legacy AS INT) cod_suc_sucursal
	, suc.sucu_nombre ds_suc_sucursal
	, zon.cod_suc_zona
	, NULL ds_suc_zona
	, dir.calle ds_suc_domiciliocalle
	, dir.numero ds_suc_domicilionumero
	, dir.barrio ds_suc_localidad
	, pro.nombre ds_suc_provincia
	, dir.cod_postal cod_suc_codpostal
	, coo.latitud ds_suc_latitud
	, coo.longitud ds_suc_longitud
	, CONCAT(tel.cod_area, '-', tel.numero) ds_suc_telefono
	, srv.horario ds_suc_horario
	, NULL ds_suc_gerente
	, IF(suc.habilitado = '1', 'ABIERTA', 'CERRADA') ds_suc_estado
	, NULL dt_suc_fechaalta
	, NULL dt_suc_fechamodif
	, NULL dt_suc_fechabaja
	, srv.servicios ds_suc_servicios
    , suc.partition_date
FROM bi_corp_staging.rio350_sucursales suc 
LEFT JOIN bi_corp_staging.rio350_rel_sucursal_zona zon 
	ON zon.cod_suc_sucursal = CAST(suc.ref_legacy AS INT)
LEFT JOIN bi_corp_staging.rio350_telefonos tel 
	ON tel.sucu_id = suc.sucu_id
	AND tel.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
	AND tel.numero != '0000000'
LEFT JOIN servicios srv 
	ON srv.sucu_id = suc.sucu_id
LEFT JOIN bi_corp_staging.rio350_direcciones dir 
	ON dir.sucu_id = suc.sucu_id
	AND dir.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
LEFT JOIN bi_corp_staging.rio350_coordenadas coo 
	ON coo.coordenada_id = suc.sucu_id
	AND coo.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
LEFT JOIN bi_corp_staging.rio350_provincias pro 
	ON pro.provincia_id = dir.provincia_id
	AND pro.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}'
WHERE suc.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;
