SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccion PARTITION (partition_date)
SELECT TRIM(TI.cd_interaccion) cod_suc_interaccion
	, CAST(TI.nup AS BIGINT) cod_per_nup
	, TRIM(TI.cd_ejecutivo) cod_suc_legajo
	, to_date(TI.dt_inicio) dt_suc_inicio
	, SUBSTRING(TI.dt_inicio, 1, 19) ts_suc_inicio
	, to_date(TI.dt_cierre) dt_suc_cierre
	, SUBSTRING(TI.dt_cierre, 1, 19) ts_suc_cierre
	, TRIM(TI.cd_canal_comunicacion) cod_suc_canalcomunicacion 
	, TRIM(CC.ds_canal_comunicacion) ds_suc_canalcomunicacion
	, TRIM(TI.cd_canal_venta) cod_suc_canalventa
	, TRIM(CV.ds_canal_venta) ds_suc_canalventa
	, CAST(TI.cd_sucursal AS INT) cod_suc_sucursal
	, TRIM(TI.ds_comentario) ds_suc_comentario
	, TI.partition_date
FROM bi_corp_staging.rio151_tbl_interaccion TI 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_comunicacion CC 
	ON TI.cd_canal_comunicacion = CC.cd_canal_comunicacion 
	AND TI.partition_date = CC.partition_date 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_venta CV
	ON TI.cd_canal_venta = CV.cd_canal_venta 
	AND TI.partition_date = CV.partition_date 
WHERE TI.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;