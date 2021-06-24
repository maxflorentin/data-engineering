
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_maschetransaccion PARTITION (partition_date)

SELECT

DISTINCT   IT.cd_interaccion 			                  	    as cod_cc_interaccion,
  IT.id_transaccion 			                  	    as cod_cc_transaccion,
  IT.numero_operacion 		                  		    as cod_cc_operacion,
  IT.datos					                            as ds_cc_datos,
  CASE WHEN IT.marca_reversado ='S' THEN 1 ELSE 0 END as flag_cc_reservado,
  CASE WHEN IT.marca_operacion_exitosa ='S' THEN 1 ELSE 0 END as flag_cc_operacionexitosa,
  CASE WHEN IT.id_transaccion_reversado = 'null' THEN  NULL ELSE TRIM(IT.id_transaccion_reversado) END as cod_cc_transaccion_reservado,
  from_unixtime(unix_timestamp(TRIM( SUBSTRING(IT.fecha_modificacion, 0,19 )),'yyyy-MM-dd HH:mm:ss')) as ts_cc_modificaciontransaccion,
  IT.tipo_operacion 			                  	    as cod_cc_tipooperacion,
  CASE WHEN IT.marca_reversable ='S' THEN 1 ELSE 0 END as flag_cc_reversabletransaccion,
  CASE WHEN IT.id_trackeo_transaccion = 'null' THEN NULL ELSE TRIM(IT.id_trackeo_transaccion) END as cod_cc_trackeotransaccion,
  IT.id_servicio				                        as cod_cc_servicio,
  IT.nombre_servicio			                  	    as cod_cc_nombre_servicio,
  S.descripcion				                      	    as ds_cc_nombre_servicio,
  S.tipo					                            as cod_cc_tipo_sevicio,
  CASE WHEN S.marca_reversable ='S' THEN 1 ELSE 0 END as flag_cc_reversableservicio,
  CASE WHEN S.marca_venta	 ='S' THEN 1 ELSE 0 END as flag_cc_venta,
  from_unixtime(unix_timestamp(TRIM( SUBSTRING(S.fecha_modificacion, 0,19 )),'yyyy-MM-dd HH:mm:ss')) as ts_cc_modificacionservicio,
  CASE WHEN S.grupo = 'null' THEN NULL ELSE TRIM(S.grupo) END as ds_cc_grupo,
  IT.partition_date

FROM bi_corp_staging.rio226_tbl_interaccion_transaccion IT
LEFT join bi_corp_staging.rio226_tbl_servicio S
  on IT.id_servicio = S.id
Where
  IT.partition_date     = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
  and S.partition_date  = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
