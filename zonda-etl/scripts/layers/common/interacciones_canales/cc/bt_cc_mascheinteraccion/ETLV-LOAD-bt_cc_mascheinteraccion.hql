SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_mascheinteraccion PARTITION (partition_date)

SELECT
DISTINCT   cd_interaccion 	     as cod_cc_interaccion,
  CAST(nup AS BIGINT)				           as cod_per_nup,
  cd_ejecutivo	       as cod_cc_legajo,
  from_unixtime(unix_timestamp(TRIM( SUBSTRING(I.dt_inicio, 0,19 )),'yyyy-MM-dd HH:mm:ss'))     as ts_cc_inicio,
  from_unixtime(unix_timestamp(TRIM( SUBSTRING(I.dt_cierre, 0,19 )),'yyyy-MM-dd HH:mm:ss'))     as ts_cc_cierre,
  CASE WHEN I.cd_canal_comunicacion = 'null' THEN  NULL ELSE TRIM(I.cd_canal_comunicacion) END  as cod_cc_canalcomunicacion,
  co.descripcion                                                                                as  ds_cc_canalcomunicacion,
  CASE WHEN I.cd_canal_venta = 'null' THEN  NULL ELSE TRIM(I.cd_canal_venta) END                as cod_cc_canalventa,
  ve.descripcion                                                                                as  ds_cc_canalventa,
  cd_sucursal          as cod_cc_sucursal,
  CASE WHEN I.ds_comentario = 'null' THEN  NULL ELSE TRIM(I.ds_comentario) END                  as ds_cc_comentario,
  CASE WHEN I.motivo = 'null' THEN  NULL ELSE TRIM(I.motivo) END                                as cod_cc_motivo,
  CASE WHEN I.nro_llamado = 'null' THEN  NULL ELSE TRIM(I.nro_llamado) END                      as cod_cc_nrollamado,
  CASE WHEN I.origen = 'null' THEN  NULL ELSE TRIM(I.origen) END as cod_cc_origen,
  CASE WHEN I.cd_interaccion_padre = 'null' THEN  NULL ELSE TRIM(I.cd_interaccion_padre) END    as cod_cc_interaccionpadre,
  CASE WHEN I.numero_ticket = 'null' THEN  NULL ELSE TRIM(I.numero_ticket) END                  as cod_cc_nroticket,
  i.partition_date
FROM
  bi_corp_staging.rio226_tbl_interaccion I
LEFT JOIN bi_corp_staging.rio226_tbl_canal_venta ve on (ve.codigo=I.cd_canal_venta
and ve.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
LEFT JOIN bi_corp_staging.rio226_tbl_canal_comunicacion co on (co.codigo=I.cd_canal_comunicacion
and co.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
Where
  i.partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
