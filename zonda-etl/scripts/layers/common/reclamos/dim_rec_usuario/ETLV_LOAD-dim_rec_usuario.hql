set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS max_usuario;
CREATE temporary TABLE IF NOT EXISTS max_usuario as
SELECT
	TRIM(usu.id_usuario) max_id_usuario,
	max(to_date(usu.fecha_alta)) max_fecha_alta
FROM
	bi_corp_staging.rio187_usuarios usu
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_usuarios', dag_id='LOAD_CMN_Cosmos') }}'
GROUP BY TRIM(usu.id_usuario);

INSERT OVERWRITE TABLE bi_corp_common.dim_rec_usuario
SELECT
	TRIM(usu.id_usuario) cod_rec_usuario , 
	TRIM(usu.apellido) ds_rec_usuario_apellido ,
	TRIM(usu.nombre) ds_rec_usuario_nombre ,
	CASE WHEN LENGTH(TRIM(usu.email))=0 THEN NULL ELSE TRIM(usu.email) END as ds_rec_usuario_email ,
	CASE WHEN LENGTH(TRIM(usu.guid_tibco))=0 THEN NULL ELSE TRIM(usu.guid_tibco) END as cod_rec_guid_tibco ,
	TRIM(usu.id_usuario_alta) cod_usuario_alta ,
	to_date(usu.fecha_alta) dt_rec_usuario_alta,
	to_date(usu.fecha_baja) dt_rec_usuario_baja,
	TRIM(usu.id_usuario_modif) cod_usuario_modif ,
	to_date(usu.fecha_modif) dt_rec_usuario_modif,
	TRIM(usu.cod_sucursal) cod_suc_sucursal ,
	TRIM(usu.cod_oficina) cod_rec_oficina,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}' partition_date
FROM
	bi_corp_staging.rio187_usuarios usu
inner join 	max_usuario maxi
on TRIM(usu.id_usuario)=maxi.max_id_usuario
and to_date(usu.fecha_alta)=maxi.max_fecha_alta
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_usuarios', dag_id='LOAD_CMN_Cosmos') }}';