
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.bt_cc_sivdoperacion PARTITION (partition_date)

Select
distinct op.operacion as cod_cc_operacion,
op.contacto	 as cod_cc_contacto,
ses.sesion   as cod_cc_sesion,
origen		 as cod_cc_campania,
ori.descri	 as ds_cc_campania,
medio		 as cod_cc_medio,
op.llamada	 as cod_cc_llamada,
trim(usuario)		 as cod_cc_legajo,
IF(cast(op.fecha as int) != 0 and cast(hora as int) !=0  ,concat (SUBSTRING(op.fecha,0,4),'-', SUBSTRING(op.fecha,5,2),'-',SUBSTRING(op.fecha,7,2),' ',SUBSTRING(hora,0,2),':', SUBSTRING(hora,3,2),':',SUBSTRING(hora,5,2)),NULL ) as ts_cc_operacion,
gestion 	as cod_cc_gestion,
ges.descri      as ds_cc_gestion,
cast (op.duracion as int)	as fc_cc_duracion,
IF(cast(dia_rel as int) != 0 and cast(hor_rel as int) !=0  ,concat (SUBSTRING(dia_rel,0,4),'-', SUBSTRING(dia_rel,5,2),'-',SUBSTRING(dia_rel,7,2),' ',SUBSTRING(hor_rel,0,2),':', SUBSTRING(hor_rel,3,2),':',SUBSTRING(hor_rel,5,2)),NULL ) as ts_cc_rellamada,
CASE WHEN trim(com_rel) ='' THEN NULL ELSE com_rel END as ds_cc_comentarios,
CASE WHEN unico_ope ='S' THEN 1 ELSE 0 END as flag_cc_unicaoperacion,
IF(CAST(vfecha AS INT)=0,NULL,concat (SUBSTRING(vfecha,0,4),'-', SUBSTRING(vfecha,5,2),'-',SUBSTRING(vfecha,7,2)))		as dt_cc_fecha_visita,
IF(CAST(vdesde AS INT)=0,NULL,concat (SUBSTRING(vdesde,0,2),':', SUBSTRING(vdesde,3,2),':',SUBSTRING(vdesde,5,2)))		as ds_cc_desde_visita,
IF(CAST(vdesde AS INT)=0,NULL,concat (SUBSTRING(vhasta,0,2),':', SUBSTRING(vhasta,3,2),':',SUBSTRING(vhasta,5,2)))		as ds_cc_hasta_visita,
CASE WHEN trim(vcomenta) ='' THEN NULL ELSE vcomenta END as ds_cc_comentario_visita,
CASE WHEN cast(exportada as int) =0 then NULL else trim(exportada)	end as ds_cc_exportada,
CASE WHEN trim(vgestor) ='' THEN NULL ELSE vgestor END as ds_cc_vgestor,
op.canal		as cod_cc_canal,
ca.descri        as ds_cc_canal,
CASE WHEN trim(canal_comunicacion)='' then null else trim(canal_comunicacion) end  as cod_cc_canalcomunicacion,
com.descri               as ds_cc_canalcomunicacion,
cast( CASE WHEN trim(sucursal) ='' THEN NULL ELSE sucursal END as int) as cod_cc_sucursal,
llamada2	as cod_cc_llamada2,
op.partition_date

from bi_corp_staging.rio3_operacion op
left join bi_corp_staging.rio3_canal ca on (ca.codigo=op.canal and ca.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join bi_corp_staging.rio3_tbl_canal_comunicacion com on (com.codigo=op.canal_comunicacion and com.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join bi_corp_staging.rio3_tmk_sesionxoperacion ses on (ses.operacion= op.operacion and ses.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join  bi_corp_staging.rio3_origen ori on (ori.codigo=op.origen and ori.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
left join  bi_corp_staging.rio3_gestion ges on (ges.codigo=op.gestion and ges.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}')
Where op.partition_date ='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
