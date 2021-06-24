set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_casuistica_motivos
select
	case when trim(id_casuistica)='null' then null else trim(id_casuistica) end as cod_rec_casuistica,
	case when trim(cod_motivo)='null' then null else  trim(cod_motivo) end as cod_rec_motivo,
	case when trim(desc_motivo)='null' then null else  trim(desc_motivo)end  as ds_rec_motivo,
	case when trim(ind_texto_libre)='null' then null else trim(ind_texto_libre) end  as flag_rec_casuistica,
	case when trim(id_usuario_alta)='null' then null else trim(id_usuario_alta) end  as cod_rec_usuario_alta,
	case when trim(fecha_alta)='null' then null else trim(fecha_alta) end  as ts_rec_alta,
	case when trim(id_usuario_modif)='null' then null else trim(id_usuario_modif) end  as cod_rec_usuario_modif,
	case when trim(fecha_modif)='null' then null else trim(fecha_modif) end  as ts_rec_modif,
	case when trim(fecha_baja)='null' then null else trim(fecha_baja) end  as ts_rec_baja,
	case when trim(id_motivo)='null' then null else trim(id_motivo) end  as cod_rec_idmotivo,
	case when trim(id_tipo_gestion)='null' then null else trim(id_tipo_gestion) end  as cod_rec_tipo_gestion,
	partition_date
from  bi_corp_staging.rio187_casuistica_motivos
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_casuistica_motivos', dag_id='LOAD_CMN_Cosmos') }}';