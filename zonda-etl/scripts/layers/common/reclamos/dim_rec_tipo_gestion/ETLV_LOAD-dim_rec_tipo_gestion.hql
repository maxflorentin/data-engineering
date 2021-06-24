set mapred.job.queue.name=root.dataeng;

-- Calculamos la maxima particion
DROP TABLE IF EXISTS max_partition_tipo;
CREATE TEMPORARY TABLE max_partition_tipo AS
SELECT tpo_gestion, max(partition_date) max_partition_date
FROM bi_corp_staging.rio56_tipo_gestion
where partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
group by tpo_gestion;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_tipo_gestion
SELECT
	cod_entidad ,
	tpo_gestion ,
	desc_tpo_gest ,
	desc_detall_tpo_gest ,
	est_tpo_gest ,
	cod_rec_tipo_gestion_cosmos ,
	desc_tipo_gestion_cosmos ,
	flag_rec_informa_bcra ,
	ds_rec_prefijo_ticket ,
	'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}' partition_date
FROM
	(
	SELECT
		ori_sgc.cod_entidad ,
		ori_sgc.tpo_gestion , 
		ori_sgc.desc_tpo_gest , 
		ori_sgc.desc_detall_tpo_gest , 
		ori_sgc.est_tpo_gest , 
		TRIM(ori.id_tipo_gestion) cod_rec_tipo_gestion_cosmos , 
		TRIM(UPPER(ori.desc_tipo_gestion)) desc_tipo_gestion_cosmos ,
		CASE WHEN TRIM(ori.informa_bcra)='S' then 1 else 0 end flag_rec_informa_bcra , 
		TRIM(ori.prefijo_ticket) ds_rec_prefijo_ticket , 
		ori_sgc.partition_date
	FROM
		bi_corp_staging.rio56_tipo_gestion ori_sgc
	LEFT OUTER JOIN bi_corp_staging.rio187_tipos_gestion ori ON
		ori_sgc.tpo_gestion = TRIM(ori.id_tipos_gestion_sgc)
		AND ori.fecha_baja = 'null'
		and ori.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_tipos_gestion', dag_id='LOAD_CMN_Cosmos') }}'
    INNER JOIN max_partition_tipo b
    on trim(ori_sgc.tpo_gestion)= trim(b.tpo_gestion)
    and ori_sgc.partition_date =b.max_partition_date
     )A;