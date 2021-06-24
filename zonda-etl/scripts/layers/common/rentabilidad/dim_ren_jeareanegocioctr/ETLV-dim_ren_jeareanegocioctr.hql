"
set mapred.job.queue.name=root.dataeng;


INSERT OVERWRITE TABLE bi_corp_common.dim_ren_jeareanegocioctr
SELECT 
	 TRIM(cod_pais) cod_ren_pais ,
	 TRIM(cod_padre) cod_ren_areanegociopadre ,
	 TRIM(cod_hijo) cod_ren_areanegociohijo ,
	 TRIM(des_hijo) ds_ren_areanegociohijo ,
	 TRIM(cod_jerarquia) cod_ren_areanegociojerarquia ,
	 IF(ind_consolidacion = 'null', NULL, TRIM(ind_consolidacion)) flag_ren_consolidacion ,
	 IF(cod_primer_corporativo = 'null', NULL, TRIM(cod_primer_corporativo)) cod_ren_primercorporativo ,
	 TRIM(userid_umo) cod_ren_userumo ,
	 from_unixtime(unix_timestamp(TRIM(SUBSTRING(timest_umo, 0,19)),'yyyy-MM-dd HH:mm:ss')) ts_ren_timestumo ,
	 TRIM(num_nivel) int_ren_nivel ,
	 TRIM(ind_corporativo) cod_ren_indcorporativo ,
	 TRIM(num_orden) int_ren_orden ,
	 partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_je_dwh_area_negocio_ctr 
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio157_ms0_dm_je_dwh_area_negocio_ctr', dag_id='LOAD_CMN_Rentabilidad_Daily') }}'
	AND cod_jerarquia = 'JAN01' ; 
"