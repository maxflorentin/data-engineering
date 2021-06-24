"
set mapred.job.queue.name=root.dataeng ;

SELECT 'ARG' cod_ren_unidad
, RE.fec_data dt_ren_fechadatos
, ADN.cod_area_negocio_niv_9 cod_ren_areadenegociocorp  
, RE.cod_area_negocio cod_ren_areadenegociolocal 
FROM 
(SELECT DISTINCT RE.fec_data 
      , RE.cod_area_negocio FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE
 WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='RORAC_EXPORT_Tables') }}'
) RE
JOIN bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr ADN ON 
	ADN.partition_date = '2020-08-11'
	AND ADN.cod_area_negocio = RE.cod_area_negocio
    AND ADN.cod_jerq_adn01 = 'JAN01'
"		
