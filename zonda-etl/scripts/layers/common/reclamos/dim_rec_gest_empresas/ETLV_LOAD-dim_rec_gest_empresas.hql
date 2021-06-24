SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_rec_gest_empresas

SELECT
	trim(gc_empresa_gest.cod_entidad) cod_rec_entidad ,
	trim(gc_empresa_gest.ide_gestion_sector) cod_rec_sector ,
	trim(gc_empresa_gest.ide_gestion_nro) cod_rec_gestion_nro ,
	trim(gc_empresa_gest.tpo_doc_empr) cod_rec_tipo_doc_empr ,
	trim(gc_empresa_gest.nro_doc_empr) cod_rec_nro_doc_empr ,
	trim(gc_empresa_gest.sec_doc_empr) cod_rec_sec_empr ,
	trim(gc_empresas.tpo_empr) cod_rec_tipo_empr ,
	case when date_format(gc_empresas.fec_ini_activ_empr,'yyyy-MM-dd') = '0001-01-01' then null else date_format(gc_empresas.fec_ini_activ_empr,'yyyy-MM-dd') end dt_rec_ini_activ_empr,
	trim(gc_empresas.cod_activ_empr) cod_rec_activ_empr ,
	trim(gc_empresas.nom_comer_empr) ds_rec_nom_comer_empr ,
	trim(gc_empresas.razon_social_empr) ds_rec_razon_social_empr ,
	CAST(gc_empresas.ide_nup_empr AS INT) cod_per_nup ,
	trim(gc_empresas.ape_resp_empr) ds_rec_apellido_resp_empr ,
	trim(gc_empresas.nom_resp_empr) ds_rec_nombre_resp_empr ,
	trim(gc_empresas.tpo_doc2_empr) cod_rec_tipo_doc2_empr ,
	trim(gc_empresas.nro_doc2_empr) int_rec_tipo_doc2_empr ,
	trim(gc_empresas.cod_suc_empr) cod_suc_sucursal_empr ,
	trim(gc_empresas.cod_segm_empr) cod_rec_segmento_empr ,
	trim(gc_empresas.cod_crm_empr) cod_rec_crm_empr ,
	clasif_crm.ds_rec_crm ds_rec_crm_empr ,
	clasif_crm.cod_rec_clasif_select cod_rec_clasif_select_empr ,
	CASE WHEN LOWER(gc_empresas.semf_ingreso_crm) = 'null' THEN NULL ELSE gc_empresas.semf_ingreso_crm END ds_rec_semf_ingreso_crm_empr ,
	CASE WHEN LOWER(gc_empresas.semf_renta_crm) = 'null' THEN NULL ELSE gc_empresas.semf_renta_crm END ds_rec_semf_renta_crm_empr ,
	CASE WHEN LOWER(gc_empresas.semf_riesgo_crm) = 'null' THEN NULL ELSE gc_empresas.semf_riesgo_crm END ds_rec_semf_riesgo_crm_empr ,
	CASE WHEN LOWER(gc_empresas.rentabilidad_promedio) = 'null' THEN NULL ELSE gc_empresas.rentabilidad_promedio END fc_rec_rentabilidad_promedio_empr ,
	CASE WHEN LOWER(gc_empresas.color_semaforo) = 'null' THEN NULL ELSE gc_empresas.color_semaforo END ds_rec_color_semf_empr ,
	CASE WHEN LOWER(gc_empresas.color_semaf_riesgo) = 'null' THEN NULL ELSE gc_empresas.color_semaf_riesgo END ds_rec_color_semf_riesgo_empr ,
	gc_empresa_gest.partition_date
FROM bi_corp_staging.rio56_gc_empresa_gestiones gc_empresa_gest
LEFT JOIN bi_corp_staging.rio56_gc_empresas gc_empresas ON
	gc_empresa_gest.nro_doc_empr = gc_empresas.nro_doc_empr
	AND gc_empresa_gest.tpo_doc_empr = gc_empresas.tpo_doc_empr
	AND gc_empresa_gest.partition_date= gc_empresas.partition_date
	AND gc_empresa_gest.sec_doc_empr= gc_empresas.sec_doc_empr
LEFT JOIN bi_corp_common.dim_rec_clasif_crm clasif_crm ON
	gc_empresas.cod_crm_empr = clasif_crm.cod_rec_crm
	AND LOWER(clasif_crm.cod_rec_tipo_persona) = 'j'
WHERE
	gc_empresa_gest.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
