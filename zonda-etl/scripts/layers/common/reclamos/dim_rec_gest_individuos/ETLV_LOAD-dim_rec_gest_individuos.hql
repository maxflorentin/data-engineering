SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.dim_rec_gest_individuos

SELECT
	trim(gc_individuo_gest.cod_entidad) cod_rec_entidad ,
	trim(gc_individuo_gest.ide_gestion_sector) cod_rec_sector ,
	trim(gc_individuo_gest.ide_gestion_nro) cod_rec_gestion_nro ,
	trim(gc_individuo_gest.tpo_doc_indi) cod_rec_tipo_doc_indi ,
	trim(gc_individuo_gest.nro_doc_indi) int_rec_nro_doc_indi ,
	date_format(gc_individuo_gest.fec_naci_indi,'yyyy-MM-dd') dt_rec_nacimiento_indi ,
	trim(gc_individuo_gest.mar_sex_indi) cod_rec_sexo_indi ,
	trim(gc_individuos.ape_indi) ds_rec_apellido_indi ,
	trim(gc_individuos.nom_indi) ds_rec_nombre_indi ,
	CAST(gc_individuos.ide_nup_indi AS INT) cod_per_nup ,
	trim(gc_individuos.cod_suc_indi) cod_suc_sucursal_indi ,
	trim(gc_individuos.cod_segm_indi) cod_rec_segmento_indi ,
	trim(gc_individuos.cod_crm_indi) cod_rec_crm_indi ,
	clasif_crm.ds_rec_crm ds_rec_crm_indi ,
	clasif_crm.cod_rec_clasif_select cod_rec_clasif_select_indi ,
	CASE WHEN LOWER(gc_individuos.semf_ingreso_crm) = 'null' THEN NULL ELSE gc_individuos.semf_ingreso_crm END ds_rec_semf_ingreso_crm_indi ,
	CASE WHEN LOWER(gc_individuos.semf_renta_crm) = 'null' THEN NULL ELSE gc_individuos.semf_renta_crm END ds_rec_semf_renta_crm_indi ,
	CASE WHEN LOWER(gc_individuos.semf_riesgo_crm) = 'null' THEN NULL ELSE gc_individuos.semf_riesgo_crm END ds_rec_semf_riesgo_crm_ind ,
	CASE WHEN LOWER(gc_individuos.rentabilidad_promedio) = 'null' THEN NULL ELSE gc_individuos.rentabilidad_promedio END fc_rec_rentabilidad_promedio_indi ,
	CASE WHEN LOWER(gc_individuos.color_semaforo) = 'null' THEN NULL ELSE gc_individuos.color_semaforo END ds_rec_color_semf_indi ,
	CASE WHEN LOWER(gc_individuos.color_semaf_riesgo) = 'null' THEN NULL ELSE gc_individuos.color_semaf_riesgo END ds_rec_color_semf_riesgo_indi ,
	gc_individuo_gest.partition_date
FROM
	bi_corp_staging.rio56_gc_individuo_gestiones gc_individuo_gest
LEFT JOIN bi_corp_staging.rio56_gc_individuos gc_individuos ON
	gc_individuo_gest.nro_doc_indi = gc_individuos.nro_doc_indi
	AND gc_individuo_gest.tpo_doc_indi = gc_individuos.tpo_doc_indi
	AND gc_individuos.partition_date = gc_individuo_gest.partition_date
	AND gc_individuos.ide_nup_indi != '00000000'
	AND gc_individuo_gest.fec_naci_indi = gc_individuos.fec_naci_indi
	AND gc_individuo_gest.mar_sex_indi = gc_individuos.mar_sex_indi
LEFT JOIN bi_corp_common.dim_rec_clasif_crm clasif_crm ON
	gc_individuos.cod_crm_indi = clasif_crm.cod_rec_crm
	AND LOWER(clasif_crm.cod_rec_tipo_persona) = 'f'
WHERE
	gc_individuo_gest.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}';
  