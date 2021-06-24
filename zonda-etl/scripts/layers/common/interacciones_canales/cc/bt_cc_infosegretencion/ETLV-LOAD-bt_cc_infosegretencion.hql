SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  OVERWRITE TABLE bi_corp_common.bt_cc_infosegretencion
PARTITION (partition_date)


SELECT
DISTINCT          CASE WHEN trim(crec_fe_proceso)='null'
         	  THEN NULL ELSE SUBSTRING(crec_fe_proceso,1,19) END  									ts_cc_fecha,
         CASE WHEN trim(crec_tp_cuestionario)='null'
         	  THEN NULL ELSE crec_tp_cuestionario END 								ds_cc_cuestionario,
         CASE WHEN NVL(b.rv_meaning, c.rv_meaning)='null'
         	  THEN NULL ELSE NVL(b.rv_meaning, c.rv_meaning) END 					ds_cc_signigicado,
         CASE WHEN trim(caus_capj_cd_sucursal)='null'
         	  THEN NULL ELSE caus_capj_cd_sucursal END          														cod_cc_sucursal,
         CASE WHEN trim(crec_cd_usuario)='null'
         	  THEN NULL ELSE crec_cd_usuario END          															cod_cc_legajo,
         CASE WHEN trim(caus_nm_usuario)='null'
         	  THEN NULL ELSE caus_nm_usuario END          															ds_cc_apellido_nombre,
         CASE WHEN trim(crec_cd_ramo_nuevo)='null'
         	  THEN NULL ELSE crec_cd_ramo_nuevo END          														cod_cc_ramonuevo,
         CASE WHEN trim(carp_de_ramo)='null'
         	  THEN NULL ELSE carp_de_ramo END          																ds_cc_ramo,
         CASE WHEN trim(crec_nu_poliza_nuevo)='null'
         	  THEN NULL ELSE crec_nu_poliza_nuevo END          														ds_cc_num_polizanuevo,
         CASE WHEN trim(crec_nu_certificado_nuevo)='null'
         	  THEN NULL ELSE crec_nu_certificado_nuevo END          													ds_cc_num_certificadonuevo,
         CASE WHEN trim(cace_capu_cd_producto)='null'
         	  THEN NULL ELSE cace_capu_cd_producto END          														cod_cc_producto,
         CASE WHEN trim(crec_cd_sucursal_emision)='null'
         	  THEN NULL ELSE crec_cd_sucursal_emision END          													cod_cc_sucursal_emision,
         CASE WHEN trim(capj_de_sucursal)='null'
         	  THEN NULL ELSE capj_de_sucursal END          															ds_cc_sucursal,
         CASE WHEN trim(canl_cd_canal)='null'
         	  THEN NULL ELSE canl_cd_canal END          																cod_cc_canal,
         CASE WHEN trim(canl_de_canal)='null'
         	  THEN NULL ELSE canl_de_canal END          																ds_cc_canal,
         CASE WHEN trim(cace_st_certificado)='null'
         	  THEN NULL ELSE cace_st_certificado END          														cod_cc_estado_certificado,
         CASE WHEN trim(SUBSTRING(a.rv_meaning, 1, 30))='null'
         	  THEN NULL ELSE SUBSTRING(a.rv_meaning, 1, 30) END           											ds_cc_estado,
         CASE WHEN trim(cace_cacn_cd_nacionalidad)='null'
         	  THEN NULL ELSE cace_cacn_cd_nacionalidad END           													cod_cc_nacionalidad,
         CASE WHEN trim(cace_cacn_nu_cedula_rif)='null'
         	  THEN NULL ELSE cace_cacn_nu_cedula_rif END           													ds_cc_num_cedularif,
         CASE WHEN trim(cacn_nm_apellido_razon)='null'
         	  THEN NULL ELSE cacn_nm_apellido_razon END           													ds_cc_apellido_razon,
         CASE WHEN trim(SUBSTRING(cace_fe_hasta,1,10))='null'
         	  THEN NULL ELSE SUBSTRING(cace_fe_hasta,1,10) END           												dt_cc_fecha_hasta,
         CASE WHEN crec_fe_comision='null'
          	  THEN NULL ELSE SUBSTRING(crec_fe_comision,1,10)	END						dt_cc_fecha_comision,
         CASE WHEN trim(crec_cd_ramo)='null'
         	  THEN NULL ELSE crec_cd_ramo END           																cod_cc_ramo,
         CASE WHEN trim(crec_nu_poliza)='null'
         	  THEN NULL ELSE crec_nu_poliza END           															ds_cc_num_poliza,
         CASE WHEN trim(crec_nu_certificado)='null'
         	  THEN NULL ELSE crec_nu_certificado END           														ds_cc_num_certificado,
         CAST(cacn_nu_nup AS BIGINT)																				cod_per_nup,

          cue.partition_date                                                        partition_date

          FROM bi_corp_staging.rio6_cart_retencion_cuestionarios cue
          LEFT JOIN bi_corp_staging.rio6_cart_retencion_resultados res ON (crec_nu_interaccion = cres_nu_interaccion and res.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
          LEFT JOIN(
          SELECT *
          FROM bi_corp_staging.rio6_cart_certificados
          WHERE partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}"
           AND cace_casu_cd_sucursal= '1'
          )
          cer  ON (
           trim(cace_carp_cd_ramo) = trim(crec_cd_ramo_nuevo)
          AND trim(cace_capo_nu_poliza) = trim(crec_nu_poliza_nuevo)
          AND trim(cace_nu_certificado)= trim(crec_nu_certificado_nuevo)
          )
          LEFT JOIN bi_corp_staging.rio6_cart_clientes cli ON (cacn_cd_nacionalidad= cace_cacn_cd_nacionalidad
          AND cacn_nu_cedula_rif = cace_cacn_nu_cedula_rif and cli.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")

          LEFT JOIN bi_corp_staging.rio6_cart_usuarios us ON (caus_cd_usuario= crec_cd_usuario and us.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
          LEFT JOIN bi_corp_staging.rio6_cart_sucursal_banco suc ON (capj_cd_sucursal = crec_cd_sucursal_emision and suc.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
          LEFT JOIN bi_corp_staging.rio6_cart_canales can ON (canl_cd_canal = capj_canl_cd_canal and can.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
          LEFT JOIN bi_corp_staging.rio6_cart_ramos_polizas pol ON (carp_cd_ramo = crec_cd_ramo_nuevo and pol.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}")
          LEFT JOIN (
          	SELECT *
          	FROM bi_corp_staging.rio6_cg_ref_codes
          	WHERE rv_domain = 'CART_CERTIFICADOS.CACE_ST_CERTIFICADO'
          	and partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}") a
          	ON (trim(a.rv_low_value) = trim(cace_st_certificado))
          LEFT JOIN (
          	SELECT *
          	FROM bi_corp_staging.rio6_cg_ref_codes
          	WHERE rv_domain = 'RETENCION_RESULTADOS_POSITIVOS'
          	and partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}") b
          	ON (b.rv_high_value = cres_cd_resultado )
          LEFT JOIN (
          	SELECT *
          	FROM bi_corp_staging.rio6_cg_ref_codes
          	WHERE rv_domain = 'RETENCION_CAUSA_BAJA'
          	and partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}") c
          	 ON (c.rv_high_value = crec_cd_causa_baja )

         WHERE cue.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}";