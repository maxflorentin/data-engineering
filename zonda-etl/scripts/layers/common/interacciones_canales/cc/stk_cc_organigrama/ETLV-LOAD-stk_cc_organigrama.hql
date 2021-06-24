SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT  overwrite TABLE bi_corp_common.stk_cc_organigrama PARTITION (partition_date)

SELECT
DISTINCT  		 e.c_empresa as cod_cc_empresa,
         e.d_empresa as ds_cc_empresa,
         n.c_negociorh as cod_cc_negocio,
         n.d_negocio as ds_cc_negocio,
         us4.c_usuariorh as cod_cc_jefecomercial,
         concat(us4.d_apellido ,',',us4.d_nombre) as ds_cc_jefecomercial,
         us3.c_usuariorh as cod_cc_jefesupervisor,
         concat(us3.d_apellido ,',',us3.d_nombre) as ds_cc_jefesupervisor,
         m.d_modo as ds_cc_modo,
         concat(us2.d_apellido ,'-',us2.d_nombre) as ds_cc_supervisor,
         g.c_gruporh as cod_cc_grupo,
         g.d_grupo as ds_cc_grupo,
         us.n_leg_admin as cod_cc_legajo_admin,
         us.c_usuariorh as cod_cc_legajo_usuariorh,
         TRIM(UPPER(us.c_usuariont)) AS cod_cc_legajo_usuariont,
         concat(us.d_apellido ,', ',us.d_nombre) AS ds_cc_usuario,
         us.partition_date
	FROM 	bi_corp_staging.rio75_rh_usuarios us

	inner join bi_corp_staging.rio75_rh_usuarios_grupos ug
	 on us.c_usuariorh = ug.c_usuariorh

	inner join bi_corp_staging.rio75_rh_grupos g
	 on  ug.c_gruporh = g.c_gruporh

	inner join bi_corp_staging.rio75_rh_usuarios us2
	 on g.c_usu_resp = us2.c_usuariorh

	inner join bi_corp_staging.rio75_rh_negocios n
	 on g.c_negociorh = n.c_negociorh

	inner join  bi_corp_staging.rio75_rh_usuarios us3
	 on n.c_usu_resp = us3.c_usuariorh

	inner join bi_corp_staging.rio75_rh_usuarios_grupos ug2
	 on us3.c_usuariorh = ug2.c_usuariorh

	inner join bi_corp_staging.rio75_rh_grupos g2
	 on g2.c_gruporh = ug2.c_gruporh

	inner join bi_corp_staging.rio75_rh_usuarios us4
	 on us4.c_usuariorh = g2.c_usu_resp

	inner join bi_corp_staging.rio75_rh_modos m
	 on g.c_modo = m.c_modo

	inner join bi_corp_staging.rio75_rh_usuarios_modalidad um
	 on us.c_usuariorh = um.c_usuariorh

	inner join bi_corp_staging.rio75_rh_empresas e
	 on e.c_empresa = um.c_empresa

	Where ug.f_baja = 'null'
      AND ug.i_activo = 'S'
      AND ug2.f_baja = 'null'
      AND ug2.i_activo = 'S'
      AND (us.f_egreso = 'null' or concat (SUBSTRING(us.f_egreso,0,4),'-', SUBSTRING(us.f_egreso,5,2),'-',SUBSTRING(us.f_egreso,7,2)) >= us.partition_date)
      AND (um.f_baja = 'null' or  concat (SUBSTRING(UM.f_baja,0,4),'-', SUBSTRING(UM.f_baja,5,2),'-',SUBSTRING(UM.f_baja,7,2)) >= us.partition_date)
      AND us.partition_date    = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND ug.partition_date    = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND g.partition_date     = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND us2.partition_date   = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND n.partition_date     = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND us3.partition_date   = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND ug2.partition_date   = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND g2.partition_date    = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND us4.partition_date   = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND m.partition_date     = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND um.partition_date    = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}'
	    AND e.partition_date     = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_CONTACT_CENTER') }}';
