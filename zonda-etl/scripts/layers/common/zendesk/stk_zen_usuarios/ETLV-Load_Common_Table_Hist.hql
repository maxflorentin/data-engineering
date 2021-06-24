SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS stk_zen_usuarios_temp;
create temporary table stk_zen_usuarios_temp as
SELECT
		 usr_id as cod_zen_usuario
		,usr_url as ds_zen_url
		,usr_name as ds_zen_nombre
		,usr_email as ds_zen_email
		,regexp_replace(regexp_replace(usr_created_at,"T"," "),"Z","") as ts_zen_creacion
		,regexp_replace(regexp_replace(usr_updated_at,"T"," "),"Z","") as ts_zen_actualizacion
		,usr_phone as ds_zen_telefono
		,usr_organization_id as cod_zen_organizacion
		,usr_role as ds_zen_rol
		,usr_verified as flag_zen_verificado
		,usr_external_id as cod_zen_externo
		,usr_tags  as cod_zen_etiquetas
		,usr_active as flag_zen_activo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular') ds_zen_celular
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular2') ds_zen_celular2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular3') ds_zen_celular3
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.dni') ds_zen_numdoc
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado') ds_zen_ejecutivo_asignado
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado_2') ds_zen_ejecutivo_asignado2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.estado_de_cliente') cod_zen_estado_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.id_de_cliente') cod_per_nup
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_followers') cod_zen_ig_followers
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_login') cod_zen_ig_login
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_valid') cod_zen_ig_valid
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo') ds_zen_mail_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo_2') ds_zen_mail_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_agro') flag_zen_agro
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_duo') flag_zen_duo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_exciti') flag_zen_exciti
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_pyme') flag_zen_pyme
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_sol') flag_zen_sol
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_vip') flag_zen_vip
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.m_remoto') flag_zen_remoto
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo') ds_legajo_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo_2') ds_legajo_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.producto_de_clientes') cod_zen_producto_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.provincia') ds_zen_provincia
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_rentabilidad') cod_zen_semaf_rentabilidad
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_renta_fac') cod_zen_semaf_renta_fac
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.social_messaging_user_info') ds_zen_social_messagin_user
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.whatsapp') ds_zen_whatsapp
from bi_corp_staging.zendesk_usr_santander_ar
WHERE partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_usr_santander_ar', dag_id='LOAD_CMN_Zendesk') }}'

union ALL

SELECT
		 usr_id as cod_zen_usuario
		,usr_url as ds_zen_url
		,usr_name as ds_zen_nombre
		,usr_email as ds_zen_email
		,regexp_replace(regexp_replace(usr_created_at,"T"," "),"Z","") as ts_zen_creacion
		,regexp_replace(regexp_replace(usr_updated_at,"T"," "),"Z","") as ts_zen_actualizacion
		,usr_phone as ds_zen_telefono
		,usr_organization_id as cod_zen_organizacion
		,usr_role as ds_zen_rol
		,usr_verified as flag_zen_verificado
		,usr_external_id as cod_zen_externo
		,usr_tags  as cod_zen_etiquetas
		,usr_active as flag_zen_activo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular') ds_zen_celular
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular2') ds_zen_celular2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular3') ds_zen_celular3
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.dni') ds_zen_numdoc
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado') ds_zen_ejecutivo_asignado
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado_2') ds_zen_ejecutivo_asignado2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.estado_de_cliente') cod_zen_estado_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.id_de_cliente') cod_per_nup
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_followers') cod_zen_ig_followers
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_login') cod_zen_ig_login
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_valid') cod_zen_ig_valid
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo') ds_zen_mail_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo_2') ds_zen_mail_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_agro') flag_zen_agro
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_duo') flag_zen_duo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_exciti') flag_zen_exciti
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_pyme') flag_zen_pyme
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_sol') flag_zen_sol
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_vip') flag_zen_vip
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.m_remoto') flag_zen_remoto
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo') ds_legajo_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo_2') ds_legajo_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.producto_de_clientes') cod_zen_producto_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.provincia') ds_zen_provincia
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_rentabilidad') cod_zen_semaf_rentabilidad
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_renta_fac') cod_zen_semaf_renta_fac
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.social_messaging_user_info') ds_zen_social_messagin_user
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.whatsapp') ds_zen_whatsapp
from bi_corp_staging.zendesk_usr_puc_santander
WHERE partition_date=  '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_usr_puc_santander', dag_id='LOAD_CMN_Zendesk_Hist3') }}'

union ALL

SELECT
		 usr_id as cod_zen_usuario
		,usr_url as ds_zen_url
		,usr_name as ds_zen_nombre
		,usr_email as ds_zen_email
		,regexp_replace(regexp_replace(usr_created_at,"T"," "),"Z","") as ts_zen_creacion
		,regexp_replace(regexp_replace(usr_updated_at,"T"," "),"Z","") as ts_zen_actualizacion
		,usr_phone as ds_zen_telefono
		,usr_organization_id as cod_zen_organizacion
		,usr_role as ds_zen_rol
		,usr_verified as flag_zen_verificado
		,usr_external_id as cod_zen_externo
		,usr_tags  as cod_zen_etiquetas
		,usr_active as flag_zen_activo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular') ds_zen_celular
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular2') ds_zen_celular2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.celular3') ds_zen_celular3
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.dni') ds_zen_numdoc
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado') ds_zen_ejecutivo_asignado
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ejecutivo_asignado_2') ds_zen_ejecutivo_asignado2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.estado_de_cliente') cod_zen_estado_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.id_de_cliente') cod_per_nup
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_followers') cod_zen_ig_followers
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_login') cod_zen_ig_login
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.ig_valid') cod_zen_ig_valid
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo') ds_zen_mail_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.mail_ejecutivo_2') ds_zen_mail_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_agro') flag_zen_agro
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_duo') flag_zen_duo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_exciti') flag_zen_exciti
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_pyme') flag_zen_pyme
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_sol') flag_zen_sol
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.marca_vip') flag_zen_vip
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.m_remoto') flag_zen_remoto
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo') ds_legajo_ejecutivo
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.num_==legajo==_ejecutivo_2') ds_legajo_ejecutivo2
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.producto_de_clientes') cod_zen_producto_cliente
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.provincia') ds_zen_provincia
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_rentabilidad') cod_zen_semaf_rentabilidad
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.semaforo_renta_fac') cod_zen_semaf_renta_fac
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.social_messaging_user_info') ds_zen_social_messagin_user
		,get_json_object(regexp_replace(regexp_replace(regexp_replace(regexp_replace(usr_user_fields, "False", "false"), "True","true"), "None","\"none\""),"\'","\"") ,'$.whatsapp') ds_zen_whatsapp
from bi_corp_staging.zendesk_usr_comex_santander
WHERE partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_zendesk_usr_comex_santander', dag_id='LOAD_CMN_Zendesk_Hist3') }}';


-- CALCULO LOS USUARIOS QUE VIENEN ACUMULANDOSE DESDE LAS OTRAS PARTICIONES
DROP TABLE IF EXISTS stk_zen_usuarios_ant;
create temporary table stk_zen_usuarios_ant as
select
        cod_zen_usuario,
        ds_zen_url,
        ds_zen_nombre,
        ds_zen_email,
        ts_zen_creacion,
        ts_zen_actualizacion,
        cod_zen_organizacion,
        ds_zen_rol,
        flag_zen_verificado,
        cod_zen_externo,
        cod_zen_etiquetas,
        flag_zen_activo,
        ds_zen_celular,
        ds_zen_celular2,
        ds_zen_celular3,
        ds_zen_numdoc,
        ds_zen_ejecutivo_asignado,
        ds_zen_ejecutivo_asignado2,
        cod_zen_estado_cliente,
        cod_per_nup,
        cod_zen_ig_followers,
        cod_zen_ig_login,
        cod_zen_ig_valid,
        ds_zen_mail_ejecutivo,
        ds_zen_mail_ejecutivo2,
        flag_zen_agro,
        flag_zen_duo,
        flag_zen_exciti,
        flag_zen_pyme,
        flag_zen_sol,
        flag_zen_vip,
        flag_zen_remoto,
        ds_zen_legajo_ejecutivo,
        ds_zen_legajo_ejecutivo2,
        cod_zen_producto_cliente,
        ds_zen_provincia,
        cod_zen_semaf_rentabilidad,
        cod_zen_semaf_renta_fac,
        ds_zen_social_messagin_user,
        ds_zen_whatsapp,
        '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}' as partition_date
from bi_corp_common.stk_zen_usuarios
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_partition_date', dag_id='LOAD_CMN_Zendesk_Hist3') }}'
;

insert overwrite table bi_corp_common.stk_zen_usuarios
partition(partition_date)
select  distinct
		case when trim(cod_zen_usuario)='None' then cast(null as bigint) else cast(trim(cod_zen_usuario) as bigint) end cod_zen_usuario
		,case when trim(ds_zen_url)='None' then cast(null as string) else trim(ds_zen_url) end ds_zen_url
		,case when trim(ds_zen_nombre)='None' then cast(null as string) else trim(ds_zen_nombre) end ds_zen_nombre
		,case when trim(ds_zen_email)='None' then cast(null as string) else trim(ds_zen_email) end ds_zen_email
		,case when trim(ts_zen_creacion)='None' then cast(null as string) else trim(ts_zen_creacion) end ts_zen_creacion
		,case when trim(ts_zen_actualizacion)='None' then cast(null as string) else trim(ts_zen_actualizacion) end ts_zen_actualizacion
		,case when trim(cod_zen_organizacion)='None' then cast(null as string) else trim(cod_zen_organizacion) end cod_zen_organizacion
		,case when trim(ds_zen_rol)='None' then cast(null as string) else trim(ds_zen_rol) end ds_zen_rol
		,case when trim(flag_zen_verificado)='True' then 1 else 0 end flag_zen_verificado
		,case when trim(cod_zen_externo)='None' then cast(null as string) else trim(cod_zen_externo) end cod_zen_externo
		,case when trim(cod_zen_etiquetas)='None' then cast(null as string) else trim(cod_zen_etiquetas) end cod_zen_etiquetas
		,case when trim(flag_zen_activo)='True' then 1 else 0 end flag_zen_activo
		,case when trim(ds_zen_celular)='none' then cast(null as string) else trim(ds_zen_celular) end ds_zen_celular
		,case when trim(ds_zen_celular2)='none' then cast(null as string) else trim(ds_zen_celular2) end ds_zen_celular2
		,case when trim(ds_zen_celular3)='none' then cast(null as string) else trim(ds_zen_celular3) end ds_zen_celular3
		,case when trim(ds_zen_numdoc)='none' then cast(null as string) else trim(ds_zen_numdoc) end ds_zen_numdoc
		,case when trim(ds_zen_ejecutivo_asignado)='none' then cast(null as string) else trim(ds_zen_ejecutivo_asignado) end ds_zen_ejecutivo_asignado
		,case when trim(ds_zen_ejecutivo_asignado2)='none' then cast(null as string) else trim(ds_zen_ejecutivo_asignado2) end ds_zen_ejecutivo_asignado2
		,case when trim(cod_zen_estado_cliente)='none' then cast(null as string) else trim(cod_zen_estado_cliente) end cod_zen_estado_cliente
		,case when trim(cod_per_nup)='none' then cast(null as int) else trim(cod_per_nup) end cod_per_nup
		,case when trim(cod_zen_ig_followers)='none' then cast(null as string) else trim(cod_zen_ig_followers) end cod_zen_ig_followers
		,case when trim(cod_zen_ig_login)='none' then cast(null as string) else trim(cod_zen_ig_login) end cod_zen_ig_login
		,case when trim(cod_zen_ig_valid)='none' then cast(null as string) else trim(cod_zen_ig_valid) end cod_zen_ig_valid
		,case when trim(ds_zen_mail_ejecutivo)='none' then cast(null as string) else trim(ds_zen_mail_ejecutivo) end ds_zen_mail_ejecutivo
		,case when trim(ds_zen_mail_ejecutivo2)='none' then cast(null as string) else trim(ds_zen_mail_ejecutivo2) end ds_zen_mail_ejecutivo2
		,case when trim(flag_zen_agro)='true' then 1 else 0 end flag_zen_agro
		,case when trim(flag_zen_duo)='true' then 1 else 0 end flag_zen_duo
		,case when trim(flag_zen_exciti)='true' then 1 else 0 end flag_zen_exciti
		,case when trim(flag_zen_pyme)='true' then 1 else 0 end flag_zen_pyme
		,case when trim(flag_zen_sol)='true' then 1 else 0 end flag_zen_sol
		,case when trim(flag_zen_vip)='true' then 1 else 0 end flag_zen_vip
		,case when trim(flag_zen_remoto)='true' then 1 else 0 end flag_zen_remoto
		,case when trim(ds_legajo_ejecutivo)='none' then cast(null as string) else trim(ds_legajo_ejecutivo) end ds_zen_legajo_ejecutivo
		,case when trim(ds_legajo_ejecutivo2)='none' then cast(null as string) else trim(ds_legajo_ejecutivo2) end ds_zen_legajo_ejecutivo2
		,case when trim(cod_zen_producto_cliente)='none' then cast(null as string) else trim(cod_zen_producto_cliente) end cod_zen_producto_cliente
		,case when trim(ds_zen_provincia)='none' then cast(null as string) else trim(ds_zen_provincia) end ds_zen_provincia
		,case when trim(cod_zen_semaf_rentabilidad)='none' then cast(null as string) else trim(cod_zen_semaf_rentabilidad) end cod_zen_semaf_rentabilidad
		,case when trim(cod_zen_semaf_renta_fac)='none' then cast(null as string) else trim(cod_zen_semaf_renta_fac) end cod_zen_semaf_renta_fac
		,case when trim(ds_zen_social_messagin_user)='none' then cast(null as string) else trim(ds_zen_social_messagin_user) end ds_zen_social_messagin_user
		,case when trim(ds_zen_whatsapp)='none' then cast(null as string) else trim(ds_zen_whatsapp) end ds_zen_whatsapp
		,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk') }}' as partition_date
from stk_zen_usuarios_temp

union all
select
        ant.cod_zen_usuario
        ,ant.ds_zen_url
        ,ant.ds_zen_nombre
        ,ant.ds_zen_email
        ,ant.ts_zen_creacion
        ,ant.ts_zen_actualizacion
        ,ant.cod_zen_organizacion
        ,ant.ds_zen_rol
        ,ant.flag_zen_verificado
        ,ant.cod_zen_externo
        ,ant.cod_zen_etiquetas
        ,ant.flag_zen_activo
        ,ant.ds_zen_celular
        ,ant.ds_zen_celular2
        ,ant.ds_zen_celular3
        ,ant.ds_zen_numdoc
        ,ant.ds_zen_ejecutivo_asignado
        ,ant.ds_zen_ejecutivo_asignado2
        ,ant.cod_zen_estado_cliente
        ,ant.cod_per_nup
        ,ant.cod_zen_ig_followers
        ,ant.cod_zen_ig_login
        ,ant.cod_zen_ig_valid
        ,ant.ds_zen_mail_ejecutivo
        ,ant.ds_zen_mail_ejecutivo2
        ,ant.flag_zen_agro
        ,ant.flag_zen_duo
        ,ant.flag_zen_exciti
        ,ant.flag_zen_pyme
        ,ant.flag_zen_sol
        ,ant.flag_zen_vip
        ,ant.flag_zen_remoto
        ,ant.ds_zen_legajo_ejecutivo
        ,ant.ds_zen_legajo_ejecutivo2
        ,ant.cod_zen_producto_cliente
        ,ant.ds_zen_provincia
        ,ant.cod_zen_semaf_rentabilidad
        ,ant.cod_zen_semaf_renta_fac
        ,ant.ds_zen_social_messagin_user
        ,ant.ds_zen_whatsapp
        ,partition_date
from stk_zen_usuarios_ant ant
left join stk_zen_usuarios_temp act
on ant.cod_zen_usuario = cast(trim(act.cod_zen_usuario) as bigint)
where trim(act.cod_zen_usuario) is null;

