SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table bi_corp_common.stk_zen_usuarios
partition(partition_date)
select
   cast(usr_id as bigint) as cod_zen_usuario
  ,usr_url as ds_zen_url
  ,usr_name as ds_zen_nombre
  ,usr_email as ds_zen_email
  ,usr_created_at as ts_zen_creacion
  ,usr_updated_at as ts_zen_actualizacion
  ,usr_phone as ds_zen_telefono
  ,usr_organization_id as cod_zen_organizacion
  ,usr_role as ds_zen_rol
  ,usr_verified as flag_zen_verificado
  ,usr_external_id as cod_zen_externo
  ,usr_tags  as cod_zen_etiquetas
  ,usr_active as flag_zen_activo
  ,CASE WHEN split(usr_user_fields,"\'")[5] = 'celular2' OR trim(split(usr_user_fields,"\'")[5])=': None,' THEN null split(usr_user_fields,"\'")[5]) as ds_zen_celular
  ,if(split(usr_user_fields,"\'")[7] = 'celular3',null,split(usr_user_fields,"\'")[8]) as ds_zen_celular2
  ,if(split(usr_user_fields,"\'")[9] = 'dni',null,split(usr_user_fields,"\'")[10]) as ds_zen_celular3
  ,if(split(usr_user_fields,"\'")[11] = 'ejecutivo_asignado',null,split(usr_user_fields,"\'")[12]) as ds_zen_numdoc
  ,if(split(usr_user_fields,"\'")[13] = 'ejecutivo_asignado_2',null,split(usr_user_fields,"\'")[15]) as ds_zen_ejecutivo_asignado
  ,if(split(usr_user_fields,"\'")[18] = 'estado_de_cliente',null,split(usr_user_fields,"\'")[18]) as ds_zen_ejecutivo_asignado2
  ,if(split(usr_user_fields,"\'")[20] = 'id_de_cliente',null,split(usr_user_fields,"\'")[20]) as cod_zen_estado_cliente
  ,if(split(usr_user_fields,"\'")[22] = 'ig_followers',null,split(usr_user_fields,"\'")[22]) as cod_per_nup
  ,if(split(usr_user_fields,"\'")[24] = 'ig_login',null,split(usr_user_fields,"\'")[24]) as cod_zen_ig_followers
  ,if(split(usr_user_fields,"\'")[26] = 'ig_valid',null,split(usr_user_fields,"\'")[26]) as cod_zen_ig_login
  ,if(split(usr_user_fields,"\'")[28] = 'mail_ejecutivo',null,split(usr_user_fields,"\'")[28]) as cod_zen_ig_valid
  ,if(split(usr_user_fields,"\'")[31] = 'mail_ejecutivo_2',null,split(usr_user_fields,"\'")[31]) as ds_zen_mail_ejecutivo
  ,if(split(usr_user_fields,"\'")[34] = 'marca_agro',null,split(usr_user_fields,"\'")[34]) as ds_zen_mail_ejecutivo2
  ,if(split(usr_user_fields,"\'")[36] = 'marca_duo',null,split(usr_user_fields,"\'")[36]) as flag_zen_agro
  ,if(split(usr_user_fields,"\'")[38] = 'marca_exciti',null,split(usr_user_fields,"\'")[38]) as flag_zen_duo
  ,if(split(usr_user_fields,"\'")[40] = 'marca_pyme',null,split(usr_user_fields,"\'")[40]) as flag_zen_exciti
  ,if(split(usr_user_fields,"\'")[42] = 'marca_sol',null,split(usr_user_fields,"\'")[42]) as flag_zen_pyme
  ,if(split(usr_user_fields,"\'")[44] = 'marca_vip',null,split(usr_user_fields,"\'")[44]) as flag_zen_sol
  ,if(split(usr_user_fields,"\'")[46] = 'm_remoto',null,split(usr_user_fields,"\'")[46]) as flag_zen_vip
  ,if(split(usr_user_fields,"\'")[48] = 'num_==legajo==_ejecutivo',null,split(usr_user_fields,"\'")[48]) as flag_zen_remoto
  ,if(split(usr_user_fields,"\'")[51] = 'num_==legajo==_ejecutivo_2',null,split(usr_user_fields,"\'")[51]) as ds_legajo_ejecutivo
  ,if(split(usr_user_fields,"\'")[54] = 'producto_de_clientes',null,split(usr_user_fields,"\'")[54]) as ds_legajo_ejecutivo2
  ,if(split(usr_user_fields,"\'")[56] = 'provincia',null,split(usr_user_fields,"\'")[56]) as cod_zen_producto_cliente
  ,if(split(usr_user_fields,"\'")[58] = 'semaforo_rentabilidad',null,split(usr_user_fields,"\'")[58]) as ds_zen_provincia
  ,if(split(usr_user_fields,"\'")[61] = 'semaforo_renta_fac',null,split(usr_user_fields,"\'")[61]) as cod_zen_semaf_rentabilidad
  ,if(split(usr_user_fields,"\'")[65] = 'social_messaging_user_info',null,split(usr_user_fields,"\'")[65]) as cod_zen_semaf_renta_fac
  ,if(split(usr_user_fields,"\'")[68] = 'whatsapp',null,split(usr_user_fields,"\'")[68]) as ds_zen_social_messagin_user
  ,if(split(usr_user_fields,"\'")[71] = 'zona',null,split(usr_user_fields,"\'")[71]) as ds_zen_whatsapp
  ,'Santander Argentina' as ds_zen_origen
from bi_corp_staging.zendesk_usr_santander_ar
WHERE partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk') }}'

union ALL

select
   cast(usr_id as bigint)  as cod_zen_usuario
  ,usr_url as ds_zen_url
  ,usr_name as ds_zen_nombre
  ,usr_email as ds_zen_email
  ,usr_created_at as ts_zen_creacion
  ,usr_updated_at as ts_zen_actualizacion
  ,usr_phone as ds_zen_telefono
  ,usr_organization_id as cod_zen_organizacion
  ,usr_role as ds_zen_rol
  ,usr_verified as flag_zen_verificado
  ,usr_external_id as cod_zen_externo
  ,usr_tags  as cod_zen_etiquetas
  ,usr_active as flag_zen_activo
  ,split(usr_user_fields,"\'")[5]  as ds_zen_celular
  ,split(usr_user_fields,"\'")[8]  as ds_zen_celular2
  ,split(usr_user_fields,"\'")[10]  as ds_zen_celular3
  ,split(usr_user_fields,"\'")[12] as ds_zen_numdoc
  ,split(usr_user_fields,"\'")[15] as ds_zen_ejecutivo_asignado
  ,split(usr_user_fields,"\'")[18] as ds_zen_ejecutivo_asignado2
  ,split(usr_user_fields,"\'")[20] as cod_zen_estado_cliente
  ,split(usr_user_fields,"\'")[22] as cod_per_nup
  ,split(usr_user_fields,"\'")[24] as cod_zen_ig_followers
  ,split(usr_user_fields,"\'")[26] as cod_zen_ig_login
  ,split(usr_user_fields,"\'")[28] as cod_zen_ig_valid
  ,split(usr_user_fields,"\'")[31] as ds_zen_mail_ejecutivo
  ,split(usr_user_fields,"\'")[34] as ds_zen_mail_ejecutivo2
  ,split(usr_user_fields,"\'")[36] as flag_zen_agro
  ,split(usr_user_fields,"\'")[38] as flag_zen_duo
  ,split(usr_user_fields,"\'")[40] as flag_zen_exciti
  ,split(usr_user_fields,"\'")[42] as flag_zen_pyme
  ,split(usr_user_fields,"\'")[44] as flag_zen_sol
  ,split(usr_user_fields,"\'")[46] as flag_zen_vip
  ,split(usr_user_fields,"\'")[48] as flag_zen_remoto
  ,split(usr_user_fields,"\'")[51] as ds_legajo_ejecutivo
  ,split(usr_user_fields,"\'")[54] as ds_legajo_ejecutivo2
  ,split(usr_user_fields,"\'")[56] as cod_zen_producto_cliente
  ,split(usr_user_fields,"\'")[58] as ds_zen_provincia
  ,split(usr_user_fields,"\'")[61] as cod_zen_semaf_rentabilidad
  ,split(usr_user_fields,"\'")[65] as cod_zen_semaf_renta_fac
  ,split(usr_user_fields,"\'")[68] as ds_zen_social_messagin_user
  ,split(usr_user_fields,"\'")[71] as ds_zen_whatsapp
  ,'PUC' as ds_zen_origen
from bi_corp_staging.zendesk_usr_puc_santander
WHERE partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk') }}'

union ALL

select
   usr_id  as cod_zen_usuario
  ,usr_url as ds_zen_url
  ,usr_name as ds_zen_nombre
  ,usr_email as ds_zen_email
  ,usr_created_at as ts_zen_creacion
  ,usr_updated_at as ts_zen_actualizacion
  ,usr_phone as ds_zen_telefono
  ,usr_organization_id as cod_zen_organizacion
  ,usr_role as ds_zen_rol
  ,usr_verified as flag_zen_verificado
  ,usr_external_id as cod_zen_externo
  ,usr_tags  as cod_zen_etiquetas
  ,usr_active as flag_zen_activo
  ,split(usr_user_fields,"\'")[5]  as ds_zen_celular
  ,split(usr_user_fields,"\'")[8]  as ds_zen_celular2
  ,split(usr_user_fields,"\'")[10]  as ds_zen_celular3
  ,split(usr_user_fields,"\'")[12] as ds_zen_numdoc
  ,split(usr_user_fields,"\'")[15] as ds_zen_ejecutivo_asignado
  ,split(usr_user_fields,"\'")[18] as ds_zen_ejecutivo_asignado2
  ,split(usr_user_fields,"\'")[20] as cod_zen_estado_cliente
  ,split(usr_user_fields,"\'")[22] as cod_per_nup
  ,split(usr_user_fields,"\'")[24] as cod_zen_ig_followers
  ,split(usr_user_fields,"\'")[26] as cod_zen_ig_login
  ,split(usr_user_fields,"\'")[28] as cod_zen_ig_valid
  ,split(usr_user_fields,"\'")[31] as ds_zen_mail_ejecutivo
  ,split(usr_user_fields,"\'")[34] as ds_zen_mail_ejecutivo2
  ,split(usr_user_fields,"\'")[36] as flag_zen_agro
  ,split(usr_user_fields,"\'")[38] as flag_zen_duo
  ,split(usr_user_fields,"\'")[40] as flag_zen_exciti
  ,split(usr_user_fields,"\'")[42] as flag_zen_pyme
  ,split(usr_user_fields,"\'")[44] as flag_zen_sol
  ,split(usr_user_fields,"\'")[46] as flag_zen_vip
  ,split(usr_user_fields,"\'")[48] as flag_zen_remoto
  ,split(usr_user_fields,"\'")[51] as ds_legajo_ejecutivo
  ,split(usr_user_fields,"\'")[54] as ds_legajo_ejecutivo2
  ,split(usr_user_fields,"\'")[56] as cod_zen_producto_cliente
  ,split(usr_user_fields,"\'")[58] as ds_zen_provincia
  ,split(usr_user_fields,"\'")[61] as cod_zen_semaf_rentabilidad
  ,split(usr_user_fields,"\'")[65] as cod_zen_semaf_renta_fac
  ,split(usr_user_fields,"\'")[68] as ds_zen_social_messagin_user
  ,split(usr_user_fields,"\'")[71] as ds_zen_whatsapp
  ,'COMEX' as ds_zen_origen
from bi_corp_staging.zendesk_usr_comex_santander
WHERE partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Zendesk') }}';