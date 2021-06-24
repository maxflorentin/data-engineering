set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT overwrite TABLE bi_corp_staging.abae_maestarj_aux_bajas
PARTITION(partition_date)

Select
db.tar_numero_tarjeta,
db.tar_sucursal_adm,
db.tar_entidad_ppal,
db.tar_centro_ppal,
db.tar_cuenta_ppal_prod,
db.tar_cuenta_ppal_nro,
db.tar_divisa_ppal,
db.tar_firmante_ppal,
db.tar_tipo_cuenta_banelco,
db.tar_tipo_tarjeta,
db.tar_estado_tarjeta,
db.tar_cod_lim,
db.tar_fec_vto,
db.tar_fec_alta,
db.tar_usuario_alta,
db.tar_fec_habilitacion,
db.tar_fec_ult_act,
db.tar_usuario_act,
db.tar_fec_envio_banelco,
db.tar_fec_act_banelco,
db.tar_estado_renov,
db.tar_marca_pin,
db.tar_fec_embz,
db.tar_fec_pin,
db.tar_usuario_pin,
db.tar_fec_cbio_suc,
db.tar_can_cuentas,
db.tar_marca_tar,
db.tar_nro_tar_ant,
db.tar_cod_reem,
db.tar_cod_dist,
db.tar_tipo_plas,
db.tar_tipo_plas_origi,
db.tar_cod_comercial,
db.tar_cod_emision,
db.tar_nombre_foto,
db.tar_trackii,
db.tar_cuarta_linea,
db.tar_mot_emision,
db.tar_retorno_banelco,
db.tar_nup,
db.tar_ppal_subprod,
db.tar_tracki_disc1,
db.tar_tracki_disc2,
db.tar_tracki_disc3,
db.tar_fec_utiles,
db.tar_cod_utiles,
db.tar_fec_estado_renov,
db.tar_tarjeta_coord,
db.tar_inhabil_cbio,
db.tar_cod_destino,
db.tar_adhesion_otp,
db.tar_canal,
db.tar_permisionaria,
db.tar_chip,
db.tar_secuencial,
db.partition_date as tar_fec_baja,
db.partition_date

from
bi_corp_staging.abae_maestarj db

Where
db.tar_estado_tarjeta = '9'
and  db.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_STG_Cuentas_Abae') }}'
and db.tar_numero_tarjeta not in (
									Select dbant.tar_numero_tarjeta
									from
									bi_corp_staging.abae_maestarj dbant
									Where  dbant.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_STG_Cuentas_Abae') }}'
									and dbant.tar_estado_tarjeta = '9'
									UNION ALL
									Select b.tar_numero_tarjeta
									from bi_corp_staging.abae_maestarj_aux_bajas b
									Where b.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_STG_Cuentas_Abae') }}'

									);
