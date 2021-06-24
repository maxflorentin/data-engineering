set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

--------------Creo un tabla temporal con la ultima particion de los datos de Cuenta - Cliente

DROP TABLE IF EXISTS pedt008_temp;
CREATE TEMPORARY TABLE pedt008_temp AS
select p.pecodofi, p.penumcon, p.pecodpro, p.pecodsub, p.penumper,p.pefecalt,concat('0', p.pecodpro, SUBSTRING(p.penumcon, 4)) AS cuenta
from bi_corp_staging.malpe_pedt008 p
inner join
(select pecodofi, penumcon, pecodpro, pecodsub, min(cast(case when REGEXP_REPLACE(PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(PEORDPAR, "^0+", '') end as int)) PEORDPAR
from bi_corp_staging.malpe_pedt008
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Cuentas_Abae') }}'
AND pecalpar = 'TI'
AND pecodpro IN ('02', '03', '04', '05', '06', '07', '08', '98', '99', '14')
--AND peestrel='A'
group by pecodofi, penumcon, pecodpro, pecodsub) t2
on t2.pecodofi=p.pecodofi and t2.penumcon=p.penumcon and t2.pecodpro=p.pecodpro and t2.pecodsub=p.pecodsub and t2.peordpar=cast(case when REGEXP_REPLACE(p.PEORDPAR, "^0+", '')='' then '0' else REGEXP_REPLACE(p.PEORDPAR, "^0+", '') end as int)
where p.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_malpe_pedt008', dag_id='LOAD_CMN_Cuentas_Abae') }}'
AND p.pecalpar = 'TI';


DROP TABLE IF EXISTS stk_cue_tarjetas_debito_actual;
create temporary table stk_cue_tarjetas_debito_actual as
SELECT
		trim(maestarje.tar_centro_ppal) as cod_suc_sucursal
		,trim(maestarje.tar_cuenta_ppal_nro) as cod_cue_cuenta
		,substring(trim(maestarje.tar_cuenta_ppal_prod),2,2) as cod_cue_producto
		,CASE WHEN LENGTH(trim(maestarje.tar_ppal_subprod))=0 THEN NULL ELSE trim(maestarje.tar_ppal_subprod) end  as cod_cue_subproducto
		,trim(maestarje.tar_divisa_ppal) as cod_cue_divisa
		,trim(maestarje.tar_firmante_ppal) as cod_cue_firmante
		,CAST(trim(maestarje.tar_numero_tarjeta) AS bigint) as cod_cue_tarjeta
		,trim(maestarje.tar_sucursal_adm) as cod_suc_sucursal_adm
		,trim(maestarje.tar_entidad_ppal) as cod_cue_entidad
		,trim(maestarje.tar_tipo_cuenta_banelco) as cod_cue_tipo_cuenta_banelco
		,trim(maestarje.tar_tipo_tarjeta) as cod_cue_tipo_tarjeta
		,trim(maestarje.tar_estado_tarjeta) as cod_cue_tarjeta_estado
		,cast(null as string) as dt_cue_baja
		,trim(maestarje.tar_cod_lim) as cod_cue_lim
		,from_unixtime(unix_timestamp(maestarje.tar_fec_vto ,'yyyyMM'), 'yyyy-MM') as ds_cue_mes_vencimiento
		,from_unixtime(unix_timestamp(maestarje.tar_fec_alta ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_alta
		,CASE WHEN LENGTH(trim(maestarje.tar_usuario_alta))=0 THEN NULL ELSE trim(maestarje.tar_usuario_alta) end  as cod_cue_legajo_alta
		,from_unixtime(unix_timestamp(maestarje.tar_fec_habilitacion ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_habilitacion
		,from_unixtime(unix_timestamp(maestarje.tar_fec_ult_act ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_ult_act
		,CASE WHEN LENGTH(trim(maestarje.tar_usuario_act))=0 THEN NULL ELSE trim(maestarje.tar_usuario_act) end  as cod_cue_legajo_act
		,CASE WHEN trim(maestarje.tar_fec_envio_banelco)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_envio_banelco ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_envio_banelco
		,from_unixtime(unix_timestamp(maestarje.tar_fec_act_banelco ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_act_banelco
		,CASE WHEN LENGTH(trim(maestarje.tar_estado_renov))=0 THEN NULL ELSE trim(maestarje.tar_estado_renov) end  as cod_cue_estado_renov
		,CASE WHEN LENGTH(trim(maestarje.tar_marca_pin))=0 THEN NULL ELSE trim(maestarje.tar_marca_pin) end  as cod_cue_marca_pin
		,CASE WHEN trim(maestarje.tar_fec_embz)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_embz ,'yyyyMMdd'), 'yyyy-MM-dd') end as dt_cue_embz
		,CASE WHEN trim(maestarje.tar_fec_pin)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_pin ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_pin
		,CASE WHEN LENGTH(trim(maestarje.tar_usuario_pin))=0 THEN NULL ELSE trim(maestarje.tar_usuario_pin) end  as cod_cue_legajo_pin
		,CASE WHEN trim(maestarje.tar_fec_cbio_suc)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_cbio_suc ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_cambio_sucursal
		,trim(maestarje.tar_can_cuentas) as fc_cue_cantidad_cuentas
		,CASE WHEN trim(maestarje.tar_marca_tar)='*' THEN NULL ELSE trim(maestarje.tar_marca_tar) end as cod_cue_marca_tar
		,CASE WHEN LENGTH(trim(maestarje.tar_nro_tar_ant))=0 THEN NULL ELSE trim(maestarje.tar_nro_tar_ant) end  as cod_cue_tarjeta_anterior
		,CASE WHEN LENGTH(trim(maestarje.tar_cod_reem))=0 THEN NULL ELSE trim(maestarje.tar_cod_reem) end  as cod_cue_reem
		,CASE WHEN LENGTH(trim(maestarje.tar_cod_dist))=0 THEN NULL ELSE trim(maestarje.tar_cod_dist) end  as cod_cue_dist
		,trim(maestarje.tar_tipo_plas) as cod_cue_tipo_plastico
		,CASE WHEN LENGTH(trim(maestarje.tar_tipo_plas_origi))=0 THEN NULL ELSE trim(maestarje.tar_tipo_plas_origi) end  as cod_cue_tipo_plastico_original
		,trim(maestarje.tar_cod_comercial) as cod_cue_comercial
		,CASE WHEN LENGTH(trim(maestarje.tar_cod_emision))=0 THEN NULL ELSE trim(maestarje.tar_cod_emision) end  as cod_cue_emision
		,CASE WHEN LENGTH(trim(maestarje.tar_nombre_foto))=0 THEN NULL ELSE trim(maestarje.tar_nombre_foto) end  as cod_cue_nombre_foto
		,CASE WHEN LENGTH(trim(maestarje.tar_trackii))=0 THEN NULL ELSE trim(maestarje.tar_trackii) end  as cod_cue_trackii
		,CASE WHEN LENGTH(trim(maestarje.tar_cuarta_linea))=0 OR trim(maestarje.tar_cuarta_linea)='*' THEN NULL ELSE trim(maestarje.tar_cuarta_linea) end  as cod_cue_cuarta_linea
		,trim(maestarje.tar_mot_emision) as cod_cue_motivo_emision
		,CASE WHEN LENGTH(trim(maestarje.tar_retorno_banelco))=0 THEN NULL ELSE trim(maestarje.tar_retorno_banelco) end  as cod_cue_retorno_banelco
		,CASE WHEN LENGTH(trim(maestarje.tar_nup))=0 THEN NULL ELSE trim(maestarje.tar_nup) end  as cod_per_nup_tarjeta
		,pedt008_temp.penumper as cod_per_nup_titular
		,CASE WHEN LENGTH(trim(maestarje.tar_tracki_disc1))=0 THEN NULL ELSE trim(maestarje.tar_tracki_disc1) end  as cod_cue_tar_tracki_disc1
		,CASE WHEN LENGTH(trim(maestarje.tar_tracki_disc2))=0 THEN NULL ELSE trim(maestarje.tar_tracki_disc2) end  as cod_cue_tar_tracki_disc2
		,CASE WHEN LENGTH(trim(maestarje.tar_tracki_disc3))=0 THEN NULL ELSE trim(maestarje.tar_tracki_disc3) end  as cod_cue_tar_tracki_disc3
		,CASE WHEN LENGTH(trim(maestarje.tar_fec_utiles))=0 THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_utiles ,'yyyyMMdd'), 'yyyy-MM-dd') end  as dt_cue_utiles
		,CASE WHEN LENGTH(trim(maestarje.tar_cod_utiles))=0 THEN NULL ELSE trim(maestarje.tar_cod_utiles) end  as cod_cue_utiles
		,CASE WHEN LENGTH(trim(maestarje.tar_fec_estado_renov))=0 THEN NULL ELSE from_unixtime(unix_timestamp(maestarje.tar_fec_estado_renov ,'yyyyMMdd'), 'yyyy-MM-dd') end  as dt_cue_estado_renov
		,CASE WHEN LENGTH(trim(maestarje.tar_tarjeta_coord))=0 THEN NULL ELSE trim(maestarje.tar_tarjeta_coord) end  as cod_cue_tarjeta_coordenadas
		,CASE WHEN trim(maestarje.tar_inhabil_cbio)='S' THEN 1 ELSE 0 end  as flag_cue_inhabil_cbio
		,CASE WHEN LENGTH(trim(maestarje.tar_cod_destino))=0 THEN NULL ELSE trim(maestarje.tar_cod_destino) end  as cod_cue_destino
		,CASE WHEN LENGTH(trim(maestarje.tar_adhesion_otp))=0 THEN NULL ELSE trim(maestarje.tar_adhesion_otp) end  as cod_cue_adhesion_otp
		,CASE WHEN LENGTH(trim(maestarje.tar_canal))=0 THEN NULL ELSE trim(maestarje.tar_canal) end  as cod_cue_canal
		,CASE WHEN LENGTH(trim(maestarje.tar_permisionaria))=0 THEN NULL ELSE trim(maestarje.tar_permisionaria) end  as cod_cue_permisionaria
		,CASE WHEN LENGTH(trim(maestarje.tar_chip))=0 THEN NULL ELSE trim(maestarje.tar_chip) end  as cod_cue_chip
		,trim(maestarje.tar_secuencial) as cod_cue_secuencial
		,CASE WHEN maestarje.tar_nup=pedt008_temp.penumper THEN 1 ELSE 0 END AS flag_cue_titular
    ,'ABAE' as cod_cue_origen
		,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}' as partition_date
FROM  bi_corp_staging.abae_maestarj maestarje
left join bi_corp_staging.abae_maestarj_aux_bajas b
on maestarje.tar_numero_tarjeta=b.tar_numero_tarjeta
and b.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_abae_maestarj_aux_bajas', dag_id='LOAD_CMN_Cuentas_Abae') }}'
LEFT JOIN pedt008_temp pedt008_temp
on  CAST(maestarje.tar_centro_ppal AS int) = CAST(trim(pedt008_temp.pecodofi) AS int)
and cast(trim(maestarje.tar_cuenta_ppal_nro) AS int) = cast(trim(pedt008_temp.penumcon) as int)
and cast(trim(maestarje.tar_cuenta_ppal_prod) as int) = cast(trim(pedt008_temp.pecodpro) as int)
WHERE maestarje.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_abae_maestarj', dag_id='LOAD_CMN_Cuentas_Abae') }}'
and b.tar_numero_tarjeta is null

union all

SELECT

		trim(tar_centro_ppal) as cod_suc_sucursal
		,trim(tar_cuenta_ppal_nro) as cod_cue_cuenta
		,substring(trim(tar_cuenta_ppal_prod),2,2) as cod_cue_producto
		,CASE WHEN LENGTH(trim(tar_ppal_subprod))=0 THEN NULL ELSE trim(tar_ppal_subprod) end  as cod_cue_subproducto
		,trim(tar_divisa_ppal) as cod_cue_divisa
		,trim(tar_firmante_ppal) as cod_cue_firmante
		,CAST(trim(tar_numero_tarjeta) AS bigint) as cod_cue_tarjeta
		,trim(tar_sucursal_adm) as cod_suc_sucursal_adm
		,trim(tar_entidad_ppal) as cod_cue_entidad
		,trim(tar_tipo_cuenta_banelco) as cod_cue_tipo_cuenta_banelco
		,trim(tar_tipo_tarjeta) as cod_cue_tipo_tarjeta
		,trim(tar_estado_tarjeta) as cod_cue_tarjeta_estado
		,trim(tar_fec_baja) as dt_cue_baja
		,trim(tar_cod_lim) as cod_cue_lim
		,from_unixtime(unix_timestamp(tar_fec_vto ,'yyyyMM'), 'yyyy-MM') as ds_cue_mes_vencimiento
		,from_unixtime(unix_timestamp(tar_fec_alta ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_alta
		,CASE WHEN LENGTH(trim(tar_usuario_alta))=0 THEN NULL ELSE trim(tar_usuario_alta) end  as cod_cue_legajo_alta
		,from_unixtime(unix_timestamp(tar_fec_habilitacion ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_habilitacion
		,from_unixtime(unix_timestamp(tar_fec_ult_act ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_ult_act
		,CASE WHEN LENGTH(trim(tar_usuario_act))=0 THEN NULL ELSE trim(tar_usuario_act) end  as cod_cue_legajo_act
		,CASE WHEN trim(tar_fec_envio_banelco)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_envio_banelco ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_envio_banelco
		,from_unixtime(unix_timestamp(tar_fec_act_banelco ,'yyyyMMdd'), 'yyyy-MM-dd') as dt_cue_act_banelco
		,CASE WHEN LENGTH(trim(tar_estado_renov))=0 THEN NULL ELSE trim(tar_estado_renov) end  as cod_cue_estado_renov
		,CASE WHEN LENGTH(trim(tar_marca_pin))=0 THEN NULL ELSE trim(tar_marca_pin) end  as cod_cue_marca_pin
		,CASE WHEN trim(tar_fec_embz)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_embz ,'yyyyMMdd'), 'yyyy-MM-dd') end as dt_cue_embz
		,CASE WHEN trim(tar_fec_pin)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_pin ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_pin
		,CASE WHEN LENGTH(trim(tar_usuario_pin))=0 THEN NULL ELSE trim(tar_usuario_pin) end  as cod_cue_legajo_pin
		,CASE WHEN trim(tar_fec_cbio_suc)='00000000' THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_cbio_suc ,'yyyyMMdd'), 'yyyy-MM-dd')  end as dt_cue_cambio_sucursal
		,trim(tar_can_cuentas) as fc_cue_cantidad_cuentas
		,CASE WHEN trim(tar_marca_tar)='*' THEN NULL ELSE trim(tar_marca_tar) end as cod_cue_marca_tar
		,CASE WHEN LENGTH(trim(tar_nro_tar_ant))=0 THEN NULL ELSE trim(tar_nro_tar_ant) end  as cod_cue_tarjeta_anterior
		,CASE WHEN LENGTH(trim(tar_cod_reem))=0 THEN NULL ELSE trim(tar_cod_reem) end  as cod_cue_reem
		,CASE WHEN LENGTH(trim(tar_cod_dist))=0 THEN NULL ELSE trim(tar_cod_dist) end  as cod_cue_dist
		,trim(tar_tipo_plas) as cod_cue_tipo_plastico
		,CASE WHEN LENGTH(trim(tar_tipo_plas_origi))=0 THEN NULL ELSE trim(tar_tipo_plas_origi) end  as cod_cue_tipo_plastico_original
		,trim(tar_cod_comercial) as cod_cue_comercial
		,CASE WHEN LENGTH(trim(tar_cod_emision))=0 THEN NULL ELSE trim(tar_cod_emision) end  as cod_cue_emision
		,CASE WHEN LENGTH(trim(tar_nombre_foto))=0 THEN NULL ELSE trim(tar_nombre_foto) end  as cod_cue_nombre_foto
		,CASE WHEN LENGTH(trim(tar_trackii))=0 THEN NULL ELSE trim(tar_trackii) end  as cod_cue_trackii
		,CASE WHEN LENGTH(trim(tar_cuarta_linea))=0 OR trim(tar_cuarta_linea)='*' THEN NULL ELSE trim(tar_cuarta_linea) end  as cod_cue_cuarta_linea
		,trim(tar_mot_emision) as cod_cue_motivo_emision
		,CASE WHEN LENGTH(trim(tar_retorno_banelco))=0 THEN NULL ELSE trim(tar_retorno_banelco) end  as cod_cue_retorno_banelco
		,CASE WHEN LENGTH(trim(tar_nup))=0 THEN NULL ELSE trim(tar_nup) end  as cod_per_nup_tarjeta
		,pedt008_temp.penumper as cod_per_nup_titular
		,CASE WHEN LENGTH(trim(tar_tracki_disc1))=0 THEN NULL ELSE trim(tar_tracki_disc1) end  as cod_cue_tar_tracki_disc1
		,CASE WHEN LENGTH(trim(tar_tracki_disc2))=0 THEN NULL ELSE trim(tar_tracki_disc2) end  as cod_cue_tar_tracki_disc2
		,CASE WHEN LENGTH(trim(tar_tracki_disc3))=0 THEN NULL ELSE trim(tar_tracki_disc3) end  as cod_cue_tar_tracki_disc3
		,CASE WHEN LENGTH(trim(tar_fec_utiles))=0 THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_utiles ,'yyyyMMdd'), 'yyyy-MM-dd') end  as dt_cue_utiles
		,CASE WHEN LENGTH(trim(tar_cod_utiles))=0 THEN NULL ELSE trim(tar_cod_utiles) end  as cod_cue_utiles
		,CASE WHEN LENGTH(trim(tar_fec_estado_renov))=0 THEN NULL ELSE from_unixtime(unix_timestamp(tar_fec_estado_renov ,'yyyyMMdd'), 'yyyy-MM-dd') end  as dt_cue_estado_renov
		,CASE WHEN LENGTH(trim(tar_tarjeta_coord))=0 THEN NULL ELSE trim(tar_tarjeta_coord) end  as cod_cue_tarjeta_coordenadas
		,CASE WHEN trim(tar_inhabil_cbio)='S' THEN 1 ELSE 0 end  as flag_cue_inhabil_cbio
		,CASE WHEN LENGTH(trim(tar_cod_destino))=0 THEN NULL ELSE trim(tar_cod_destino) end  as cod_cue_destino
		,CASE WHEN LENGTH(trim(tar_adhesion_otp))=0 THEN NULL ELSE trim(tar_adhesion_otp) end  as cod_cue_adhesion_otp
		,CASE WHEN LENGTH(trim(tar_canal))=0 THEN NULL ELSE trim(tar_canal) end  as cod_cue_canal
		,CASE WHEN LENGTH(trim(tar_permisionaria))=0 THEN NULL ELSE trim(tar_permisionaria) end  as cod_cue_permisionaria
		,CASE WHEN LENGTH(trim(tar_chip))=0 THEN NULL ELSE trim(tar_chip) end  as cod_cue_chip
		,trim(tar_secuencial) as cod_cue_secuencial
		,CASE WHEN b.tar_nup=pedt008_temp.penumper THEN 1 ELSE 0 END AS flag_cue_titular
    ,'ABAE' as cod_cue_origen
		,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}' as partition_date

FROM  bi_corp_staging.abae_maestarj_aux_bajas b
LEFT JOIN pedt008_temp pedt008_temp
on  CAST(b.tar_centro_ppal AS int) = CAST(trim(pedt008_temp.pecodofi) AS int)
and cast(trim(b.tar_cuenta_ppal_nro) AS int) = cast(trim(pedt008_temp.penumcon) as int)
and cast(trim(b.tar_cuenta_ppal_prod) as int) = cast(trim(pedt008_temp.pecodpro) as int)
WHERE b.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_abae_maestarj_aux_bajas', dag_id='LOAD_CMN_Cuentas_Abae') }}'
; -- Cargo Las novedades

DROP TABLE IF EXISTS stk_cue_tarjetas_debito_ant;
create temporary table stk_cue_tarjetas_debito_ant as

select
  ant.cod_suc_sucursal ,
  ant.cod_cue_cuenta ,
  ant.cod_cue_producto ,
  ant.cod_cue_subproducto ,
  ant.cod_cue_divisa ,
  ant.cod_cue_firmante ,
  ant.cod_cue_tarjeta ,
  ant.cod_suc_sucursal_adm ,
  ant.cod_cue_entidad ,
  ant.cod_cue_tipo_cuenta_banelco ,
  ant.cod_cue_tipo_tarjeta ,
  ant.cod_cue_tarjeta_estado ,
  ant.dt_cue_baja ,
  ant.cod_cue_lim ,
  ant.ds_cue_mes_vencimiento ,
  ant.dt_cue_alta ,
  ant.cod_cue_legajo_alta ,
  ant.dt_cue_habilitacion ,
  ant.dt_cue_ult_act ,
  ant.cod_cue_legajo_act ,
  ant.dt_cue_envio_banelco ,
  ant.dt_cue_act_banelco ,
  ant.cod_cue_estado_renov ,
  ant.cod_cue_marca_pin ,
  ant.dt_cue_embz ,
  ant.dt_cue_pin ,
  ant.cod_cue_legajo_pin ,
  ant.dt_cue_cambio_sucursal ,
  ant.fc_cue_cantidad_cuentas ,
  ant.cod_cue_marca_tar ,
  ant.cod_cue_tarjeta_anterior ,
  ant.cod_cue_reem ,
  ant.cod_cue_dist ,
  ant.cod_cue_tipo_plastico ,
  ant.cod_cue_tipo_plastico_original ,
  ant.cod_cue_comercial ,
  ant.cod_cue_emision ,
  ant.cod_cue_nombre_foto ,
  ant.cod_cue_trackii ,
  ant.cod_cue_cuarta_linea ,
  ant.cod_cue_motivo_emision ,
  ant.cod_cue_retorno_banelco ,
  ant.cod_per_nup_tarjeta ,
  ant.cod_per_nup_titular ,
  ant.cod_cue_tar_tracki_disc1 ,
  ant.cod_cue_tar_tracki_disc2 ,
  ant.cod_cue_tar_tracki_disc3 ,
  ant.dt_cue_utiles ,
  ant.cod_cue_utiles ,
  ant.dt_cue_estado_renov ,
  ant.cod_cue_tarjeta_coordenadas ,
  ant.flag_cue_inhabil_cbio ,
  ant.cod_cue_destino ,
  ant.cod_cue_adhesion_otp ,
  ant.cod_cue_canal ,
  ant.cod_cue_permisionaria,
  ant.cod_cue_chip ,
  ant.cod_cue_secuencial ,
  ant.flag_cue_titular ,
  ant.cod_cue_origen ,
  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}' as partition_date
from bi_corp_common.stk_cue_tarjetas_debito ant
left join stk_cue_tarjetas_debito_actual actual
on ant.cod_cue_tarjeta=actual.cod_cue_tarjeta
where ant.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_working_day', dag_id='LOAD_CMN_Cuentas_Abae') }}' and actual.cod_cue_tarjeta is null;
-- cargo el stock anterior

-- Cargo el stock
insert overwrite table bi_corp_common.stk_cue_tarjetas_debito
partition(partition_date)
select
  cod_suc_sucursal ,
  cod_cue_cuenta ,
  cod_cue_producto ,
  cod_cue_subproducto ,
  cod_cue_divisa ,
  cod_cue_firmante ,
  cod_cue_tarjeta ,
  cod_suc_sucursal_adm ,
  cod_cue_entidad ,
  cod_cue_tipo_cuenta_banelco ,
  cod_cue_tipo_tarjeta ,
  cod_cue_tarjeta_estado ,
  dt_cue_baja ,
  cod_cue_lim ,
  ds_cue_mes_vencimiento ,
  dt_cue_alta ,
  cod_cue_legajo_alta ,
  dt_cue_habilitacion ,
  dt_cue_ult_act ,
  cod_cue_legajo_act ,
  dt_cue_envio_banelco ,
  dt_cue_act_banelco ,
  cod_cue_estado_renov ,
  cod_cue_marca_pin ,
  dt_cue_embz ,
  dt_cue_pin ,
  cod_cue_legajo_pin ,
  dt_cue_cambio_sucursal ,
  fc_cue_cantidad_cuentas ,
  cod_cue_marca_tar ,
  cod_cue_tarjeta_anterior ,
  cod_cue_reem ,
  cod_cue_dist ,
  cod_cue_tipo_plastico ,
  cod_cue_tipo_plastico_original ,
  cod_cue_comercial ,
  cod_cue_emision ,
  cod_cue_nombre_foto ,
  cod_cue_trackii ,
  cod_cue_cuarta_linea ,
  cod_cue_motivo_emision ,
  cod_cue_retorno_banelco ,
  cod_per_nup_tarjeta ,
  cod_per_nup_titular ,
  cod_cue_tar_tracki_disc1 ,
  cod_cue_tar_tracki_disc2 ,
  cod_cue_tar_tracki_disc3 ,
  dt_cue_utiles ,
  cod_cue_utiles ,
  dt_cue_estado_renov ,
  cod_cue_tarjeta_coordenadas ,
  flag_cue_inhabil_cbio ,
  cod_cue_destino ,
  cod_cue_adhesion_otp ,
  cod_cue_canal ,
  cod_cue_permisionaria STING,
  cod_cue_chip ,
  cod_cue_secuencial ,
  flag_cue_titular ,
  cod_cue_origen,
  partition_date
FROM stk_cue_tarjetas_debito_actual

union all

select
  cod_suc_sucursal ,
  cod_cue_cuenta ,
  cod_cue_producto ,
  cod_cue_subproducto ,
  cod_cue_divisa ,
  cod_cue_firmante ,
  cod_cue_tarjeta ,
  cod_suc_sucursal_adm ,
  cod_cue_entidad ,
  cod_cue_tipo_cuenta_banelco ,
  cod_cue_tipo_tarjeta ,
  cod_cue_tarjeta_estado ,
  dt_cue_baja ,
  cod_cue_lim ,
  ds_cue_mes_vencimiento ,
  dt_cue_alta ,
  cod_cue_legajo_alta ,
  dt_cue_habilitacion ,
  dt_cue_ult_act ,
  cod_cue_legajo_act ,
  dt_cue_envio_banelco ,
  dt_cue_act_banelco ,
  cod_cue_estado_renov ,
  cod_cue_marca_pin ,
  dt_cue_embz ,
  dt_cue_pin ,
  cod_cue_legajo_pin ,
  dt_cue_cambio_sucursal ,
  fc_cue_cantidad_cuentas ,
  cod_cue_marca_tar ,
  cod_cue_tarjeta_anterior ,
  cod_cue_reem ,
  cod_cue_dist ,
  cod_cue_tipo_plastico ,
  cod_cue_tipo_plastico_original ,
  cod_cue_comercial ,
  cod_cue_emision ,
  cod_cue_nombre_foto ,
  cod_cue_trackii ,
  cod_cue_cuarta_linea ,
  cod_cue_motivo_emision ,
  cod_cue_retorno_banelco ,
  cod_per_nup_tarjeta ,
  cod_per_nup_titular ,
  cod_cue_tar_tracki_disc1 ,
  cod_cue_tar_tracki_disc2 ,
  cod_cue_tar_tracki_disc3 ,
  dt_cue_utiles ,
  cod_cue_utiles ,
  dt_cue_estado_renov ,
  cod_cue_tarjeta_coordenadas ,
  flag_cue_inhabil_cbio ,
  cod_cue_destino ,
  cod_cue_adhesion_otp ,
  cod_cue_canal ,
  cod_cue_permisionaria STING,
  cod_cue_chip ,
  cod_cue_secuencial ,
  flag_cue_titular ,
  cod_cue_origen,
  partition_date
FROM stk_cue_tarjetas_debito_ant;
