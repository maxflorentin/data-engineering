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


CREATE TEMPORARY TABLE maestarje  AS
Select
t.tar_numero_tarjeta,
t.tar_sucursal_adm,
t.tar_entidad_ppal,
t.tar_centro_ppal,
t.tar_cuenta_ppal_prod,
t.tar_cuenta_ppal_nro,
t.tar_divisa_ppal,
t.tar_firmante_ppal,
t.tar_tipo_cuenta_banelco,
t.tar_tipo_tarjeta,
t.tar_estado_tarjeta,
t.tar_cod_lim,
t.tar_fec_vto,
t.tar_fec_alta,
t.tar_usuario_alta,
t.tar_fec_habilitacion,
t.tar_fec_ult_act,
t.tar_usuario_act,
t.tar_fec_envio_banelco,
t.tar_fec_act_banelco,
t.tar_estado_renov,
t.tar_marca_pin,
t.tar_fec_embz,
t.tar_fec_pin,
t.tar_usuario_pin,
t.tar_fec_cbio_suc,
t.tar_can_cuentas,
t.tar_marca_tar,
t.tar_nro_tar_ant,
t.tar_cod_reem,
t.tar_cod_dist,
t.tar_tipo_plas,
t.tar_tipo_plas_origi,
t.tar_cod_comercial,
t.tar_cod_emision,
t.tar_nombre_foto,
t.tar_trackii,
t.tar_cuarta_linea,
t.tar_mot_emision,
t.tar_retorno_banelco,
t.tar_nup,
t.tar_ppal_subprod,
t.tar_tracki_disc1,
t.tar_tracki_disc2,
t.tar_tracki_disc3,
t.tar_fec_utiles,
t.tar_cod_utiles,
t.tar_fec_estado_renov,
t.tar_tarjeta_coord,
t.tar_inhabil_cbio,
t.tar_cod_destino,
t.tar_adhesion_otp,
t.tar_canal,
t.tar_permisionaria,
t.tar_chip,
t.tar_secuencial,
cast(null as string) as tar_fec_baja

FROM  bi_corp_staging.abae_maestarj t
left join bi_corp_staging.abae_maestarj_aux_bajas b
on t.tar_numero_tarjeta=b.tar_numero_tarjeta
and b.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'
WHERE t.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_abae_maestarj', dag_id='LOAD_CMN_Cuentas_Abae') }}'
and b.tar_numero_tarjeta is null

Union all

Select
n.tar_numero_tarjeta,
n.tar_sucursal_adm,
n.tar_entidad_ppal,
n.tar_centro_ppal,
n.tar_cuenta_ppal_prod,
n.tar_cuenta_ppal_nro,
n.tar_divisa_ppal,
n.tar_firmante_ppal,
n.tar_tipo_cuenta_banelco,
n.tar_tipo_tarjeta,
n.tar_estado_tarjeta,
n.tar_cod_lim,
n.tar_fec_vto,
n.tar_fec_alta,
n.tar_usuario_alta,
n.tar_fec_habilitacion,
n.tar_fec_ult_act,
n.tar_usuario_act,
n.tar_fec_envio_banelco,
n.tar_fec_act_banelco,
n.tar_estado_renov,
n.tar_marca_pin,
n.tar_fec_embz,
n.tar_fec_pin,
n.tar_usuario_pin,
n.tar_fec_cbio_suc,
n.tar_can_cuentas,
n.tar_marca_tar,
n.tar_nro_tar_ant,
n.tar_cod_reem,
n.tar_cod_dist,
n.tar_tipo_plas,
n.tar_tipo_plas_origi,
n.tar_cod_comercial,
n.tar_cod_emision,
n.tar_nombre_foto,
n.tar_trackii,
n.tar_cuarta_linea,
n.tar_mot_emision,
n.tar_retorno_banelco,
n.tar_nup,
n.tar_ppal_subprod,
n.tar_tracki_disc1,
n.tar_tracki_disc2,
n.tar_tracki_disc3,
n.tar_fec_utiles,
n.tar_cod_utiles,
n.tar_fec_estado_renov,
n.tar_tarjeta_coord,
n.tar_inhabil_cbio,
n.tar_cod_destino,
n.tar_adhesion_otp,
n.tar_canal,
n.tar_permisionaria,
n.tar_chip,
n.tar_secuencial,
n.tar_fec_baja
from bi_corp_staging.abae_maestarj_aux_bajas n
Where n.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}'

Union all

Select
h.nro_tarjeta 	as tar_numero_tarjeta,
h.sucursal 		as tar_sucursal_adm,
cast(null as string)  	        as tar_entidad_ppal,
h.centro_ppal 	as tar_centro_ppal,
cast(null as string)            as tar_cuenta_ppal_prod,
h.cuenta_ppal 	as tar_cuenta_ppal_nro,
h.divisa_ppal 	as tar_divisa_ppal,
h.firmante_ppal as tar_firmante_ppal,
cast(null as string)            as tar_tipo_cuenta_banelco,
cast(null as string)            as tar_tipo_tarjeta,
h.estado        as tar_estado_tarjeta,
cast(null as string)            as tar_cod_lim,
h.fevto         as tar_fec_vto,
h.fealta        as tar_fec_alta,
cast(null as string)            as tar_usuario_alta,
cast(null as string)            as tar_fec_habilitacion,
cast(null as string)            as tar_fec_ult_act,
cast(null as string)            as tar_usuario_act,
cast(null as string)            as tar_fec_envio_banelco,
cast(null as string)            as tar_fec_act_banelco,
cast(null as string)            as tar_estado_renov,
cast(null as string)            as tar_marca_pin,
h.fecha_emboza  as tar_fec_embz,
cast(null as string)            as tar_fec_pin,
cast(null as string)            as tar_usuario_pin,
cast(null as string)            as tar_fec_cbio_suc,
cast(null as string)            as tar_can_cuentas,
cast(null as string)            as tar_marca_tar,
h.nro_tar_ant   as tar_nro_tar_ant,
cast(null as string)            as tar_cod_reem,
cast(null as string)            as tar_cod_dist,
cast(null as string)            as tar_tipo_plas,
cast(null as string)            as tar_tipo_plas_origi,
h.cod_comercial as tar_cod_comercial,
cast(null as string)            as tar_cod_emision,
cast(null as string)            as tar_nombre_foto,
cast(null as string)            as tar_trackii,
cast(null as string)            as tar_cuarta_linea,
cast(null as string)            as tar_mot_emision,
cast(null as string)            as tar_retorno_banelco,
h.penumper      as tar_nup,
cast(null as string)            as tar_ppal_subprod,
cast(null as string)            as tar_tracki_disc1,
cast(null as string)            as tar_tracki_disc2,
cast(null as string)            as tar_tracki_disc3,
cast(null as string)            as tar_fec_utiles,
cast(null as string)            as tar_cod_utiles,
cast(null as string)            as tar_fec_estado_renov,
cast(null as string)            as tar_tarjeta_coord,
cast(null as string)            as tar_inhabil_cbio,
cast(null as string)            as tar_cod_destino,
h.marca_otp     as tar_adhesion_otp,
cast(null as string)            as tar_canal,
cast(null as string)            as tar_permisionaria,
h.marca_tarjeta_con_chip            as tar_chip,
cast(null as string)            as tar_secuencial,
to_date(h.febaja)  as tar_fec_baja

from bi_corp_staging.tmae_blco_hist h
Where h.partition_date = '2020-05-02'
;

insert overwrite table bi_corp_common.stk_cue_tarjetas_debito
partition(partition_date)
SELECT

		trim(tar_centro_ppal) as cod_suc_sucursal
		,trim(tar_cuenta_ppal_nro) as cod_cue_cuenta
		,substring(trim(tar_cuenta_ppal_prod),2,2) as cod_cue_producto
		,CASE WHEN LENGTH(trim(tar_ppal_subprod))=0 THEN NULL ELSE trim(tar_ppal_subprod) end  as cod_cue_subproducto
		,trim(tar_divisa_ppal) as cod_cue_divisa
		,trim(tar_firmante_ppal) as cod_cue_firmante
		,trim(tar_numero_tarjeta) as cod_cue_tarjeta
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
		,CASE WHEN maestarje.tar_nup=pedt008_temp.penumper THEN 1 ELSE 0 END AS flag_cue_titular
		,'{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cuentas_Abae') }}' as partition_date

FROM   maestarje
LEFT JOIN pedt008_temp pedt008_temp
on  CAST(maestarje.tar_centro_ppal AS int) = CAST(trim(pedt008_temp.pecodofi) AS int)
and cast(trim(maestarje.tar_cuenta_ppal_nro) AS int) = cast(trim(pedt008_temp.penumcon) as int)
and cast(trim(maestarje.tar_cuenta_ppal_prod) as int) = cast(trim(pedt008_temp.pecodpro) as int);
