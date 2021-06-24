set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;



insert overwrite table bi_corp_common.bt_ate_cuentas_digitales
partition(partition_date)
SELECT
     case when trim(soli.id)='null' then null else trim(soli.id) end as cod_ate_solicitud
    ,case when trim(soli.nup)='null' then null else trim(soli.nup) end as cod_per_nup
    ,case when trim(soli.id_sucursal)='null' then null else trim(soli.id_sucursal) end as cod_suc_sucursal
    ,case when trim(soli.id_prod)='null' then null else trim(soli.id_prod) end as cod_ate_producto
    ,case when trim(prod.DESCRIPCION)='null' then null else trim(prod.DESCRIPCION) end as ds_ate_producto
    ,case when trim(est_soli.DESCRIPCION_AMIGABLE)='null' then null else trim(est_soli.DESCRIPCION_AMIGABLE) end as ds_ate_estado_solicitud
    ,case when trim(soli.dispositivo)='null' then null else trim(soli.dispositivo) end as ds_ate_dispositivo
    ,case when trim(soli.fecha_creacion)='null' then null else trim(soli.fecha_creacion) end as dt_ate_creacion
    ,case when trim(soli.fecha_alta)='null' then null else trim(soli.fecha_alta) end as dt_ate_alta
    ,case when trim(soli.nro_asol)='null' then null else trim(soli.nro_asol) end as cod_ate_asol
    ,case when trim(soli.nro_doc)='null' then null else trim(soli.nro_doc) end as ds_ate_numdoc
    ,case when trim(soli.estado_asol)='null' then null else trim(soli.estado_asol) end as cod_ate_estado_asol
    ,case when trim(soli.nro_cuenta)='null' then null else trim(soli.nro_cuenta) end as cod_cue_cuenta
    ,case when trim(soli.canal)='null' then null else trim(soli.canal) end as cod_ate_canal
    ,case when trim(soli.id_biometria)='null' then null else trim(soli.id_biometria) end as cod_ate_biometria
    ,case when trim(soli.appid)='null' then null else trim(soli.appid) end as cod_ate_app
    ,case when trim(soli.sucursal_cr)='null' then null else trim(soli.sucursal_cr) end as cod_suc_sucursal_cr
    ,case when trim(soli.cajero)='null' then null else trim(soli.cajero) end as cod_ate_cajero
    ,case when trim(prosp.NOMBRE)='null' then null else trim(prosp.NOMBRE) end as ds_ate_nombre
    ,case when trim(prosp.APELLIDO)='null' then null else trim(prosp.APELLIDO) end as ds_ate_apellido
    ,case when trim(prosp.ID_TRAMITE_RENAPER)='null' then null else trim(prosp.ID_TRAMITE_RENAPER) end as cod_ate_tramite_renaper
    ,case when trim(prosp.ID_NACIONALIDAD)='null' then null else trim(prosp.ID_NACIONALIDAD) end as cod_ate_nacionalidad
    ,case when trim(prosp.ID_PAIS_ORIGEN)='null' then null else trim(prosp.ID_PAIS_ORIGEN) end as cod_ate_pais_origen
    ,case when trim(prosp.FECHA_NACIMIENTO)='null' then null else to_date(trim(prosp.FECHA_NACIMIENTO)) end as dt_ate_fecha_nacimiento
    ,case when trim(prosp.SEXO)='null' then null else trim(prosp.SEXO) end as cod_ate_sexo
    ,case when trim(prosp.ESTADO_CIVIL)='null' then null else trim(prosp.ESTADO_CIVIL) end as cod_ate_estado_civil
    ,case when trim(prosp.MAIL)='null' then null else trim(prosp.MAIL) end as ds_ate_email
    ,case when trim(prosp.CELULAR)='null' then null else trim(prosp.CELULAR) end as ds_ate_celular
    ,case when trim(prosp.COMPANIA_CELULAR)='null' then null else trim(prosp.COMPANIA_CELULAR) end as ds_ate_compania_celular
    ,case when trim(prosp.CUIL)='null' then null else trim(prosp.CUIL) end as ds_ate_cuil
    ,case when trim(prosp.TIPO_LABORAL)='null' then null else trim(prosp.TIPO_LABORAL) end as cod_ate_tipo_laboral
    ,case when trim(prosp.INGRESOS)='null' then null else trim(prosp.INGRESOS) end as fc_ate_ingresos
    ,case when trim(prosp.INDICO_ESUNIV)='S' then 1 else 0 end as flag_ate_universal
    ,case when trim(prosp.ACEPTO_TYC)='S' then 1 else 0 end as flag_ate_acepto_tyc
    ,case when trim(prosp.GENERO_CLAVE)='S' then 1 else 0 end as flag_ate_genero_clave
    ,case when trim(prosp.FECHA_INICIO_ACTIVIDAD)='null' then null else trim(prosp.FECHA_INICIO_ACTIVIDAD) end as dt_ate_inicio_actividad
    ,case when trim(prosp.COD_TIPO_RESIDENCIA)='null' then null else trim(prosp.COD_TIPO_RESIDENCIA) end as cod_ate_tipo_residencia
    ,case when trim(prosp.EXPIRACION_DOC)='null' then null else trim(prosp.EXPIRACION_DOC) end as dt_ate_expiracion_doc
    ,case when trim(prosp.EJEMPLAR)='null' then null else trim(prosp.EJEMPLAR) end as cod_ate_ejemplar
    ,case when trim(prosp.ID_ACTIVIDAD)='null' then null else trim(prosp.ID_ACTIVIDAD) end as cod_ate_actividad
    ,case when trim(prosp.SIMILITUD)='null' then null else trim(prosp.SIMILITUD) end as cod_ate_similitud
    ,case when trim(prosp.SANIDAD)='null' then null else trim(prosp.SANIDAD) end as flag_ate_sanidad
    ,case when trim(prosp.FECHA_EMISION)='null' then null else trim(prosp.FECHA_EMISION) end as dt_ate_emision
    ,case when trim(prosp.VCTO_RESIDENCIA)='null' then null else trim(prosp.VCTO_RESIDENCIA) end as dt_ate_vto_residencia
    ,case when trim(prosp.CLIENTE_CON_CUENTA)='S' then 1 else 0 end flag_ate_cliente_con_cuenta
    ,case when trim(domi.LOCALIDAD)='null' then null else trim(domi.LOCALIDAD) end as ds_ate_localidad
    ,case when trim(domi.ID_PROV)='null' then null else trim(domi.ID_PROV) end as ds_ate_provincia
    ,case when trim(domi.CP)='null' then null else trim(domi.CP) end as ds_ate_cod_postal
    ,case when trim(camp.source)='null' then null else trim(camp.source) end as ds_ate_origen
    ,case when trim(camp.medium)='null' then null else trim(camp.medium) end as ds_ate_medium
    ,case when trim(camp.campaign)='null' then null else trim(camp.campaign) end as ds_ate_campaign
    ,case when trim(tel.PREFIJO)='null' then null else trim(tel.PREFIJO) end as ds_ate_cod_area
    ,case when trim(tel.TELEFONO)='null' then null else trim(tel.TELEFONO) end as ds_ate_nro_telefono
    ,soli.partition_date as partition_date
from bi_corp_staging.rio256_crsc_solicitud soli
left join bi_corp_staging.rio256_crsc_producto prod
on soli.id_prod=prod.id
and soli.partition_date=prod.partition_date
left join bi_corp_staging.rio256_crsc_estado_solicitud est_soli
on soli.id_estado_solicitud=est_soli.id
and soli.partition_date=est_soli.partition_date
left join bi_corp_staging.rio256_crsc_prospecto prosp
on soli.id_prospecto= prosp.id
and soli.partition_date=prosp.partition_date
left join bi_corp_staging.rio256_crsc_domicilio domi
on prosp.id_domicilio=domi.id
and soli.partition_date=domi.partition_date
left join bi_corp_staging.rio256_crsc_telefono_solicitud tel
on soli.id=tel.id_solicitud
and soli.partition_date=tel.partition_date
left join bi_corp_staging.rio256_crsc_campania camp
on soli.id=camp.id_solicitud
and soli.partition_date=camp.partition_date
where soli.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}'
    and soli.id_estado_solicitud not in  ('41','42','43','44','45','46','47','48','49')
    and (prod.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  prod.partition_date is null)
    and (est_soli.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  est_soli.partition_date is null)
    and (prosp.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  prosp.partition_date is null)
    and (domi.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  domi.partition_date is null)
    and (tel.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  tel.partition_date is null)
    and (camp.partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_ATENEA') }}' or  camp.partition_date is null);

