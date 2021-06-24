set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.stk_chq_ordenesnopago
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')

-- source: malbgc (cuentas)
-- cheques del santander que tienen onp
SELECT
o.onc_entidad as cod_chq_entidad,
cast(o.onc_cuenta as bigint) as cod_chq_cuentacheque,
cast(substr(o.onc_num_cheque, 2, 9) as bigint) as cod_chq_nrocheque,
cast(o.onc_centro_alta as bigint) as cod_chq_sucursalgirada,
'0000' as cod_chq_codigopostal,
substr(o.onc_secuencia, 2, 5) as cod_chq_secuencia,
'MALBGC' as source,
case when o.onc_fecha_baja = '9999-12-31' then 1 else 0 end as flag_chq_active,
case when to_date(o.onc_fecha_alta) in ('9999-12-31', '0001-01-01') then NULL else to_date(o.onc_fecha_alta) end as dt_chq_fechaalta,
case when to_date(o.onc_fecha_baja) in ('9999-12-31', '0001-01-01') then NULL else to_date(o.onc_fecha_baja) end as dt_chq_fechabaja,
case when to_date(o.onc_fecha_confirm) in ('9999-12-31', '0001-01-01') then NULL else to_date(o.onc_fecha_confirm) end as dt_chq_fechaconfirm,
o.onc_divisa as cod_chq_divisa,
substr(o.onc_cont_onp, 2, 5) as cod_chq_contonp,
trim(o.onc_cod_periodico) as cod_chq_periodico,
trim(o.onc_motivo) as ds_chq_motivo,
trim(o.onc_observaciones) as ds_chq_observaciones,
case o.onc_denunciante WHEN 'B' THEN 'BEN' WHEN 'T' THEN 'TIT' ELSE null END AS ds_chq_denunciante,
trim(o.onc_nombre_benef) as ds_chq_nombrebenef,
trim(o.onc_tipo_id_denun) as cod_chq_tipoiddenun,
o.onc_id_denun as cod_chq_iddenun,
regexp_replace(trim(o.onc_hora_alta), "\\.", "\\:") as ds_chq_horaalta,
trim(o.onc_usuario_alta) as ds_chq_usuarioalta,
o.onc_estado as cod_chq_estado,
trim(o.onc_tipo_id_baja) as cod_chq_tipobaja,
trim(o.onc_id_baja) as cod_chq_baja,
regexp_replace(trim(o.onc_hora_baja), "\\.", "\\:") as ds_chq_horabaja,
trim(o.onc_usuario_baja) as ds_chq_usuariobaja,
trim(o.onc_tipo_id_conf) as cod_chq_tipoconf,
trim(o.onc_id_conf) as cod_chq_conf,
trim(o.onc_usuario_confir) as ds_chq_usuarioconfir,
regexp_replace(trim(o.onc_hora_confirm), "\\.", "\\:") as ds_chq_horaconfirm,
cast(o.onc_centro_umo as bigint) as cod_suc_sucursalumo,
o.onc_userid_umo as cod_chq_userumo,
trim(o.onc_netname_umo) as ds_chq_netnameumo,
o.onc_timest_umo as ts_chq_fechaumo,
o.onc_entidad_ogl as cod_chq_entidadogl,
cast(o.onc_centro_ogl as bigint) as cod_suc_sucursalogl,
cast(o.onc_cuenta_ogl as bigint) as cod_chq_cuentaogl,
o.onc_entidad_umo as cod_chq_entidadumo,
o.onc_divisa_ogl as cod_chq_divisaogl,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(o.onc_importe, "^0+", ''),1,length(REGEXP_REPLACE(o.onc_importe, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(o.onc_importe, "^0+", ''),-2)))as decimal(19, 4)),0) as fc_chq_importe
FROM bi_corp_staging.bqdtonc o
WHERE o.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
AND o.onc_fecha_alta >= '2019-01-01'
AND o.onc_fecha_alta <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'

UNION ALL

-- source: malzy (cheques)
-- cheques de otros bancos, que ingresan en cartera y tienen onp.
SELECT
o.bco_girado as cod_chq_entidad,
cast(o.cta_girada as bigint) as cod_chq_cuentacheque,
cast(o.nro_cheque as bigint) as cod_chq_nrocheque,
cast(o.suc_girada as bigint) as cod_chq_sucursalgirada,
cast(trim(o.cod_postal) as int) as cod_chq_codigopostal,
'1' as cod_chq_secuencia,
'MALZY' as source,
case when o.fecha_baja = '9999-12-31' then 1 else 0 end as flag_chq_active,
case when o.fecha_alta in ('9999-12-31', '0001-01-01') then NULL else o.fecha_alta end as dt_chq_fechaalta,
case when o.fecha_baja in ('9999-12-31', '0001-01-01') then NULL else o.fecha_baja end as dt_chq_fechabaja,
null as dt_chq_fechaconfirm,
null as cod_chq_divisa,
null as cod_chq_contonp,
null as cod_chq_periodico,
null as ds_chq_motivo,
null as ds_chq_observaciones,
CASE o.estado_onp WHEN '6' THEN 'TIT' WHEN '7' THEN 'BAN' WHEN '8' THEN 'TER' END AS ds_chq_denunciante,
null as ds_chq_nombrebenef,
null as cod_chq_tipoiddenun,
null as cod_chq_iddenun,
null as ds_chq_horaalta,
null as ds_chq_usuarioalta,
null as cod_chq_estado,
null as cod_chq_tipobaja,
null as cod_chq_baja,
null as ds_chq_horabaja,
null as ds_chq_usuariobaja,
null as cod_chq_tipoconf,
null as cod_chq_conf,
null as ds_chq_usuarioconfir,
null as ds_chq_horaconfirm,
null as cod_suc_sucursalumo,
null as cod_chq_userumo,
null as ds_chq_netnameumo,
o.tim_ult_modf as ts_chq_fechaumo,
null as cod_chq_entidadogl,
null as cod_suc_sucursalogl,
null as cod_chq_cuentaogl,
null as cod_chq_entidadumo,
null as cod_chq_divisaogl,
null as fc_chq_importe
FROM bi_corp_staging.malzy_alzyuonp o
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
AND o.fecha_alta = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}'
;