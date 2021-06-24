SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table bi_corp_common.stk_chq_pagoatribuciones
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')

select
case when cmc7 = 'null' then cast(null as string) else lpad(substr(cmc7,1,3), 4, '0') end as cod_chq_entidad,
case when cmc7 = 'null' then cast(null as bigint) else cast(substr(cmc7,11,8) as bigint) end as cod_chq_nrocheque,
case when cmc7 = 'null' then cast(null as bigint) else cast(substr(cmc7,23,11) as bigint) end as cod_chq_cuentacheque,
case when cmc7 = 'null' then cast(null as bigint) else cast(lpad(substr(cmc7,4,3), 4, '0') as bigint) end as cod_chq_sucursalgirada,
case when cmc7 = 'null' then cast(null as int) else cast(substr(cmc7,7,4) as bigint) end as cod_chq_codigopostal,
cast(lpad(suc_rio, 4, '0') as bigint) as cod_suc_sucursal,
cod_prod as cod_chq_producto,
cod_subprod as cod_chq_subproducto,
cast(cod_nup as bigint) as cod_per_nup,
cast(cta_rio as bigint) as cod_chq_cuentacorriente,
cod_moneda as cod_chq_divisa,
trim(nom_titular) as ds_chq_nombretitular,
cod_seg as cod_chq_segmento,
tipo_camara as cod_chq_camara,
nivel_workf as cod_chq_nivelworkflow,
ind_pag_rech_chq as cod_chq_marcaprcheque,
ind_pag_rech_rech as cod_chq_marcaprmotivo,
cod_mot_rech as cod_chq_motivorechazo,
case when trim(cod_just_pago)='' then cast(null as string) else trim(cod_just_pago) end as cod_chq_justificacionpago,
case when trim(leg_gestion_n1)='' then cast(null as string) else leg_gestion_n1 end as cod_chq_legajogestionn1,
case when trim(hora_gestion_n1)='' then cast(null as string) else hora_gestion_n1 end as ds_chq_horagestionn1,
case when trim(leg_gestion_n2)='' then cast(null as string) else leg_gestion_n2 end as cod_chq_legajogestionn2,
case when trim(hora_gestion_n2)='' then cast(null as string) else hora_gestion_n2 end as ds_chq_horagestionn2,
case when trim(leg_gestion_n3)='' then cast(null as string) else leg_gestion_n3 end as cod_chq_legajogestionn3,
case when trim(hora_gestion_n3)='' then cast(null as string) else hora_gestion_n3 end as ds_chq_horagestionn3,
case when trim(leg_aprob)='' then cast(null as string) else leg_aprob end as cod_chq_legajoaprobacion,
case when trim(hora_aprob)='' then cast(null as string) else hora_aprob end as ds_chq_horaaprobacion,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(imp_mto_acuerdo, "^0+", ''),1,length(REGEXP_REPLACE(imp_mto_acuerdo, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(imp_mto_acuerdo, "^0+", ''),-2))) as decimal(19, 2)),0) as fc_chq_montoacuerdo,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(imp_mto_calific, "^0+", ''),1,length(REGEXP_REPLACE(imp_mto_calific, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(imp_mto_calific, "^0+", ''),-2))) as decimal(19, 2)),0) as fc_chq_montocalificado,
coalesce(cast(trim(concat(substr(REGEXP_REPLACE(imp_cheque, "^0+", ''),1,length(REGEXP_REPLACE(imp_cheque, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(imp_cheque, "^0+", ''),-2))) as decimal(19, 2)),0) as fc_chq_importe,
case when sdo_deudor_signo='+' then coalesce(cast(trim(concat(substr(REGEXP_REPLACE(saldo_deudor, "^0+", ''),1,length(REGEXP_REPLACE(saldo_deudor, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(saldo_deudor, "^0+", ''),-2))) as decimal(19, 2)),0) else (-1) * coalesce(cast(trim(concat(substr(REGEXP_REPLACE(saldo_deudor, "^0+", ''),1,length(REGEXP_REPLACE(saldo_deudor, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(saldo_deudor, "^0+", ''),-2))) as decimal(19, 2)),0) end as fc_chq_saldodeudor,
case when sdo_disp_signo='+' then coalesce(cast(trim(concat(substr(REGEXP_REPLACE(sdo_disponible, "^0+", ''),1,length(REGEXP_REPLACE(sdo_disponible, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(sdo_disponible, "^0+", ''),-2)))as decimal(19, 2)),0) else (-1) * coalesce(cast(trim(concat(substr(REGEXP_REPLACE(sdo_disponible, "^0+", ''),1,length(REGEXP_REPLACE(sdo_disponible, "^0+", ''))-2),'.',substr(REGEXP_REPLACE(sdo_disponible, "^0+", ''),-2)))as decimal(19, 2)),0) end as fc_chq_saldodisponible
from bi_corp_staging.acle_pacha_historial_riesgos
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}';