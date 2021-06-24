SET mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;


insert overwrite table bi_corp_common.stk_chq_rechazadosbcra
partition(partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}')

SELECT distinct
cast(lpad(trim(nro_cheque),8,'0') as bigint) as cod_chq_nrocheque,
case when to_date(from_unixtime(UNIX_TIMESTAMP(fecha_rechazo,"yyyyMMdd"))) in ('9999-12-31', '0001-01-01') then NULL else to_date(from_unixtime(UNIX_TIMESTAMP(fecha_rechazo,"yyyyMMdd"))) end as dt_chq_fecharechazo,
case when to_date(from_unixtime(UNIX_TIMESTAMP(fecha_levantamiento,"yyyyMMdd"))) in ('9999-12-31', '0001-01-01') then NULL else to_date(from_unixtime(UNIX_TIMESTAMP(fecha_levantamiento,"yyyyMMdd"))) end as dt_chq_fechalevantamiento,
case when upper(trim(pago_multa)) not in ('IMPAGA','SUSPENDIDA') then to_date(from_unixtime(UNIX_TIMESTAMP(pago_multa,"dd/MM/yyyy"))) else upper(trim(pago_multa)) end as ds_chq_pagomulta,
cuit as ds_chq_cuit,
judicial as cod_chq_judicial,
case judicial when 'F' then 'FIN DE PROCESO JUDICIAL' when 'J' then 'EN PROCESO JUDICIAL' else '' end as ds_chq_judicial,
revision as cod_chq_revision,
case revision when 'E' then 'EN REVISION' when 'S' then 'FIN DE REVISION' else '' end as ds_chq_revision,
causal as cod_chq_causal,
case causal when '1' then 'POR VICIOS FORMALES' when '2' then 'SIN FONDOS' else '' end as ds_chq_causal,
cuit_relacionado as ds_chq_cuitrelacionado,
cast(trim(concat(substr(monto,1,length(monto)-2),'.',substr(monto,-2)))as decimal(19, 4)) as fc_chq_monto
FROM bi_corp_staging.bcra_cheques_rechazados
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cheques') }}';