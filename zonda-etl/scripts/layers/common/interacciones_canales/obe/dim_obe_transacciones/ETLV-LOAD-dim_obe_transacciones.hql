SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.dim_obe_transacciones
PARTITION (partition_date)

select
DISTINCT CASE WHEN trx.transaccion='null' THEN NULL ELSE TRIM(trx.transaccion)	END					        cod_obe_transaccion,
CASE WHEN trx.descripcion='null' THEN NULL ELSE	 TRIM(trx.descripcion)	END			                ds_obe_transaccion,
CASE WHEN trx.modulo='null' THEN NULL ELSE	 TRIM(trx.modulo)	END			                        cod_obe_modulo,
CASE WHEN modu.descripcion='null' THEN NULL ELSE TRIM(modu.descripcion)	END				            ds_obe_modulo,
CASE WHEN trx.tipo_trx='null' THEN NULL ELSE	 TRIM(trx.tipo_trx)	END			                    cod_obe_tipo_transaccion,
CASE WHEN tipo.descripcion_tipo='null' THEN NULL ELSE	 TRIM(tipo.descripcion_tipo)	END			 ds_obe_tipo_transaccion,
CASE WHEN trx.submodulo='null' THEN NULL ELSE	 TRIM(trx.submodulo)	END			                cod_obe_submodulo,
CASE WHEN sub.descripcion='null' THEN NULL ELSE	 TRIM(sub.descripcion)	END			                ds_obe_submodulo,

trx.partition_date

from  bi_corp_staging.rio15_nb_desc_trx trx
left join bi_corp_staging.rio15_nb_modulos modu on (modu.modulo=trx.modulo and modu.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
left join bi_corp_staging.rio15_nb_submodulos sub  on (sub.submodulo=trx.submodulo and sub.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")
left join bi_corp_staging.rio15_nb_tipo_trx tipo  on (tipo.tipo_trx=trx.tipo_trx and tipo.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}")

where trx.partition_date="{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_OBE-Daily') }}";
