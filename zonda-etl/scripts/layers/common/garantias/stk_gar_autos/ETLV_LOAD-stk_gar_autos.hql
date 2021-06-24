"
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.stk_gar_autos
PARTITION(partition_date)

select
trim(AUVV_AUMA_CD_MARCA)                  				 	cod_gar_marca,
trim(AUVV_AUMO_CD_MODELO)                 					cod_gar_modelo,
trim(AUVV_ANO)                            					cod_gar_anio,
trim(auma_de_marca)                       					ds_gar_marca,
trim(aumo_de_modelo)                      					ds_gar_modelo,
trim(auth_de_tipo_vehiculo)               					ds_gar_tipo_vehiculo,
CASE WHEN trim(aumo_in_importado) ='S' THEN 1 ELSE 0 END    flag_gar_importado,
CAST(AUVV_VALOR AS DECIMAL(19,4))   						fc_gar_valor,
ve.partition_date                   						partition_date

from bi_corp_staging.rio6_autt_valores_vehiculos ve
left join bi_corp_staging.rio6_autt_marcas ma on (ma.auma_cd_marca=ve.AUVV_AUMA_CD_MARCA and ve.partition_date=ma.partition_date)
left join bi_corp_staging.rio6_autt_modelos mo on
(trim(mo.aumo_cd_modelo)=trim(ve.AUVV_AUMO_CD_MODELO)
and trim(mo.aumo_auma_cd_marca)=trim(ve.AUVV_AUMA_CD_MARCA)
and mo.partition_date=ve.partition_date)left join bi_corp_staging.rio6_autt_tipos_vehiculos ti on (ti.auth_cd_tipo_vehiculo= mo.aumo_auth_cd_tipo_veh and ti.partition_date=ve.partition_date)
where ve.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'
and  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}'> '2020-08-31'

UNION ALL

select
cd_marca                                                    cod_gar_marca,
cd_modelo                                                   cod_gar_modelo,
anio                                                        cod_gar_anio,
desc_marca                                                  ds_gar_marca,
desc_modelo                                                 ds_gar_modelo,
tipo_veh                                                    ds_gar_tipo_vehiculo,
CASE WHEN trim(importado) ='S' THEN 1 ELSE 0 END            flag_gar_importado,
CAST(valor AS DECIMAL(19,4))   						        fc_gar_valor,
partition_date                   						    partition_date
from bi_corp_staging.infoautos
where partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_GARANTIAS') }}'
       '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_GARANTIAS') }}' < '2020-08-31'


"