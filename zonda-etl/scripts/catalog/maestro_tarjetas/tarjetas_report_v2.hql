set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
SELECT coalesce(cast(s02.s02_nro_tarjeta as string),''),
coalesce(cast(s02.s02_rel_aplicacion as string),''),
coalesce(cast(s02.s02_rel_cod_suc as string),''),
coalesce(cast(s02.s02_rel_tipo_cta as string),''),
coalesce(cast(s02.s02_rel_nro_cta as string),''),
coalesce(cast(s02.s02_rel_nro_firm as string),''),
coalesce(cast(s02.s02_tipo_cuenta as string),''),
coalesce(cast(s02.s02_cuenta_tc as string),''),
coalesce(cast(s02.s02_cod_sucursal as string),''),
coalesce(cast(s02.s02_estado_tarjeta as string),''),
coalesce(cast(s02.s02_fecha_alta as string),''),
coalesce(cast(s02.s02_motivo_alta as string),''),
coalesce(cast(s02.s02_nro_solicitud as string),''),
coalesce(cast(s02.s02_campania_id as string),''),
coalesce(cast(s02.s02_campania_nro_shot as string),''),
coalesce(cast(s02.s02_vig_des_aa as string),''),
coalesce(cast(s02.s02_vig_des_mm as string),''),
coalesce(cast(s02.s02_vig_has_aa as string),''),
coalesce(cast(s02.s02_vig_has_mm as string),''),
coalesce(cast(s02.s02_ult_modif_fec as string),''),
coalesce(cast(s02.s02_ult_modif_orig as string),''),
coalesce(cast(s02.s02_ult_modif_hora as string),''),
coalesce(cast(s02.s02_ult_modif_userid as string),''),
coalesce(cast(s02.s02_tipo_tarjeta as string),''),
coalesce(cast(s02.s02_marca_women as string),''),
coalesce(cast(s02.s02_no_renov_motivo as string),''),
coalesce(cast(s02.s02_no_renov_fecha as string),''),
coalesce(cast(s02.s02_fec_baja as string),''),
coalesce(cast(s02.s02_motivo_baja as string),''),
coalesce(cast(s02.s02_nro_cta_emp as string),''),
coalesce(cast(s02.s02_mca_habilit as string),''),
coalesce(cast(s02.s02_mca_cargo as string),''),
coalesce(cast(s02.s02_apel_nomb_emb as string),''),
coalesce(cast(s02.s02_cod_categoria as string),''),
coalesce(cast(s02.s02_fec_incl_boletin as string),''),
coalesce(cast(s02.s02_motivo_incl_boletin as string),''),
coalesce(cast(s02.s02_userid_alta as string),''),
coalesce(cast(s02.s02_canal_vta as string),''),
coalesce(cast(s02.s02_autorizado_tc as string),''),
coalesce(cast(s02.s02_marca_tc as string),''),
coalesce(cast(s02.s02_cargo as string),''),
coalesce(cast(s02.s02_bonificacion as string),''),
coalesce(cast(s02.s02_cuotas as string),''),
coalesce(cast(s02.s02_linea4 as string),''),
coalesce(cast(s02.s02_lim_compras as string),''),
coalesce(cast(s02.s02_lim_cuotas as string),''),
coalesce(cast(s02.s02_lim_adelantos as string),''),
coalesce(cast(s02.s02_estado_activacion as string),''),
coalesce(cast(s02.s02_nro_tarjeta_relac as string),''),
coalesce(cast(s02.s02_fecha_actual_women as string),''),
coalesce(cast(p08_use.penumper as string),'')
from bi_corp_staging.amas_wamas02 s02
left join (
SELECT p08.penumper,p08.penumcon,p08.pecodofi,p08.peordpar,p08.partition_date
FROM bi_corp_staging.malpe_pedt008 p08
INNER JOIN (
SELECT max(partition_date) AS partition_date
FROM bi_corp_staging.malpe_pedt008
WHERE partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Maestro-Tarjetas_v2') }}' and partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Maestro-Tarjetas_v2') }}',7)
) mx8 ON p08.partition_date = mx8.partition_date
where p08.partition_date <= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Maestro-Tarjetas_v2') }}' and p08.partition_date >= date_sub('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Maestro-Tarjetas_v2') }}',7)
) p08_use on p08_use.penumcon = coalesce(cast(s02.s02_rel_nro_cta as string),'') AND REGEXP_REPLACE(p08_use.pecodofi, "^0+", '') = coalesce(cast(s02.s02_rel_cod_suc as string),'') AND REGEXP_REPLACE(p08_use.peordpar, "^0+", '') = coalesce(cast(s02.s02_rel_nro_firm as string),'')
where s02.partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='REPORT_Maestro-Tarjetas_v2') }}';
