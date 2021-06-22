(
case when '{{ ti.xcom_pull(task_ids='InputConfig', key='first_calendar_day', dag_id='BDR_LOAD_Tables-Monthly')}}' >= '2019-12-01' then '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='BDR_LOAD_Tables-Monthly')}}' else '2019-12-04' end
)




bt_rec_gest_empresas_sgc
bt_rec_gest_estados_sgc
bt_rec_gest_individuos_sgc
bt_rec_gestiones_sgc
bt_rec_informacion_adjunta
dim_rec_clasif_crm
dim_rec_concepto
dim_rec_concepto_bcra
dim_rec_concepto_bcra_circ
dim_rec_concepto_espana
dim_rec_concepto_espana_circ
dim_rec_concepto_sac
dim_rec_concepto_sac_circ
dim_rec_info_requerida_valores_hijos
dim_rec_informacion_requerida
dim_rec_producto
dim_rec_sector
dim_rec_subproducto
dim_rec_tipo_medio