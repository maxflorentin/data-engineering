"set mapred.job.queue.name=root.dataeng;
SELECT cod_trf_entidad_origen
      ,cod_trf_tipo_entidad_origen
      ,ds_trf_entidad_origen
      ,LPAD(cod_trf_suc_origen, 4, '0') cod_trf_suc_origen
      ,cod_trf_cta_origen
      ,ds_per_cuit_origen
      ,cod_trf_cbu_origen
      ,ds_per_nombre_origen
      ,LPAD(cod_per_nup_origen, 8, '0') cod_per_nup_origen
      ,cod_trf_entidad_destino
      ,cod_trf_tipo_entidad_destino
      ,ds_trf_entidad_destino
      ,LPAD(cod_trf_suc_destino, 4, '0') cod_trf_suc_destino
      ,cod_trf_cta_destino
      ,ds_per_cuit_destino
      ,cod_trf_cbu_destino
      ,ds_per_nombre_destino
      ,LPAD(cod_per_nup_destino, 8, '0') cod_per_nup_destino
      ,TO_DATE(ts_trf_fecha_estado) ts_trf_fecha_estado
      ,TO_DATE(ts_trf_fecha_alta) ts_trf_fecha_alta
      ,TO_DATE(ts_trf_fecha_debito) ts_trf_fecha_debito
      ,TO_DATE(ts_trf_fecha_env_riesgo) ts_trf_fecha_env_riesgo
      ,TO_DATE(ts_trf_fecha_credito) ts_trf_fecha_credito
      ,TO_DATE(ts_trf_fecha_env_camara) ts_trf_fecha_env_camara
      ,TO_DATE(ts_trf_fecha_rta_camara) ts_trf_fecha_rta_camara
      ,cod_trf_concepto
      ,ds_trf_concepto
      ,ds_trf_referencia
      ,fc_trf_importe
      ,cod_trf_moneda
      ,cod_trf_producto
      ,ds_trf_producto
      ,cod_trf_camara
      ,ds_trf_camara
      ,cod_trf_canal
      ,ds_trf_canal
      ,ds_trf_tipo
      ,cod_trf_en_camara
      ,cod_trf_estado
      ,cod_trf_mot_rechazo
      ,ds_trf_mot_rechazo
      ,cod_trf_origen
      ,TRUNC(TO_DATE(partition_date),'MM') mes_proceso
      ,TO_DATE(partition_date) fecha_proceso
FROM bi_corp_common.bt_trf_transferencias
WHERE partition_date between date_format('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='Transferencias_REPORT') }}', 'YYYY-MM-01')
 and last_day('{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='Transferencias_REPORT') }}')
 --partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='Transferencias_REPORT') }}'
"