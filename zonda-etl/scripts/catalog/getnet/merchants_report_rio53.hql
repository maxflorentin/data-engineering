"set mapred.job.queue.name=root.dataeng;
select mer_account_blocked
        ,mer_account_enabled
        ,mer_account_id
        ,mer_apartment
        ,mer_category
        ,mer_cbu
        ,mer_city
        ,mer_codigo_canal
        ,to_date(mer_created_at)
        ,mer_cuit
        ,mer_document_number
        ,mer_email
        ,mer_exclude_income_tax
        ,mer_exclude_vat
        ,to_date(mer_fecha_alta_empresa)
        ,mer_first_name
        ,mer_floor
        ,mer_funcionario
        ,lpad(trim(mer_id_externo),8,'0')
        ,mer_iibb
        ,nvl(TO_DATE(mer_iibb_exclusion_desde), to_date('0001-01-01'))
        ,nvl(TO_DATE(mer_iibb_exclusion_hasta), to_date('0001-01-01'))
        ,mer_ingresos
        ,mer_iva
        ,mer_last_name
        ,mer_legal_name
        ,mer_name
        ,mer_numero_iibb
        ,mer_pep
        ,mer_perfil_riesgo
        ,mer_phone
        ,mer_postal_code
        ,mer_province
        ,mer_province_id
        ,mer_sexo
        ,mer_street
        ,mer_street_number
        ,mer_subsidiary_enabled
        ,mer_subsidiary_id
        ,lpad(mer_sucursal_alta,4,'0')
        ,mer_sujeto_obligado
        ,mer_tyc
        ,mer_type
        ,to_date(partition_date)
 from bi_corp_staging.getnet_merchants
where partition_date =  '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='GETNET_REPORT') }}'
"