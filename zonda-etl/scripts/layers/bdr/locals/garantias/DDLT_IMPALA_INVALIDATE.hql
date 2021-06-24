
INVALIDATE METADATA bi_corp_bdr.aux_garant_calif_pais;

INVALIDATE METADATA bi_corp_bdr.aux_garant_calif_empresa;

INVALIDATE METADATA bi_corp_bdr.aux_garant_contratos;

INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_especificas;

INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_genericas;

INVALIDATE METADATA bi_corp_bdr.aux_garant_garantias_contratos;


compute incremental stats bi_corp_bdr.aux_garant_calif_pais PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');

compute incremental stats bi_corp_bdr.aux_garant_calif_empresa PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');

compute incremental stats bi_corp_bdr.aux_garant_contratos PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');

compute incremental stats bi_corp_bdr.aux_garant_garantias_especificas PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');

compute incremental stats bi_corp_bdr.aux_garant_garantias_genericas PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');

compute incremental stats bi_corp_bdr.aux_garant_garantias_contratos PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_BDR-Garantias_Contratos-Monthly') }}');
