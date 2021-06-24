set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_deudas_bancarias_propuesta
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(fecha_deuda_id as int) as id_adm_fecha_deuda,
    cast(cod_banco as int) as cod_adm_banco,
    cast(acc_cto_plz as decimal(16, 2)) as fc_adm_acc_cto_plz,
    cast(acc_lgo_plz as decimal(16, 2)) as fc_adm_acc_lgo_plz,
    cast(aecc_cto_plz as decimal(16, 2)) as fc_adm_aecc_cto_plz,
    cast(aecc_lgo_plz as decimal(16, 2)) as fc_adm_aecc_lgo_plz,
    cast(dto_doc_cto_plz as decimal(16, 2)) as fc_adm_dto_doc_cto_plz,
    cast(dto_doc_lgo_plz as decimal(16, 2)) as fc_adm_dto_doc_lgo_plz,
    cast(leasing_cto_plz as decimal(16, 2)) as fc_adm_leasing_cto_plz,
    cast(leasing_lgo_plz as decimal(16, 2)) as fc_adm_leasing_lgo_plz,
    cast(gtia_real_cto_plz as decimal(16, 2)) as fc_adm_gtia_real_cto_plz,
    cast(gtia_real_lgo_plz as decimal(16, 2)) as fc_adm_gtia_real_lgo_plz,
    cast(otr_gtias_cto_plz as decimal(16, 2)) as fc_adm_otr_gtias_cto_plz,
    cast(otr_gtias_lgo_plz as decimal(16, 2)) as fc_adm_otr_gtias_lgo_plz,
    cast(cart_cred_cto_plz as decimal(16, 2)) as fc_adm_cart_cred_cto_plz,
    cast(cart_cred_lgo_plz as decimal(16, 2)) as fc_adm_cart_cred_lgo_plz,
    cast(avales_cto_plz as decimal(16, 2)) as fc_adm_avales_cto_plz,
    cast(avales_lgo_plz as decimal(16, 2)) as fc_adm_avales_lgo_plz,
    cast(acc_cto_plz_dol as decimal(16, 2)) as fc_adm_acc_cto_plz_dol,
    cast(acc_lgo_plz_dol as decimal(16, 2)) as fc_adm_acc_lgo_plz_dol,
    cast(aecc_cto_plz_dol as decimal(16, 2)) as fc_adm_aecc_cto_plz_dol,
    cast(aecc_lgo_plz_dol as decimal(16, 2)) as fc_adm_aecc_lgo_plz_dol,
    cast(dto_doc_cto_plz_dol as decimal(16, 2)) as fc_adm_dto_doc_cto_plz_dol,
    cast(dto_doc_lgo_plz_dol as decimal(16, 2)) as fc_adm_dto_doc_lgo_plz_dol,
    cast(leasing_cto_plz_dol as decimal(16, 2)) as fc_adm_leasing_cto_plz_dol,
    cast(leasing_lgo_plz_dol as decimal(16, 2)) as fc_adm_leasing_lgo_plz_dol,
    cast(gtia_real_cto_plz_dol as decimal(16, 2)) as fc_adm_gtia_real_cto_plz_dol,
    cast(gtia_real_lgo_plz_dol as decimal(16, 2)) as fc_adm_gtia_real_lgo_plz_dol,
    cast(otr_gtias_cto_plz_dol as decimal(16, 2)) as fc_adm_otr_gtias_cto_plz_dol,
    cast(otr_gtias_lgo_plz_dol as decimal(16, 2)) as fc_adm_otr_gtias_lgo_plz_dol,
    cast(avales_cto_plz_dol as decimal(16, 2)) as fc_adm_avales_cto_plz_dol,
    cast(avales_lgo_plz_dol as decimal(16, 2)) as fc_adm_avales_lgo_plz_dol,
    cod_col_variable_1 as cod_adm_col_variable_1,
    cod_col_variable_2 as cod_adm_col_variable_2,
    cast(variable_1_cto_plz as decimal(16, 2)) as fc_adm_variable_1_cto_plz,
    cast(variable_1_lgo_plz as decimal(16, 2)) as fc_adm_variable_1_lgo_plz,
    cast(variable_1_cto_plz_dol as decimal(16, 2)) as fc_adm_variable_1_cto_plz_dol,
    cast(variable_1_lgo_plz_dol as decimal(16, 2)) as fc_adm_variable_1_lgo_plz_dol,
    cast(variable_2_cto_plz as decimal(16, 2)) as fc_adm_variable_2_cto_plz,
    cast(variable_2_lgo_plz as decimal(16, 2)) as fc_adm_variable_2_lgo_plz,
    cast(variable_2_cto_plz_dol as decimal(16, 2)) as fc_adm_variable_2_cto_plz_dol,
    cast(variable_2_lgo_plz_dol as decimal(16, 2)) as fc_adm_variable_2_lgo_plz_dol,
    cast(nro_prop as bigint) as cod_adm_nro_prop
from bi_corp_staging.sge_deudas_bancarias_prop
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';