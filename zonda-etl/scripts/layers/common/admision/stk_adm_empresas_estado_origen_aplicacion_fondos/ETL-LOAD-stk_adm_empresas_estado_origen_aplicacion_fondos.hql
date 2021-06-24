set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_adm_empresas_estado_origen_aplicacion_fondos
    PARTITION (partition_date='{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}')
select
    cast(id as int) as id_adm_eoaf,
    date_format(fec_blc,'yyyy-MM-dd') as dt_adm_blc,
    if(consolidado='S', 1, 0) as flag_adm_consolidado,
    cast(penumper as int) as cod_adm_penumper,
    date_format(fec_eoaf,'yyyy-MM-dd') as dt_adm_eoaf,
    cast(deudxvtas as decimal(16, 2)) as fc_adm_deudxvtas,
    cast(bscambio as decimal(16, 2)) as fc_adm_bscambio,
    cast(deucomerci as decimal(16, 2)) as fc_adm_deucomerci,
    cast(deusocyfisc as decimal(16, 2)) as fc_adm_deusocyfisc,
    cast(deusistfinanc as decimal(16, 2)) as fc_adm_deusistfinanc,
    cast(inv_actintang as decimal(16, 2)) as fc_adm_inv_actintang,
    cast(aportescap as decimal(16, 2)) as fc_adm_aportescap,
    cast(distresult as decimal(16, 2)) as fc_adm_distresult,
    cast(credsocyev as decimal(16, 2)) as fc_adm_credsocyev,
    otr_varcaptr1 as ds_adm_otr_varcaptr1,
    cast(imp_otr_varcaptr1 as decimal(16, 2)) as imp_otr_varcaptr1,
    otr_varcaptr2 as ds_adm_otr_varcaptr2,
    cast(imp_otr_varcaptr2 as decimal(16, 2)) as imp_otr_varcaptr2,
    otr_varcaptr3 as ds_adm_otr_varcaptr3,
    cast(imp_otr_varcaptr3 as decimal(16, 2)) as imp_otr_varcaptr3,
    otr_oanooper1 as ds_adm_otr_oanooper1,
    otr_oanooper2 as ds_adm_otr_oanooper2,
    otr_oanooper3 as ds_adm_otr_oanooper3,
    cast(imp_otr_varcaptr1_ori as decimal(16, 2)) as fc_adm_imp_otr_varcaptr1_ori,
    cast(imp_otr_varcaptr2_ori as decimal(16, 2)) as fc_adm_imp_otr_varcaptr2_ori,
    cast(imp_otr_varcaptr3_ori as decimal(16, 2)) as fc_adm_imp_otr_varcaptr3_ori,
    cast(imp_otr_oanooper1_ori as decimal(16, 2)) as fc_adm_imp_otr_oanooper1_ori,
    cast(imp_otr_oanooper2_ori as decimal(16, 2)) as fc_adm_imp_otr_oanooper2_ori,
    cast(imp_otr_oanooper3_ori as decimal(16, 2)) as fc_adm_imp_otr_oanooper3_ori,
    cast(imp_otr_varcaptr1_apl as decimal(16, 2)) as fc_adm_imp_otr_varcaptr1_apl,
    cast(imp_otr_varcaptr2_apl as decimal(16, 2)) as fc_adm_imp_otr_varcaptr2_apl,
    cast(imp_otr_varcaptr3_apl as decimal(16, 2)) as fc_adm_imp_otr_varcaptr3_apl,
    cast(imp_otr_oanooper1_apl as decimal(16, 2)) as fc_adm_imp_otr_oanooper1_apl,
    cast(imp_otr_oanooper2_apl as decimal(16, 2)) as fc_adm_imp_otr_oanooper2_apl,
    cast(imp_otr_oanooper3_apl as decimal(16, 2)) as fc_adm_imp_otr_oanooper3_apl,
    cast(ingxvtasxperid as decimal(16, 2)) as fc_adm_ingxvtasxperid,
    cast(resultposblc as decimal(16, 2)) as fc_adm_resultposblc,
    cast(difmeses as int) as int_adm_difmeses,
    peusualt as cod_adm_peusualt,
    peusumod as cod_adm_peusumod,
    pefecalt as ts_adm_pefecalt,
    pefecmod as ts_adm_pefecmod,
    cast(id_blc as bigint) as id_adm_blc,
    observaciones as ds_adm_observaciones,
    cast(nro_ultima_prop as bigint) as cod_adm_nro_ultima_prop
from bi_corp_staging.sge_eoaf
where partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Admision_Empresas-Daily') }}';