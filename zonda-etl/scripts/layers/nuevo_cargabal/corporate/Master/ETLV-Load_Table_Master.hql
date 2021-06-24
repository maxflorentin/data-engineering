set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master
partition(partition_date= '2021-05') --'{{ ti.xcom_pull(task_ids='InputConfig', key='month_partition_bdr', dag_id='NWCA_LOAD_Tables') }}')
select 
    concat_ws('|',trim(a.rubro_altair),trim(a.cargabal),trim(a.rubro_niif)) as idcomb, --falta definirlo
    case when a.rubro_bcra in ('...') then 'Non continuing'
         else 'Continuing' end as continuing_operations,
    case when a.rubro_bcra in ('...') then 'HFS1'
         else 'HFS2' end as classified_as_held_for_sale,
    case when a.rubro_altair RLIKE '^1|^2|^3|^4|^65' then 'B'
         when a.rubro_altair RLIKE '^5|^6' then 'R'
         when a.rubro_altair RLIKE '^7' then 'M'
         else null end as base,
    case when a.rubro_altair RLIKE '^1|^2' then 'B01'
         when a.rubro_altair RLIKE '^3' then 'B02'
         when a.rubro_altair RLIKE '^4' then 'B03'
         when a.rubro_altair RLIKE '^65' then '...' --¿'B04' o 'B05'?
         when a.rubro_altair RLIKE '^7' then 'M'
         else null end as sub_base_1,
    '...' as main_category,
    '...' as product,
    '...' as accounting_portfolio,
    '...' as sector,
    '...' as stages,
    '...' as poci,
    '...' as accounting_hedge,
    '...' as ineffective,
    '...' as hedged_item,
    '...' as recorded_current_period,
    '...' as vat_base,
    '...' as tangible_asset_purpose,
    '...' as assets_origin,
    '...' as oci_gross_tax_impact,
    '...' as equity_generator,
    '...' as group_minority_interests,
    '...' as pension_plan_detail,
    '...' as pension_concepts,
    '...' as expenses_income,
    '...' as pl_generator,
    '...' as acc_portfolio_reclassification,
    '...' as subordinated,
    '...' as total_partial_write_off,
    '...' as operational_risk,
    '...' as operational_risk_origin,
    '...' as assetbacked_securities_interest,
    '...' as spanish_corporate_income_tax,
    '...' as policyholder_risk,
    '...' as shadow_accounting,
    '...' as saving,
    '...' as perfoming_fv,
    '...' as technical_provisions_detail,
    '...' as temporary_difference_origin,
    '...' as detail_of_other_concepts,
    '...' as defaulted,
    '...' as forbearance,
    '...' as subsector_2,
    '...' as financial_instrument_purpose,
    '...' as collateral,
    '...' as estimated_recovery_period,
    '...' as type_of_value,
    '...' as fair_value_detail
from bi_corp_staging.alha9601 a
where partition_date = '2021-05-10';