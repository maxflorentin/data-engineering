CREATE VIEW IF NOT EXISTS bi_corp_nuevo_cargabal.view_full_master_spanish AS
SELECT m.idcomb,
       m.continuing_operations, 
       a.descripcion as classified_as_held_for_sale, 
       b.descripcion as base,
       c.descripcion as sub_base_1,
       main_category            , product                        , accounting_portfolio,
       sector                   , stages                         , poci,
       accounting_hedge         , ineffective                    , hedged_item,
       recorded_current_period  , vat_base                       , tangible_asset_purpose,
       assets_origin            , oci_gross_tax_impact           , equity_generator,
       group_minority_interests , pension_plan_detail            , pension_concepts,
       expenses_income          , pl_generator                   , acc_portfolio_reclassification,
       subordinated             , total_partial_write_off        , operational_risk,
       operational_risk_origin  , assetbacked_securities_interest, spanish_corporate_income_tax,
       policyholder_risk        , shadow_accounting              , saving,
       perfoming_fv             , technical_provisions_detail    , temporary_difference_origin,
       detail_of_other_concepts , defaulted                      , forbearance,
       subsector_2              , financial_instrument_purpose   , collateral,
       estimated_recovery_period, type_of_value                  , fair_value_detail,
       partition_date
FROM bi_corp_nuevo_cargabal.master m
    LEFT JOIN bi_corp_nuevo_cargabal.master_classified_as_held_for_sale a
        ON m.classified_as_held_for_sale = a.id
    LEFT JOIN bi_corp_nuevo_cargabal.master_base b
        ON m.base = b.id
    LEFT JOIN bi_corp_nuevo_cargabal.master_base c
        ON m.sub_base_1 = c.id        
    LEFT JOIN bi_corp_nuevo_cargabal.master_main_category d
        ON m.main_category = d.id;