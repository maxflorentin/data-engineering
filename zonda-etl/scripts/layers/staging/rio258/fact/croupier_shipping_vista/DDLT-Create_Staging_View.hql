CREATE VIEW IF NOT EXISTS bi_corp_staging.vstk_getnet_croupier_shipping
select * from bi_corp_staging.rio258_croupier_shipping cs
inner join bi_corp_staging.rio258_croupier_shipping_log csl
on csl.SHIPPING_ID = cs.id
where content = 'GETNET';