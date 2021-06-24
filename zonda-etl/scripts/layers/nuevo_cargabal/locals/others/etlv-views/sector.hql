insert overwrite table bi_corp_nuevo_cargabal.master_sector
select 'SC01','Central banks','Bancos Centrales' union all
select 'SC02','Credit institutions','Instituciones de crédito' union all
select 'SC03','Clients','Clientes' union all
select 'SC0301','General governments','Administraciones Públicas' union all
select 'SC0302','Other Financial Corporation','Otras empresas financieras' union all
select 'SC0303','Non Financial Corporation','Empresas no financieras' union all
select 'SC0304','Households','Hogares' ;