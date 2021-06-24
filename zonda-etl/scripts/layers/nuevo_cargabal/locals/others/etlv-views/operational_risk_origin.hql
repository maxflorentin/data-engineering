insert overwrite table bi_corp_nuevo_cargabal.master_operational_risk_origin
select 'OPRO1','Internal fraud','Fraude Interno' union all
select 'OPRO2','External fraud','Fraude Externo' union all
select 'OPRO3','Employment, health and safety at work practices','Prácticas de empleo salud y seguridad en el trabajo' union all
select 'OPRO4','Customer, products and business practices','Prácticas con clientes productos y de negocio' union all
select 'OPRO5','Damage to tangible assets','Daños en activos fisicos' union all
select 'OPRO6','Business interruption and system failures','Interrumpcion del negocio y fallos en los sistemas' union all
select 'OPRO7','Execution, delivery and process management','Ejecución entrega y gestion de procesos' ;