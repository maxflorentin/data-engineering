insert overwrite table bi_corp_nuevo_cargabal.master_equity_generator
select 'EQGE1','Costs incurred in transactions on own capital instruments','Costes de transacciones realizadas sobre instrumentos de capital propios' union all			
select 'EQGE2','Gains/losses on sales of treasury shares','Resultados por venta de acciones propias' union all			
select 'EQGE3','Results from derivative instruments on treasury shares','Resultados por intrumentos derivados sobre acciones propias' union all			
select 'EQGE4','Materialization of gains/losses on sales of equity instruments at FV OCI','Materializacion de resultados en venta intrumentos de patrimonio FV OCI' union all			
select 'EQGE5','Financial liabilities at FV P&L early settled - Own risk','Pasivos financieros a valor razonable con cambios PYG liquidados de forma anticipada  Riesgo propio' union all			
select 'EQGE6','Derivatives on own equity instruments','Derivados sobre intrumentos de partrimonio propio' union all			
select 'EQGE7','Coupon and Tier 1 CoCos','Cupon y gastos Tier 1 Cocos' union all			
select 'EQGE8','Adjustments due to changes in accounting policies','Impactos reexpresion estados cambios normativos' union all			
select 'EQGE9','Rest of other reserves','Otros reservas' union all			
select 'EQGE10','Nominal Value of parent company shares','Nominal Value of parent company shares' union all			
select 'EQGE11','Reserves of the parent company shares','Reserves of the parent company shares' union all			
select 'EQGE12','Reserves. Purchase difference','Reservas diferencias en compras' union all			
select 'EQGE13','Reserves. Gains/losses on sales','Reservas resultado en venta' union all			
select 'EQGE14','Reallocated Goodwill due to sales','Reallocated Goodwill due to sales' union all			
select 'EQGE15','Exchange Difference - arised from Goodwill','Exchange Difference - arised from Goodwill' union all			
select 'EQGE16','Exchange Difference - arised from the rest of assets and liabilities','Exchange Difference - arised from the rest of assets and liabilities' ;