insert overwrite table bi_corp_nuevo_cargabal.master_base
select 'B','Balance','Balance'  union all
select 'B01','Assets','Activo' union all
select 'B02','Liabilities','Pasivo' union all
select 'B03','Equity','Patrimonio' union all
select 'B04','OCI - Items that will not be reclassified to profit and loss','OCI que no son reclasificadas a P/L' union all
select 'B05','OCI - Items that may be reclassified to profit and loss','OCI que deben ser reclasificadas a P/L' union all
select 'M','Off balance and Memorandum','' union all
select 'M01','Off balance','Fuera de Balance' union all
select 'M02','Memorandum','Memorando' union all
select 'M0201','Financial assets transferred','Financial assets transferred' union all
select 'M020101','Securitization. Partially derecognised in Balance Sheet','Titulizacion. Parcialmente dado de baja en balance' union all
select 'M020102','Securitization.Derecognised in Balance Sheer before 01-01-2004','Titulizacion. Dado de baja en balance antes de 01.01.2004' union all
select 'M020103','Securitization.Derecognised in Balance Sheer after 01-01-2004','Titulizacion. Dado de baja en balance después de 01.01.2004' union all
select 'M020104','Non-securitization','Sin titulizacion' union all
select 'M0202','Write-offs memorandum','Fallidos Memorando' union all
select 'M0203','Other memorandum accounts','Other memorandum accounts' union all
select 'M0204','Unmatured foreign currency purchase and sale transactions','Unmatured foreign currency purchase and sale transactions' union all
select 'R','Results','Resultado' union all
select 'R01','Interest income','Ingresos por intereses' union all
select 'R02','Interest expense','Gastos por intereses' union all
select 'R03','Dividend income','Ingresos por Dividendos' union all
select 'R04','Income Fee and commission','Ingresos por comisiones' union all
select 'R05','Expense Fee and commission','Gastos por comisiones' union all
select 'R06','Gains and losses on derecognition and remeasurements of financial instruments','Perdidas y ganancias en instrumentos financieros dados de baja o reestimados' union all
select 'R07','Gains and losses from hedge accounting hedge instrument','Perdidas y ganancias de contabilidad de coberturas instrumento de cobertura' union all
select 'R08','Gains and losses from hedge accounting hedged item','Perdidas y ganancias de contabilidad de coberturas elemento cubierto' union all
select 'R09','Exchange differences','Diferencias por tipos de cambio' union all
select 'R10','Other operating Income','Otros ingresos de explotacion' union all
select 'R1001','Exploitation of investment property and operating leases from financial institutions','Exploitation of investment property and operating leases from financial institutions' union all
select 'R100101','Income from exploitation of investment property','Ingresos por explotacion de Inversiones inmobiliarias' union all
select 'R100102','Income from operating leases','Ingresos de arrendamientos operativos' union all
select 'R1002','Other operating Income, rest','Resto Otros ingresos de explotacion' union all
select 'R100201','Indemnities from insurance companies','Indemnizaciones de entidades aseguradoras' union all
select 'R100202','Yields for the provision of atypical services','Rendimientos por prestacion de servicios atipicos' union all
select 'R100203','Other operating income, other','Otros ingresos' union all
select 'R1003','Monetary correction Income','Ingresos por corrección monetaria' union all
select 'R1004','Non-financial activities income','Ingresos de actividades no financieras' union all
select 'R100401','Sales','Ventas' union all
select 'R100402','Increase in inventories of finished goods','Aumento existencias productos terminados' union all
select 'R100403','Work carried out by the Group for assets','Trabajos efectuados por el Grupo para su activo' union all
select 'R100404','Capital grants transferred to the result for the year','Subvenciones Capital transferidas a resultados del ejercicio' union all
select 'R100405','Provision of non-financial services','Prestacion de servicios no financieros' union all
select 'R100406','Income from operating leases - Non-financial activities','Ingresos de arrendamientos operativos - Actividades no financieras' union all
select 'R100407','Financial income','Ingresos Financieros' union all
select 'R11','Other operating expenses','Otros gastos de explotacion' union all
select 'R1101','Contribution to deposit guarantee fund','Contribucion al Fondo de Garantia de Depositos' union all
select 'R1102','Contribution to SRF (Single Resolution Fund)','Contribucion al FUR ()' union all
select 'R1103','Non-financial activities expenses','Gastos de actividades no financieras' union all
select 'R110301','Decrease in inventories of finished goods','Reduccion existencias productos terminados' union all
select 'R110302','Supplies','Aprovisionamientos' union all
select 'R110303','Expenses from operating leases','Costes de arrendamientos operativos' union all
select 'R110304','Other expenses from non-financial activities','Otros gastos de actividades no financieras' union all
select 'R110305','Financial expenses','Gastos Financieros' union all
select 'R1104','Monetary correction expenses','Gastos por corrección monetaria' union all
select 'R1105','Other operating expenses, rest','Resto Otros gastos de explotacion' union all
select 'R12','Income from assets under insurance and reinsurance contracts','Ingresos de activos amparados por contratos de seguro o reaseguro' union all
select 'R1201','Insurance and reinsurance premiums charged','Primas de seguros y reaseguros cobradas' union all
select 'R1202','Other net provisions for liabilities under insurance contracts - income','Resto de dotaciones netas a pasivos por contratos de seguros - ingreso' union all
select 'R1203','Financial income from assets related to savings insurance products','Por activos afectos a productos de Seguro Ahorro' union all
select 'R1204','Income from claims','Ingresos por siniestros' union all
select 'R1205','Fees charged and other','Comisiones cobradas y Otros' union all
select 'R13','Expenses from liabilities under insurance and reinsurance contracts','Gastos de pasivos por contratos de seguro o reaseguro' union all
select 'R1301','Other net provisions for liabilities under insurance contracts - expense','Resto de dotaciones netas a pasivos por contratos de seguros - gasto' union all
select 'R1302','Insurance benefits paid','Prestaciones pagadas' union all
select 'R1303','Reinsurance premiums paid','Primas de reaseguro pagadas' union all
select 'R1304','Derivatives valuation','Valoracion de derivados' union all
select 'R1305','Valuation or sale of other financial instruments','Valoracion o venta de otros instrumentos financieros' union all
select 'R1306','Commissions, participations and other expenses - Held for trading portfolio','Comisiones participaciones y otros gastos cartera mantenida para negociar' union all
select 'R14','Modifications. Without derecognition','Modificaciones sin dar de baja' union all
select 'R15','Impairment provision or release financial assets','Provision por deterioro o reversion de la provision por activos financieros' union all
select 'R1501','Other Impairment provision or release financial assets','Other Impairment provision or release financial assets' union all
select 'R1502','Write-offs','Fallidos' union all
select 'R16','Personnel expenses','Gastos de personal' union all
select 'R1601','Wages and salaries','Sueldos y Salarios' union all
select 'R160101','Fixed compensation','Retribuciones fijas' union all
select 'R160102','Variable compensation (bonus)','Retribuciones variables (Bonus)' union all
select 'R1602','Other personnel expenses','Otros gastos de personal' union all
select 'R160201','Social benefits','Beneficios sociales' union all
select 'R160202','Training','Formación' union all
select 'R160203','Other personnel expenses, rest','Otros gastos de personal, resto' union all
select 'R1603','National Insurance contributions','Seguridad Social' union all
select 'R1604','Compensation based on equity instruments','Remuneraciones basadas en instrumentos de patrimonio' union all
select 'R1605','Indemnity payments','Indemnizaciones' union all
select 'R1606','Pensions provision','Pensions provision' union all
select 'R17','Other Administrative expenses','Otros gastos de administracion' union all
select 'R1701','Real estate and facilities','Inmuebles e instalaciones' union all
select 'R170101','Rent','Alquileres' union all
select 'R170102','Fixed asset repairs and maintenance','Reparaciones y mantenimientos de inmovilizado' union all
select 'R170103','Repairs and maintenance of other property, plant and equipment','Reparaciones y mantenimientos de otros inmovilizado material' union all
select 'R170104','Lighting, water and heating','Alumbrado agua y calefaccion' union all
select 'R1702','Printed matter and office material','Impresos y material de oficina' union all
select 'R170201','Stationery for customer information','Papeleria para informacion a clientes' union all
select 'R170202','Stationery and consumables','Material de escritorio y consumibles' union all
select 'R170203','Card expenses','Tarjetas' union all
select 'R1703','Technology and systems','Tecnologia y sistemas' union all
select 'R170301','Software acquisition','Adquisición de software' union all
select 'R170302','Software maintenance','Mantenimiento de software' union all
select 'R170303','Equipment maintenance','Mantenimiento equipos' union all
select 'R170304','Equipment rental','Alquiler equipos' union all
select 'R170305','Outside services. Software development','Servicios externos de sistemas Desarrollo' union all
select 'R170306','Outside services. Maintenance and others','Servicios externos de sistemas Mantenimiento y otros' union all
select 'R1704','Communications','Comunicaciones' union all
select 'R170401','Telephone-Voice','TELEFONO VOZ' union all
select 'R170402','Communication-Data','COMUNICACION DATOS' union all
select 'R170403','Postage','CORREOS' union all
select 'R170404','Mailbag services','VALIJAS' union all
select 'R170405','Communications - Value added services','COMUNICACIONES SERVICIOS DE VALOR AñADIDO' union all
select 'R1705','Advertising','Publicidad' union all
select 'R170501','Advertising - Media','PUBLICIDAD MEDIOS' union all
select 'R170502','Advertising-Support at points of sale','PUBLICIDAD SOPORTE EN PUNTOS DE VENTA' union all
select 'R170503','Advertising-Promotional material gifts','PUBLICIDAD MATERIAL PROMOCIONAL REGALOS' union all
select 'R170504','Advertising-Public relations and events','PUBLICIDAD RELACIONES PUBLICAS Y EVENTOS' union all
select 'R170505','Advertising- Sponsorship and others','PUBLICIDAD PATROCINIO Y OTROS' union all
select 'R1706','Experts reports','Informes tecnicos' union all
select 'R170601','Legal advice','ASESORAMIENTO JURIDICO Y LEGAL' union all
select 'R170602','Legal and lawyer external services','SERVICIOS EXTERNOS CONTRATADOS JURIDICOS Y DE LETRADOS' union all
select 'R170603','Audit','AUDITORIAS' union all
select 'R170604','External advisory services-Consulting','ASESORAMIENTO EXTERNO CONSULTORIAS' union all
select 'R1707','Services of surveillance and transfer of funds','Servicio de vigilancia y traslado de fondos' union all
select 'R170701','Security','SEGURIDAD' union all
select 'R170702','Fund transport','TRANSPORTE Y TRASLADO FONDOS' union all
select 'R1708','Insurance premiums','Primas de seguros' union all
select 'R1709','Staff travel and per diems expenses','Dietas y desplazamientos' union all
select 'R170901','Travel expenses-Transport','GASTOS VIAJE LOCOMOCION' union all
select 'R170902','Travel expenses-Maintenance','GASTOS VIAJE MANUTENCION' union all
select 'R170903','Travel expenses-Accommodation','GASTOS VIAJE ALOJAMIENTO' union all
select 'R170904','Travel expenses-Business incentives','VIAJES POR INCENTIVOS' union all
select 'R170905','Travel expenses-Training','VIAJES FORMACION' union all
select 'R170906','Business promotion','PROMOCION DE NEGOCIO' union all
select 'R170907','Representative offices abroad','REPRESENTACIONES EN EL EXTRANJERO' union all
select 'R1710','External services contracted','Servicios externos contratados' union all
select 'R171001','Subcontracting of resources / Sales force','SUBCONTRATACION DE RECURSOS FUERZA DE VENTAS' union all
select 'R171002','External services contracted. Rating agencies','SERV EXTERNOS CONTR AGENCIAS RATING' union all
select 'R171003','Other contracted external services','OTROS SERVICIOS EXTERNOS CONTR' union all
select 'R171004','Back office subcontracted external services','SERVICIOS EXTERNOS SUBCONTRATADOS BACK OFFICE' union all
select 'R171005','Call centre subcontracted external services','SERVICIOS EXTERNOS SUBCONTRATADOS CALL CENTER' union all
select 'R171006','Subcontracted management and administration services','SERVICIOS EXTERNOS SUBCONTRATADOS DE GESTION Y ADM' union all
select 'R1711','Other administrative expenses, rest','Otros gastos de administracion, resto' union all
select 'R171101','Subscriptions','SUSCRIPCIONES' union all
select 'R171102','Association fees','CUOTAS ASOCIACIONES' union all
select 'R171103','Association fees: Central bank regulatory payments','CUOTAS ASOCIACIONES PAGOS NORMATIVA BANCO CENTRAL' union all
select 'R171104','Donations','DONATIVOS' union all
select 'R171105','Market information sources','FUENTES DE INFORMACION DE MERCADO' union all
select 'R171106','Sources of commercial information','FUENTES DE INFORMACION COMERCIAL' union all
select 'R171107','Governing and control bodies','POR ORGANOS DE GOBIERNO Y CONTROL' union all
select 'R171108','Administrative costs. External pension funds_Other long term','Administrative costs. External pension funds_Other long term' union all
select 'R171109','Administrative costs. External pension funds_Pension and other post employment','Administrative costs. External pension funds_Pension and other post employment' union all
select 'R171110','Other administrative expenses, other','Resto de Otros gastos generales' union all
select 'R1712','Taxes and levies','Tributos' union all
select 'R171201','Real estate tax','IMPUESTO SOBRE INMUEBLES' union all
select 'R171202','Other taxes','Otros impuestos' union all
select 'R18','Depreciation','Deterioro' union all
select 'R19','Provisions or release','Provisiones o reversion de provisiones' union all
select 'R20','Impairment provision or release non-financial assets','Provision por deterioro o reversion de la provision por activos no financieros' union all
select 'R21','Negative goodwill','Fondo de comercio negativo' union all
select 'R22','Investments accounted for using the equity method','Inversiones en filiales, negocios conjuntos y asociadas por el metodo de la participacion' union all
select 'R23','Gains and losses on derecognition of non-financial instruments','Perdidas y ganancias en instrumentos no financieros dados de baja' union all
select 'R24','Tax','Impuestos' union all
select 'R25','Attributable to minority interest [non-controlling interests]','Atribuible a intereses minoritarios (participaciones no dominantes)';