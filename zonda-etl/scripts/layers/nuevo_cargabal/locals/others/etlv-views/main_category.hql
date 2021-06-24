SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_main_category
select 'MC01','Cash on hand','Efectivo' union all
select 'MC02','Loans and advances','Prestamos y anticipos' union all
select 'MC03','Credit investment in securitization vehicles','Inversion crediticia en activo de vehiculos de titulizacion' union all
select 'MC04','Debt securities acquired','Valores representativos de deuda adquirida' union all
select 'MC05','Derivatives','Derivados' union all
select 'MC06','Equity instruments','Instrumentos de patrimonio' union all
select 'MC07','Fair value changes of the hedged items in portfolio hedge of interest rate risk - asset','Cambios del Valor razonable de los elementos cubiertos de una cartera con cobertura del riesgo de tipo de interes. Activo' union all
select 'MC08','Investments in subsidiaries, joint ventures and associates','Inversiones en filiales, negocios conjuntos y asociadas' union all
select 'MC0801','Associates','Entidades asociadas' union all
select 'MC0802','Joint ventures','Negocios Conjuntos' union all
select 'MC0803','Subsidiaries','Grupo' union all
select 'MC09','Insurance and reinsurance contracts - assets','Activos creados por contratos de seguro o de reaseguro' union all
select 'MC0901','Deposits placed for reinsurance assumed','Depósitos constituidos por reaseguros aceptados' union all
select 'MC0902','Other credits from insurance operations','Resto de créditos por operaciones de seguros' union all
select 'MC0903','Technical provisions - assets','Technical provisions - assets' union all
select 'MC10','Tangible assets','Activos tangibles' union all
select 'MC1001','Computer hardware and related facilities','Equipos para procesos de informacion (Hardware)' union all
select 'MC1002','Construction in progress','Obras en curso' union all
select 'MC1003','Furniture, fixtures and vehicles','Mobiliario  vehiculos y resto de instalaciones' union all
select 'MC1004','Other tangible assets','Resto Tangibles' union all
select 'MC1005','Real estate','Edificios y terrenos' union all
select 'MC100501','Buildings','Edificios' union all
select 'MC100502','Land','Terrenos' union all
select 'MC11','Intangible assets','Activos intangibles' union all
select 'MC1101','Brand names','Marcas Comerciales' union all
select 'MC1102','Credit cards','Tarjetas de credito' union all
select 'MC1103','Customer lists','Listas de clientes' union all
select 'MC1104','Goodwill','Fondo de comercio' union all
select 'MC1105','IT developments','Desarrollos informaticos' union all
select 'MC1106','Other intangible assets','Resto Intangibles' union all
select 'MC1107','Stable funding with low remuneration','Financiacion estable con baja remuneracion' union all
select 'MC12','Tax assets','Activos por impuestos' union all
select 'MC1201','Current tax assets','Activos por impuestos corrientes' union all
select 'MC120101','Withholding corporate tax','Retención en origen Pago a cta impuesto sociedad' union all
select 'MC120102','Rest','Resto' union all
select 'MC1202','Deferred tax assets','Activos por impuestos diferidos' union all
select 'MC120201','Monetizable','Monetizable' union all
select 'MC120202','Other deferred tax assets','Impuesto sobre beneficio anticipado Resto' union all
select 'MC120203','Tax assets due to unrealized losses recorded in reserves','Activo fiscal por minusvalias reconocidas en reservas' union all
select 'MC120204','Tax deductions and credits pending','Credito Perdidas a compensar ejercicios anteriores' union all
select 'MC120205','Tax loss carryforwards','Deducciones o bonificaciones fiscales pendientes de compensar' union all
select 'MC13','Other assets','Otros activos' union all
select 'MC1301','Accrual - Other assets','Periodificaciones - Otros Activos' union all
select 'MC1302','Insurance contracts linked to pensions','Contratos de seguros vinculados a pensiones' union all
select 'MC1303','Inventory','Existencias' union all
select 'MC1304','Net assets in pension plans','Activos netos en planes de pensiones' union all
select 'MC1305','Rest other assets','Otros activos' union all
select 'MC1306','Provisions for securitisation funds','Importes provisionados por los FTA' union all
select 'MC1307','Transactions in transit - Other assets','Operaciones en camino - Otros Activos' union all
select 'MC14','Deposits','Depositos' union all
select 'MC15','Debt securities issued','Valores representativos de deuda emitidos' union all
select 'MC16','Short positions','Posiciones cortas' union all
select 'MC1601','Short positions - Debt Securities','Posiciones cortas en valores representativos de deuda' union all
select 'MC1602','Short positions - Equity instruments','Posiciones cortas en instrumentos de patrimonio' union all
select 'MC17','Other financial liabilities','Otros pasivos financieros' union all
select 'MC18','Fair value changes of the hedged items in portfolio hedge of interest rate risk - liabilities','Cambios del Valor razonable de los elementos cubiertos de una cartera con cobertura del riesgo de tipo de interes. Pasivo' union all
select 'MC19','Insurance and reinsurance contracts - liabilities','Pasivos creados por contratos de seguro o de reaseguro' union all
select 'MC1901','Debts for insurance and reinsurance transactions','Debts for insurance and reinsurance transactions' union all
select 'MC1902','Deposits received reinsurance given','Deposits received reinsurance given' union all
select 'MC1903','Provisions for payments of settlement agreement','Provisions for payments of settlement agreement' union all
select 'MC1904','Technical provisions - liabilities','Technical provisions - liabilities' union all
select 'MC20','Provisions','Provisiones' union all
select 'MC2001','Financial guarantees given_Provisions','Garantías financieras recibidas_Provisiones' union all
select 'MC2002','Loan commitments given_Provisions','Compromisos de préstamo recibidos_Provisiones' union all
select 'MC2003','Other commitments given_Provisions','Otros compromisos recibidos_Provisiones' union all
select 'MC2004','Employee benefits. Other long term benefit obligations','Beneficios para empleados. Otras retribuciones a largo plazo' union all
select 'MC2005','Employee benefits. Pension and other post-employment benefit obligations','Beneficios para empleados. Pensiones y otras obligaciones post-empleo' union all
select 'MC2006','Restructuring','Restructuración' union all
select 'MC2007','Pending legal issues','Contingencias civiles' union all
select 'MC2008','Tax litigation','Litigios fiscales' union all
select 'MC2009','Payment commitments to deposit guarantee schemes','Compromisos de pago al fondo de garantía de depósitos de entidades de crédito' union all
select 'MC2010','Payment commitments to resolution funds','Compromisos de pago al fondo único de resolución' union all
select 'MC2011','Other provisions','Otras provisiones' union all
select 'MC21','Tax liabilities','Pasivos por impuestos' union all
select 'MC2101','Current tax liabilities','Pasivos por impuestos corrientes' union all
select 'MC210101','Corporate tax- Current period','Ejercicio actual' union all
select 'MC210102','Corporate tax- Previous periods','Ejercicios anteriores' union all
select 'MC210103','Other taxes','Otros impuestos' union all
select 'MC2102','Deferred tax liabilities','Pasivos por impuestos diferidos' union all
select 'MC210201','Deferred income tax','Impuesto sobre beneficio diferido' union all
select 'MC210202','Tax liabilities for unrealized gains recorded in reserves','Pasivo fiscal por minusvalias reconocidas en reservas' union all
select 'MC22','Other liabilities','Otros pasivos' union all
select 'MC2201','Accrual - Other liabilities','Periodificaciones - Otros Pasivos' union all
select 'MC2202','Transactions in transit - Other liabilities','Operaciones en camino - Otros Pasivos' union all
select 'MC2203','Rest of other liabilities','Otros pasivos' union all
select 'MC23','Capital','Capital' union all
select 'MC2301','Paid up capital','Capital desembolsado' union all
select 'MC2302','Unpaid capital which has been called up','Capital no desembolsado exigido' union all
select 'MC24','Share premium','Prima de emision' union all
select 'MC25','Partners contributions other than capital','Aportaciones de los socios distintas del capital' union all
select 'MC26','Equity instruments issued other than capital','Instrumentos de patrimonio emitidos distintos de capital' union all
select 'MC2601','Equity component of compound financial instruments','Componente de patrimonio neto de los instrumentos financieros compuestos' union all
select 'MC2602','Other equity instruments issued','Otros instrumentos de patrimonio emitidos' union all
select 'MC27','Other equity','Otros elementos de patrimonio neto' union all
select 'MC2701','Compensation based on equity instruments','Remuneraciones basadas en instrumentos de patrimonio' union all
select 'MC28','Retained earnings','Ganancias' union all
select 'MC29','Other reserves','Otras reservas' union all
select 'MC2901','Revaluation reserves','Reservas de revaloricacion' union all
select 'MC2902','Rest of other reserves','Otros reservas' union all
select 'MC30','(-) Treasury shares','Acciones propias' union all
select 'MC31','(-) Interim Dividends','Dividendos a cuenta' union all
select 'MC32','Actuarial gains or loss on defined benefit pension plans','Perdidas y ganancias actuariales en beneficios definidos y planes de pensiones' union all
select 'MC33','Fair value changes of equity instruments measured at fair value through other comprehensive income','Variaciones de valor razonable de los instrumentos de patrimonio a valor razonable con cambios en otro resultado global' union all
select 'MC34','Hedge ineffectiveness of fair value hedges for equity instruments measured at fair value through other comprehensive income','Ineficacia de las coberturas de valor razonable de los instrumentos de patrimonio valorados a valor razonable con cambios en otro resultado global' union all
select 'MC3401','Fair value changes of equity instruments measured at fair value through other comprehensive income [hedged item]','Variaciones de valor razonable de los instrumentos de patrimonio a valor razonable con cambios en otro resultado global [elemento cubierto]' union all
select 'MC3402','Fair value changes of equity instruments measured at fair value through other comprehensive income [hedging instrument]','Variaciones de valor razonable de los instrumentos de patrimonio a valor razonable con cambios en otro resultado global [instrumento de cobertura]' union all
select 'MC35','Fair value changes of financial liabilities at fair value through profit or loss attributable to changes in their credit risk','Cambios del valor razonable de los pasivos financieros a valor razonable con cambios en resultados atribui­bles a cambios en el riesgo de credito' union all
select 'MC3501','Deposits (FV OCI)','Depósitos (FV OCI)' union all
select 'MC3502','Debt securities issued (FV OCI)','Valores representativos de deuda emitidos (FV OCI)' union all
select 'MC3503','Other financial liabilities (FV OCI)','Otros pasivos financieros (FV OCI)' union all
select 'MC36','Hedges of net investments in foreign operations [effective portion]','Cobertura de inversiones netas en negocios en el extranjero [parte eficaz]' union all
select 'MC37','Foreign currency translation','Conversion de divisas' union all
select 'MC38','Hedging derivatives. Cash flow hedges reserve (effective portion)','Derivados de cobertura. Reserva de cobertura de flujos de efectivo [parte eficaz]' union all
select 'MC39','Fair value changes of debt instruments measured at fair value through other comprehensive income','Cambios del valor razonable de los instrumentos de deuda valorados a valor razonable con cambios en otro resultado global' union all
select 'MC3901','Loans and advances (FV OCI)','Prestamos y anticipos (FV OCI)' union all
select 'MC3902','Debt securities acquired (FV OCI)','Valores representativos de deuda adquirida (FV OCI)' union all
select 'MC40','Hedging instruments [not designated elements]','Instrumentos de cobertura [elementos no designados]' union all
select 'MC41','Financial guarantees given','Garantias financieras concedidas' union all
select 'MC42','Loan commitments given','Compromisos de prestamo concedidos' union all
select 'MC43','Other commitments given','Otros compromisos concedidos' union all
select 'MC4301','Non-financial guarantees given','Garantias no financieras entregadas' union all
select 'MC4302','Other commintments given, rest','Otros compromisos entregados, resto' union all
select 'MC44','Pensions','Pensiones' union all
select 'MC4401','Assets not recognised as assets','Activos no reconocidos como activos' union all
select 'MC4402','Defined benefit plans assets','Planes de prestación definidos' union all
select 'MC4403','Employee benefits. Other long term benefit obligations_pensions','Beneficios para empleados. Otras retribuciones a largo plazo_pensiones' union all
select 'MC4404','Employee benefits. Pension and other post-employment benefit obligations_pensions','Beneficios para empleados. Pensiones y otras obligaciones post-empleo_pensiones' union all
select 'MC45','Assets involved in the services provided','Activos implicados en los servicios prestados' union all
select 'MC46','Debt securities received on loan','Valores representativos de deuda recibidos en prestamo' union all
select 'MC47','Equity instruments received on loan','Instrumentos de patrimonio recibidos en préstamo' union all
select 'MC48','Financial guarantees received','Garantias financieras recibidas' union all
select 'MC49','Loan commitments received','Compromisos de prestamo recibidos' union all
select 'MC50','Other Commitments received','Otros compromisos recibidos' union all
select 'MC51','Tax assets non activated','Activos fiscales no activados' union all
select 'MC52','Other Service provided by payments and recoveries','Otros servicios por servicios de pago y recuperaciones' union all
select 'MC53','Securities services','Servicios de valores' union all
select 'MC54','Other services','Otros servicios' union all
select 'MC55','Gains and losses on derecognition of business','Pérdidas y ganancias por baja de negocios';
