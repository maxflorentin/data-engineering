SET mapred.job.queue.name=root.dataeng;

insert overwrite table bi_corp_nuevo_cargabal.master_classified_as_held_for_sale
select 'PR01','On demand [call] and short notice [current account]','A la vista y con breve plazo de preaviso' union all
select 'PR02','Reverse repurchase loans','Adquisición temporal de préstamos' union all
select 'PR0201','Money Market Transactions Through Central Counterparty - (reverse repo)','Transacciones del mercado monetario a través de una contraparte central - (reverse repo)' union all
select 'PR0202','Other Reverse repurchase loans','Otra adquisición temporal de préstamos' union all
select 'PR03','Term loans','Préstamos a plazo' union all
select 'PR0301','Credit card debt','Deuda por tarjetas de crédito' union all
select 'PR0302','Finance leases','Arrendamientos financieros' union all
select 'PR0303','Other Term loans','Otros préstamos a plazo' union all
select 'PR0304','Trade receivables','Cartera comercial' union all
select 'PR04','Advances that are not loans','Anticipos que no son préstamos' union all
select 'PR0401','Accounts receivable - tax consolidable group','Grupo Consolidable Tributario' union all
select 'PR0402','Dividends receivable','Dividendos a cobrar' union all
select 'PR0403','Guarantees','Fianzas' union all
select 'PR0404','Intergroup VAT','Iva intergrupo' union all
select 'PR0405','Other advances','Resto de anticipos' union all
select 'PR0406','Commissions Financial guarantees given ','Comisiones por Garantías financieras' union all
select 'PR05','Asset-backed securities','Asset-backed securities' union all
select 'PR0501','Mortgage asset backed securities','Bonos de titulizacion hipotecaria' union all
select 'PR0502','Other assets backed securities','otros bonos de titulización' union all
select 'PR06','Covered bonds','Bonos Garantizados' union all
select 'PR0601','Cedulas Hipotecarias','Cedulas Hipotecarias' union all
select 'PR0602','Cedulas Territoriales','Cedulas Territoriales' union all
select 'PR0603','Mortgage Covered Bonds','Mortgage Covered Bonds' union all
select 'PR0604','Non mortgage Covered Bonds','Non mortgage Covered Bonds' union all
select 'PR07','Other debt securities acquired','Otra títulos de deuda adquiridos' union all
select 'PR08','Credit default swap','Credit default swap' union all
select 'PR09','Credit spread option','Opción de spread de crédito' union all
select 'PR10','Forward rate agreements','Acuerdos de tipos futuros' union all
select 'PR11','Forwards','Futuros' union all
select 'PR12','Option','Opciones' union all
select 'PR13','Other derivatives','Otros derivados' union all
select 'PR14','Swaps','Swaps' union all
select 'PR15','Total return swap','Swap de retorno total' union all
select 'PR16','Mutual funds shares','Participaciones en Fondos de Inversion' union all
select 'PR17','Other equity instruments','Otros instrumentos de patrimonio' union all
select 'PR18','Preferred and common shares and irreversible capital contributions','Acciones preferentes y ordinarias y aportaciones irrevocables de capital' union all
select 'PR19','Asset-backed securities repurchased','Bonos de titulizacion recomprados' union all
select 'PR20','Demand deposits','Depósitos a la vista' union all
select 'PR2001','Current accounts','Cuentas corrientes' union all
select 'PR2002','Other demand deposits','Otros depósitos a la vista' union all
select 'PR2003','Saving accounts','Cuentas de ahorro' union all
select 'PR21','Guarantees received','Fianzas recibidas' union all
select 'PR22','Repurchase agreements','Acuerdos de recompra' union all
select 'PR2201','Money Market Transactions Through Central Counterparty - (repo)','Transacciones del mercado monetario a través de una contraparte central - (repo)' union all
select 'PR2202','Other Repurchase agreements','Otros acuerdos de recompra' union all
select 'PR23','Time deposits','Time deposits' union all
select 'PR2301','Fixed-term deposits','Depósitos a plazo fijos' union all
select 'PR2302','Redeemable at notice','Depósitos disponibles con preaviso' union all
select 'PR2303','Overnight deposits','Depositos overnight' union all
select 'PR2304','Deposits taken at discount','Depósitos con descuento' union all
select 'PR2305','Hybrid deposits','Depósitos híbridos' union all
select 'PR2306','other term deposits','Otros depósitos a plazo' union all
select 'PR24','Units issued (gross) - Securitization funds','Participaciones emitidas brutas   Titulizaciones' union all
select 'PR25','Certificates of deposits','Certificados de depósitos' union all
select 'PR26','Hybrid contracts','Contratos hibridos' union all
select 'PR27','Other Bonds','Otros bonos' union all
select 'PR2701','Convertible compound financial instruments Other Bonds','Intrumentos financieros convertibles contingentes. Otros bonos' union all
select 'PR2702','Non convertible Other Bonds','No convertibles - otros bonos' union all
select 'PR28','Other promissory notes and bills','Otros pagarés y efectos' union all
select 'PR29','Preference shares','Acciones preferentes' union all
select 'PR30','Preferred securities','Participaciones preferentes' union all
select 'PR3001','Convertible compound financial instruments Preferred securities','Intrumentos financieros convertibles contingentes. Participaciones preferentes' union all
select 'PR3002','Non convertible Preferred securities','Participaciones preferentes no convertibles' union all
select 'PR31','From reverse repurchase','From reverse repurchase' union all
select 'PR32','From securities borrowing','From securities borrowing' union all
select 'PR33','Accounts payable Consolidated tax group','Cuentas a pagar grupo consolidado tributario' union all
select 'PR34','Accrual accounts-Financial Guarantees','Cuentas de periodif por garantias financieras' union all
select 'PR35','Clearing houses','Camaras o sistemas de compensacion bancaria' union all
select 'PR36','Collection accounts Public authorities, OPF','Cuentas de recaudacion AAPP  OPF' union all
select 'PR37','Dividends payable','Dividendos a pagar' union all
select 'PR38','Economic group: suppliers','Cias. Grupo Economico: proveedores' union all
select 'PR39','Factoring payables','Acreedores por factoring' union all
select 'PR40','Financial Lease liabilities','Pasivos por arrendamientos financieros' union all
select 'PR41','Group entities creditors-VAT (balances to be offset or reimbursed)','Acreed. soc.grupo IVA (saldos a comp. o devolver)' union all
select 'PR42','Non Financial Lease liabilities','Pasivos por arrendamientos no financieros' union all
select 'PR43','Other financial liabilities, rest','Otros pasivos financieros  resto' union all
select 'PR44','Other payment obligations','Otras obligaciones a pagar' union all
select 'PR45','Special accounts (unsettled)','Cuentas especiales (pendientes de liquidar)' union all
select 'PR46','Trade payables','Acreedores comerciales' union all
select 'PR47','Credit derivatives','derivados de credito' union all
select 'PR48','Financial guarantees','Avales financieros' union all
select 'PR49','Commitments for placing and underwriting of securities','Compromisos de colocacion y suscripcion de valores' union all
select 'PR50','Documentary credits','Creditos documentarios' union all
select 'PR51','Documents delivered to clearing house','Documentos entregados a camara' union all
select 'PR52','Financial asset forward purchase commitments','Compromisos de compra a plazo de AF' union all
select 'PR53','Other contingent commitments','Resto compromisos contingentes' union all
select 'PR54','Other non-financial guarantees','Otras garantias no financieras' union all
select 'PR55','Purchase of annotated debt','Compra de deuda anotada' union all
select 'PR56','Purchase of other financial assets','Compra otros activos financieros' union all
select 'PR57','Regular way purchases of financial assets','Contratos convencionales de compras activos fros' union all
select 'PR58','Technical guarantees','Avales tecnicos' union all
select 'PR59','Rest Other commitments received','Resto otros compromisos recibidos' union all
select 'PR60','Spot sale of pending debt by the book-entry','Venta al contado de deuda anotada pendiente' union all
select 'PR61','Sale of other financial instruments','Venta de otros instrumentos financieros' ;