set mapred.job.queue.name=root.dataeng; 
INSERT OVERWRITE TABLE bi_corp_staging.afir_cod_causales
SELECT stack
       (
           	49
          	,'001', 'EN CONCURSO PREVENTIVO'
			,'002', 'PEDIDO DE PROPIA QUIEBRA'
			,'003', 'PEDIDO DE QUIEBRA POR TERCEROS'
			,'004', 'NEGATIVO GETNET'
			,'005', 'INHABILITADO MONEDA EXTRANJERA'
			,'014', 'COM A 6804-INHAB BCRA U$S 1000'
			,'015', 'INHABIL PARA OPERAR EN CAMBIOS'
			,'016', 'EXC COM 88216-INHAB VENTA U$S'
			,'017', 'MAPA SEGUIMIENTO-PYMES'
			,'018', 'MAPA DE SEGUIMIENTO'
			,'019', 'CLIENTES EXPAS'
			,'020', 'PEDIDO RECHAZADO QUIEBRA'
			,'021', 'HOMOLOGACION JUDICIAL'
			,'022', 'COM A6804-INHAB BCRA U$S 10000'
			,'024', 'INHAB.LIBRAM.CHEQ. SIN FONDOS'
			,'027', 'INHABILITADO P/ORDEN JUDICIAL'
			,'029', 'ORDEN JUDICIAL PRODUCTOS'
			,'030', 'INHAB.LIB.CHEQ.S/FDOS<01/07/955'
			,'032', 'INHAB.LIB.CHEQ.S/FDOS INSC.INEX'
			,'039', 'FIADOR/AVALISTA DE TI EN MORA'
			,'040', 'CLTES DE CRED. EN GEST. Y MORAA'
			,'042', 'CLTES PROD. MINOR. GEST.Y MORAA'
			,'043', 'CLTES PROD. MINOR. CON ATRASO O'
			,'044', 'CLTES CON COMISIONES INCOBRAB.'
			,'046', 'TITULAR CON CHEQUES RECHAZADOS'
			,'047', 'VALIDACION DE DATOS'
			,'048', 'RESPONSABLE DE QUEBRANTOS'
			,'049', 'USUARIO INDEBIDO DE TARJETA'
			,'050', 'DEUDOR INCOBRABLE'
			,'051', 'GERENCIA DE PERSONAL'
			,'055', 'PASAJE CTOS A GESTION DE COBRO'
			,'056', 'PASAJE CTOS A MORIA'
			,'060', 'ACCION JUDIC.INIC.POR EL BANCO'
			,'062', 'CLTE C/ARREGLO NO OPERA C/BCO.'
			,'063', 'QUIEBRA POR TERCEROS,LEVANTADA'
			,'065', 'GESTION Y MORA - PLAN DE PAGOS'
			,'071', 'PAGO MULTA INHAB.BCRA 97'
			,'072', 'VENC.INHAB.BCRA 97 - MULTAS'
			,'081', 'CLTES C/ANTEC.-ADMISION Y SEG.'
			,'083', 'UPBC - RESTRINGIDO            N'
			,'084', 'UPBC - MONITOREO'
			,'091', 'EMISION DE CHEQUES SIN FONDOS O'
			,'092', 'EMISION CHEQ.DEFECTOS FORMALESR'
			,'093', 'EMISION CHEQ. RECH. PAG/RESCATF'
			,'094', 'PROBLEMAS EN SIST. FINANCIERO'
			,'095', 'MULTA IMPAGA'
			,'096', 'PART. CTO. MULTA IMPAGA'
			,'097', 'MULTA IMPAGA BCRA'
			,'099', 'UPBC COINCIDE LIST TERRORISTAS'
       ) ;