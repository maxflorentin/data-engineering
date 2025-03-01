set mapred.job.queue.name=root.dataeng; 
INSERT 	overwrite TABLE bi_corp_staging.rio151_param_descripciones
SELECT stack
       ( 53
       	,'EXCESO_DE_DESCUBIERTO_MAYOR_30', 1, 'ALERTA'
		,'EN_CAMARA_RECHAZO_CHEQUE_CANJE_INTERNO', 10, 'ALERTA'
		,'SERVICIOS_VENCIMIENTO_PF', 13, 'ALERTA'
		,'SERVICIOS_COMEX', 14, 'ALERTA'
		,'SERVICIOS_FIRMANTE_SIN_REGLA', 15, 'ALERTA'
		,'ALERTS_GENERICAS_AVISOS_IMPORTANTES', 1601, 'ALERTA'
		,'CALIFICACION_VENCIMIENTO_CALIFICACION', 19, 'ALERTA'
		,'STOCK_CHEQUE_RECHAZADO', 2, 'ALERTA'
		,'SERVICIOS_CHEQUES_LIMITE_SEGURIDAD', 22, 'ALERTA'
		,'SERVICIOS_ORDENES_PAGO', 23, 'ALERTA'
		,'STOCK_DEVOLUCION_CORREO', 25, 'ALERTA'
		,'SERVICIOS_CUMPLE_ANOS_SELECT', 26, 'ALERTA'
		,'SERVICIOS_MULTAS_PENDIENTES', 28, 'ALERTA'
		,'STOCK_CHEQUERA', 3, 'ALERTA'
		,'MORA_EXCESOS_DESCUBIERTO', 30, 'ALERTA'
		,'STOCK_TARJETAS_PREEMBOZADAS', 31, 'ALERTA'
		,'STOCK_CUMPLEANOS', 32, 'ALERTA'
		,'CALIFICACION_VENCIMIENTO_REVISION', 34, 'ALERTA'
		,'MORA_TEMPRANA', 35, 'ALERTA'
		,'CALIFICACION_VENCIMIENTO_ACUERDO', 38, 'ALERTA'
		,'MORA_TARDIA', 39, 'ALERTA'
		,'STOCK_TARJETA_Y_PAQUETES', 4, 'ALERTA'
		,'SERVICIOS_DATOS_FILIATORIOS_BCRA', 42, 'ALERTA'
		,'MORA_PRODUCTOS_MORA_TEMPRANA', 43, 'ALERTA'
		,'CALIFICACION_ACUERDOS_PROXIMO_VENCER', 44, 'ALERTA'
		,'CALIFICACION_REPACTO_TASA', 47, 'ALERTA'
		,'EN_CAMARA_RECHAZO_CHEQUE_CAMARA', 48, 'ALERTA'
		,'STOCK_TARJETAS_DISPONIBLES', 51, 'ALERTA'
		,'STOCK_TARJETAS_DISPONIBLES_PUNTO_VENTA', 52, 'ALERTA'
		,'STOCK_CHEQUERA_DISPONIBLE_AVISO', 53, 'ALERTA'
		,'STOCK_CHEQUE_RECHAZADO_AVISO', 54, 'ALERTA'
		,'STOCK_MULTA_PENDIENTE_CHEQUE', 55, 'ALERTA'
		,'STOCK_DEBITO_RECHAZADO', 56, 'ALERTA'
		,'STOCK_PIEZA_ANDREANI_A', 60, 'ALERTA'
		,'STOCK_PIEZA_ANDREANI_B', 61, 'ALERTA'
		,'FATCA_DATOS_FALTANTES', 63, 'ALERTA'
		,'FATCA_DOCUMENTACION_FALTANTE', 64, 'ALERTA'
		,'FATCA_SOLICITUD_VENCIMIENTO', 68, 'ALERTA'
		,'DEBITOS_RECHAZADOS', 7, 'ALERTA'
		,'SERVICIOS_CONTACTO_SELECT', 70, 'ALERTA'
		,'TB_BLOQUEADA', 74, 'ALERTA'
		,'BLOQUEO_AUTOM_CACS', 75, 'ALERTA'
		,'TARJETA_VISA_AMEX_INHABILITADA', 76, 'ALERTA'
		,'CAMPANA_VENCIMIENTO_SEGURO_PRESTAMOS', 8, 'ALERTA'
		,'MARCA_CLIENTE', 81, 'ALERTA'
		,'STOCK_CHEQUERA_PENDIENTE', 82, 'ALERTA'
		,'SERVICIOS_RECALIFICACION', 84, 'ALERTA'
		,'GIRO_CARTERA', 85, 'ALERTA'
		,'GALA', 86, 'ALERTA'
		,'MORA_BAJA', 999, 'ALERTA'
		,'GESTIONALO', 0, 'GESTION'
		,'LO_GESTIONE', 1, 'GESTION'
		,'EN_GESTION', 2, 'GESTION' ) ;