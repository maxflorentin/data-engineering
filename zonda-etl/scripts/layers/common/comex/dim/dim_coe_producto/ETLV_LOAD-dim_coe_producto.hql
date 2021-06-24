"
SET mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE BI_CORP_COMMON.DIM_COE_PRODUCTO VALUES
 (50,'AVAE','Avales de Exportación')
,(50,'AVAI','Avales de Importación')
,(50,'CDE','Carta de Crédito Exportación')
,(50,'CDEC','Carta de Crédito Exportación Confirmada')
,(50,'CDI','Carta de Crédito Importación')
,(50,'CHEQ','Cheques Locales')
,(50,'COBE','Cobranza Documento de Exportación')
,(50,'COBI','Cobranza Documento de Importación')
,(50,'EXCL','Forfaitings Vencidos')
,(50,'FINE','Financiación de Exportación')
,(50,'FORF','Forfaiting')
,(50,'OPE','Órdenes de Pago de Exportación')
,(50,'OPS','Órdenes de Pago de Servicio')
,(50,'PREF','Préstamo Exportación')
,(50,'PREI','Prestamos Importación')
,(50,'STBC','StandBy Exportación Confirmada')
,(50,'STBM','StandBy Importación Confirmada')
,(50,'STBX','StandBy Exportación Avisada')
,(50,'TTA','Transferencia Anticipada de Importación')
,(50,'TTI','Pago Anticipado de Importación')
,(50,'TTS','Transferencias de Fondos al Exterior');

"