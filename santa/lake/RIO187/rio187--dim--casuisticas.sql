CREATE TABLE CSM.CASUISTICAS (
	ID_CASUISTICA NUMBER(6,0),
	DESC_CASUISTICA VARCHAR2(60),
	ID_TIPO_GESTION NUMBER(1,0),
	ID_TIPOLOGIA VARCHAR2(3),
	CANT_MESES_PRIMER_RECLAMO NUMBER(2,0),
	CANT_DIAS_RESOLUCION NUMBER(3,0),
	TIPO_PRODUCTO CHAR(1),
	ID_SECTOR_SOPORTE CHAR(4),
	HABILITADO VARCHAR2(1),
	ID_USUARIO_ALTA VARCHAR2(8),
	FECHA_ALTA DATE,
	ID_USUARIO_MODIF VARCHAR2(8),
	FECHA_MODIF DATE,
	TEXTO_MAIL_CASUISTICA VARCHAR2(100),
	IND_INTERES_COMP VARCHAR2(1),
	FECHA_BAJA DATE,
	IND_SOLO_ACTIVAS VARCHAR2(1),
	DIAS_WARNING_BCRA NUMBER(2,0) DEFAULT 10 ,
	ID_STATE VARCHAR2(100),
	SINONIMOS VARCHAR2(500),
	NOMBRE_ATAJOS VARCHAR2(100),
	DESC_ATAJOS VARCHAR2(500),
	SECCIONES VARCHAR2(2),
	MONTO_DESDE_SUCU VARCHAR2(30),
	MONTO_DESDE_ZONA VARCHAR2(30),
	CANT_TOPE_MES NUMBER(3,0),
	GESTIONES_SIMULTANEAS VARCHAR2(1))