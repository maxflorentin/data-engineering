CREATE TABLE CSM.GEST_ESTADOS (
	ID_GESTION NUMBER(9,0),
	ORDEN_ESTADO NUMBER(4,0),
	ID_ESTADO VARCHAR2(4),
	FECHA_ESTADO DATE,
	ID_ACTOR CHAR(4),
	ID_USUARIO VARCHAR2(8),
	COMENTARIO VARCHAR2(4000),
	ID_USUARIO_ALTA VARCHAR2(8),
	FECHA_ALTA DATE,
	ID_USUARIO_MODIF VARCHAR2(8),
	FECHA_MODIF DATE,
	FECHA_BAJA DATE)