CREATE TABLE CNL01.MOVIMIENTO (
	MAQ_ID VARCHAR2(5),
	FECHA DATE,
	HORA VARCHAR2(6),
	TIPO_CLIENTE VARCHAR2(1),
	SUC_NRO VARCHAR2(3),
	SUC_MAQ VARCHAR2(3),
	CANAL_ID VARCHAR2(5),
	TX_ID VARCHAR2(6),
	ENTE_ID VARCHAR2(3),
	ORIG_REV VARCHAR2(1),
	FECHA_PROC DATE,
	NUP VARCHAR2(8),
	USUARIO VARCHAR2(8),
	CUADRANTE VARCHAR2(3),
	IMPORTE NUMBER(14,2),
	MONEDA VARCHAR2(3),
	CANTIDAD_CHEQUES VARCHAR2(3),
	PESUBSEG VARCHAR2(1),
	FECHA_CARGA DATE,
	MARCA_MAN_AUT VARCHAR2(1),
	IDE_PAGO VARCHAR2(19),
	CANTIDAD_EFECTIVO NUMBER(3,0),
	CANTIDAD_OTROS NUMBER(3,0),
	IMPORTE_CHEQUES NUMBER(14,2),
	IMPORTE_EFECTIVO NUMBER(14,2),
	IMPORTE_OTROS NUMBER(14,2),
	MEDIO_PAGO VARCHAR2(1)
);