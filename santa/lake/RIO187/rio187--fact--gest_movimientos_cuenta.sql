CREATE TABLE CSM.GEST_MOVIMIENTOS_CUENTA (
	ID_GESTION NUMBER(9,0),
	NRO_MOVIMIENTO NUMBER(3,0),
	SUCURSAL_CUENTA VARCHAR2(4),
	NRO_CUENTA VARCHAR2(12),
	DIVISA_CUENTA VARCHAR2(3),
	NRO_COMPROBANTE NUMBER(9,0),
	COD_OPERATIVO VARCHAR2(10),
	FECHA_MOVIMIENTO DATE,
	MONTO_MOVIMIENTO NUMBER(15,2),
	IND_DEVOLUCION CHAR(1) DEFAULT 'N' ,
	FECHA_DEVOLUCION DATE,
	MONTO_DEVOLUCION NUMBER(15,2),
	COD_RUBRO NUMBER(4,0),
	NRO_ESTABLECIMIENTO VARCHAR2(20),
	ID_USUARIO_ALTA VARCHAR2(8),
	FECHA_ALTA DATE,
	ID_USUARIO_MODIF VARCHAR2(8),
	FECHA_MODIF DATE,
	FECHA_BAJA DATE,
	NOMBRE_ESTABLECIMIENTO VARCHAR2(500),
	COD_AUTORIZACION VARCHAR2(6),
	NRO_CUPON VARCHAR2(9),
	NOMBRE_EMPRESA VARCHAR2(30),
	EMPRESA_B24 VARCHAR2(4),
	MONTO_RECLAMADO NUMBER(15,2),
	TASA_PROMEDIO NUMBER(13,2),
	MONTO_INTERES NUMBER(13,2),
	MONTO_COMISION NUMBER(13,2),
	ID_MOTIVO NUMBER(16,0),
	COD_OP_BRIO_GRUPO VARCHAR2(4),
	COD_OP_BRIO_SUBGRUPO VARCHAR2(4),
	NRO_TARJETA_DEBITO VARCHAR2(16),
	ID_ATM VARCHAR2(16),
	ID_VOUCHAR VARCHAR2(15),
	DIRECCION_ESTABLECIMIENTO VARCHAR2(500),
	COD_BRIO_DEVOLUCION VARCHAR2(16))