-- MKT02.TBL_CALCULADOR_BENEFICIO definition

-- DDL generated by DBeaver
-- WARNING: It may differ from actual native database DDL

-- Drop table

-- DROP TABLE MKT02.TBL_CALCULADOR_BENEFICIO;

CREATE TABLE MKT02.TBL_CALCULADOR_BENEFICIO (
	CD_BENEFICIO NUMBER(38,0),
	CD_PROMOCION NUMBER(38,0),
	CD_BENEFICIO_MKT VARCHAR2(4),
	DS_BENEFICIO VARCHAR2(2000),
	DT_VIGENCIA_DESDE DATE,
	DT_VIGENCIA_HASTA DATE );
CREATE UNIQUE INDEX TBL_CALC_BENEFICIO_PK ON MKT02.TBL_CALCULADOR_BENEFICIO (CD_BENEFICIO);