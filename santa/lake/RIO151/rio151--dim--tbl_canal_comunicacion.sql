-- MKT02.TBL_CANAL_COMUNICACION definition

-- DDL generated by DBeaver
-- WARNING: It may differ from actual native database DDL

-- Drop table

-- DROP TABLE MKT02.TBL_CANAL_COMUNICACION;

CREATE TABLE MKT02.TBL_CANAL_COMUNICACION (
	CD_CANAL_COMUNICACION VARCHAR2(2),
	DS_CANAL_COMUNICACION VARCHAR2(50)
);
CREATE UNIQUE INDEX PK_CANAL_COMUNICACION ON MKT02.TBL_CANAL_COMUNICACION (CD_CANAL_COMUNICACION);