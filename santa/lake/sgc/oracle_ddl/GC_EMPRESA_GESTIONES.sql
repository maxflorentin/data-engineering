CREATE TABLE GEC01.GC_EMPRESA_GESTIONES
(
  COD_ENTIDAD         CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_SECTOR  CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_NRO     NUMBER(8)                 NOT NULL,
  TPO_DOC_EMPR        CHAR(1 BYTE)              NOT NULL,
  NRO_DOC_EMPR        NUMBER(11)                NOT NULL,
  SEC_DOC_EMPR        NUMBER(2)                 NOT NULL
)