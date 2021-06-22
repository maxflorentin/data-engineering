CREATE TABLE GEC01.GC_INDIVIDUO_GESTIONES
(
  COD_ENTIDAD         CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_SECTOR  CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_NRO     NUMBER(8)                 NOT NULL,
  TPO_DOC_INDI        CHAR(1 BYTE)              NOT NULL,
  NRO_DOC_INDI        NUMBER(11)                NOT NULL,
  FEC_NACI_INDI       DATE                      NOT NULL,
  MAR_SEX_INDI        CHAR(1 BYTE)              NOT NULL
)