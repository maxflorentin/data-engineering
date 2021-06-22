CREATE TABLE GEC01.CIRC_INFO_REQUE
(
  COD_ENTIDAD                CHAR(4 BYTE)       NOT NULL,
  IDE_CIRCUITO               NUMBER(5)          NOT NULL,
  INDI_INFO_GPO              CHAR(1 BYTE)       NOT NULL,
  COD_INFO_REQUE             NUMBER(5)          NOT NULL,
  INDI_INFO_OBLIG            CHAR(1 BYTE)       NOT NULL,
  EST_CIRC_INFO_REQUE        CHAR(1 BYTE)       NOT NULL,
  USER_ALT_CIRC_INFO_REQUE   CHAR(8 BYTE)       NOT NULL,
  FEC_ALT_CIRC_INFO_REQUE    DATE               NOT NULL,
  USER_MODF_CIRC_INFO_REQUE  CHAR(8 BYTE)       NOT NULL,
  FEC_MODF_CIRC_INFO_REQUE   DATE               NOT NULL,
  ORDEN_INFO                 NUMBER(2),
  COD_ETAPA                  NUMBER(2)          DEFAULT 1                     NOT NULL,
  ORDEN_ETAPA                NUMBER(2)          DEFAULT 1                     NOT NULL
)