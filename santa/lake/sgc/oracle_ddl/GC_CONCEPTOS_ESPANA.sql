CREATE TABLE GEC01.GC_CONCEPTOS_ESPANA
(
  COD_ENTIDAD       CHAR(4 BYTE)                NOT NULL,
  COD_CPTO          NUMBER(4)                   NOT NULL,
  DESC_CPTO         VARCHAR2(255 BYTE)          NOT NULL,
  COD_TIPO_RECLAMO  NUMBER(3)                   NOT NULL,
  USER_ALT          VARCHAR2(8 BYTE)            DEFAULT 'VINICIAL'            NOT NULL,
  FEC_ALT           DATE                        DEFAULT SYSDATE               NOT NULL,
  USER_MODF         VARCHAR2(8 BYTE)            DEFAULT 'VINICIAL'            NOT NULL,
  FEC_MODF          DATE                        DEFAULT SYSDATE               NOT NULL,
  ESTADO            VARCHAR2(1 BYTE)            DEFAULT 'A'
)