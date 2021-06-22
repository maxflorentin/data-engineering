CREATE TABLE GEC01.CONCEPTO
(
  COD_ENTIDAD       CHAR(4 BYTE)                NOT NULL,
  COD_CPTO          NUMBER(4)                   NOT NULL,
  DESC_CPTO         VARCHAR2(80 BYTE)           NOT NULL,
  DESC_DETALL_CPTO  VARCHAR2(1000 BYTE),
  EST_CPTO          CHAR(1 BYTE)                NOT NULL,
  USER_ALT_CPTO     VARCHAR2(8 BYTE)            NOT NULL,
  FEC_ALT_CPTO      DATE                        NOT NULL,
  USER_MODF_CPTO    VARCHAR2(8 BYTE)            NOT NULL,
  FEC_MODF_CPTO     DATE                        NOT NULL,
  SECTOR_OWNER      CHAR(4 BYTE)                NOT NULL
)