CREATE TABLE GEC01.TIPO_MEDIOS
(
  COD_ENTIDAD        CHAR(4 BYTE)               NOT NULL,
  TPO_MEDIO          NUMBER(5)                  NOT NULL,
  DESC_MEDIO         VARCHAR2(30 BYTE)          NOT NULL,
  DESC_DETALL_MEDIO  VARCHAR2(1000 BYTE)        NOT NULL,
  INDI_TPO_MEDIO     CHAR(1 BYTE)               NOT NULL,
  EST_MEDIO          CHAR(1 BYTE)               NOT NULL,
  USER_ALT_MED       VARCHAR2(8 BYTE)           NOT NULL,
  FEC_ALT_MED        DATE                       NOT NULL,
  USER_MODF_MED      VARCHAR2(8 BYTE)           NOT NULL,
  FEC_MODF_MED       DATE                       NOT NULL,
  SECTOR_OWNER       CHAR(4 BYTE)               NOT NULL,
  INDI_VISIBLE       CHAR(1 CHAR)               DEFAULT 'S'                   NOT NULL,
  INDI_MSJ_ASOC      CHAR(1 BYTE)
)