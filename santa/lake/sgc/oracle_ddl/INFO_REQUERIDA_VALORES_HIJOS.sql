CREATE TABLE GEC01.INFO_REQUERIDA_VALORES_HIJOS
(
  COD_ENTIDAD       CHAR(4 BYTE)                NOT NULL,
  COD_INFO_REQUE_P  NUMBER(5)                   NOT NULL,
  COD_VALOR_P       VARCHAR2(5 BYTE),
  NRO_VALOR_P       NUMBER(5),
  COD_INFO_REQUE_H  NUMBER(5)                   NOT NULL,
  COD_VALOR_H       VARCHAR2(5 BYTE),
  NRO_VALOR_H       NUMBER(5),
  DESC_VALOR        VARCHAR2(80 BYTE)           NOT NULL,
  EST_VALOR         CHAR(1 BYTE)                NOT NULL,
  USER_ALT_VALOR    VARCHAR2(8 BYTE)            NOT NULL,
  FEC_ALT_VALOR     DATE                        NOT NULL,
  USER_MODF_VALOR   VARCHAR2(8 BYTE)            NOT NULL,
  FEC_MODF_VALOR    DATE                        NOT NULL,
  COD_VALOR_INFO_H  VARCHAR2(5 BYTE),
  COD_VALOR_INFO_P  VARCHAR2(5 BYTE)
)