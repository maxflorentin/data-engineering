CREATE TABLE GEC01.PRODUCTO
(
  COD_ENTIDAD     CHAR(4 BYTE)                  NOT NULL,
  COD_PROD        NUMBER(4)                     NOT NULL,
  DESC_PROD       VARCHAR2(80 BYTE)             NOT NULL,
  EST_PROD        CHAR(1 BYTE)                  NOT NULL,
  USER_ALT_PROD   VARCHAR2(8 BYTE)              NOT NULL,
  FEC_ALT_PROD    DATE                          NOT NULL,
  USER_MODF_PROD  VARCHAR2(8 BYTE)              NOT NULL,
  FEC_MODF_PROD   DATE                          NOT NULL
)