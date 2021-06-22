CREATE TABLE GEC01.SUBPRODUCTO
(
  COD_ENTIDAD        CHAR(4 BYTE)               NOT NULL,
  COD_SUBPROD        NUMBER(4)                  NOT NULL,
  DESC_SUBPROD       VARCHAR2(80 BYTE)          NOT NULL,
  EST_SUBPROD        CHAR(1 BYTE)               NOT NULL,
  USER_ALT_SUBPROD   VARCHAR2(8 BYTE)           NOT NULL,
  FEC_ALT_SUBPROD    DATE                       NOT NULL,
  USER_MODF_SUBPROD  VARCHAR2(8 BYTE)           NOT NULL,
  FEC_MODF_SUBPROD   DATE                       NOT NULL
)