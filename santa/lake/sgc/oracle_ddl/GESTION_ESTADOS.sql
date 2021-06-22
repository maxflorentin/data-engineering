CREATE TABLE GEC01.GESTION_ESTADOS
(
  COD_ENTIDAD           CHAR(4 BYTE)            NOT NULL,
  IDE_GESTION_SECTOR    CHAR(4 BYTE)            NOT NULL,
  IDE_GESTION_NRO       NUMBER(8)               NOT NULL,
  COD_EST_GEST          NUMBER(4)               NOT NULL,
  FEC_EST_GEST          DATE                    NOT NULL,
  TPO_MEDIO             NUMBER(5),
  COD_RTA_RESOL_PREDEF  NUMBER(5),
  COD_CONTC             NUMBER(5),
  COD_NIV_CONF          NUMBER(5),
  INDI_IMPRE_MASIVA     CHAR(1 BYTE),
  IMP_GEST_ESTADO       NUMBER(13,2)            DEFAULT 0                     NOT NULL,
  COD_MONE_IMP          CHAR(3 BYTE),
  COD_CARTA             NUMBER(5),
  COD_SECT_ASIGN_ESPEC  CHAR(4 BYTE),
  TMP_EST_GEST          DATE,
  USER_ESTADO           VARCHAR2(50 BYTE)       NOT NULL,
  COD_SECT_ESTADO       CHAR(4 BYTE),
  COD_BANDEJA           VARCHAR2(30 BYTE),
  ORDEN_ESTADO          NUMBER(5)               DEFAULT 0                     NOT NULL,
  COD_RESPONSAB_EST     NUMBER(5)               DEFAULT 0,
  ACTOR_NRO_ORDEN       NUMBER(2),
  COMENTARIO            CLOB,
  IDE_GESTION_SEC       NUMBER(3)               DEFAULT 0                     NOT NULL,
  IND_MODF_FEC_RESOL    CHAR(1 BYTE)            DEFAULT 'N'                   NOT NULL
)
LOB (COMENTARIO) STORE AS BASICFILE LOBSEGMENT (
  TABLESPACE  GEC01_DATA_M
  ENABLE      STORAGE IN ROW
  CHUNK       8192
  PCTVERSION  10
  NOCACHE
  LOGGING
      STORAGE    (
                  INITIAL          4M
                  NEXT             4M
                  MINEXTENTS       1
                  MAXEXTENTS       UNLIMITED
                  PCTINCREASE      0
                  FREELISTS        1
                  FREELIST GROUPS  1
                  BUFFER_POOL      DEFAULT
                 ))