CREATE TABLE GEC01.GC_INDIVIDUOS
(
  COD_ENTIDAD            CHAR(4 BYTE)           NOT NULL,
  TPO_DOC_INDI           CHAR(1 BYTE)           NOT NULL,
  NRO_DOC_INDI           NUMBER(11)             NOT NULL,
  FEC_NACI_INDI          DATE                   NOT NULL,
  MAR_SEX_INDI           CHAR(1 BYTE)           NOT NULL,
  APE_INDI               VARCHAR2(30 BYTE)      NOT NULL,
  NOM_INDI               VARCHAR2(30 BYTE)      NOT NULL,
  IDE_NUP_INDI           CHAR(8 BYTE)           DEFAULT 0                     NOT NULL,
  COD_SUC_INDI           NUMBER(4),
  COD_SEGM_INDI          CHAR(3 BYTE)           NOT NULL,
  COD_CRM_INDI           VARCHAR2(4 BYTE)       NOT NULL,
  SEMF_INGRESO_CRM       CHAR(4 BYTE),
  SEMF_RENTA_CRM         CHAR(4 BYTE),
  SEMF_RIESGO_CRM        CHAR(4 BYTE),
  RENTABILIDAD_PROMEDIO  NUMBER(14,2),
  COLOR_SEMAFORO         CHAR(1 BYTE),
  COLOR_SEMAF_RIESGO     VARCHAR2(2 BYTE)
)