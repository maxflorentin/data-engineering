CANT_ROWS|TABLA                             |
---------|----------------------------------|
        8|GEC01.TIPO_GESTION                |
       26|GEC01.TIPO_MEDIOS                 |
       34|GEC01.CLASIF_CRM                  |
       57|GEC01.GC_CONCEPTOS_BCRA           |
       86|GEC01.PRODUCTO                    |
      101|GEC01.GC_CONCEPTOS_SAC            |
      157|GEC01.GC_CONCEPTOS_ESPANA         |
      457|GEC01.SUBPRODUCTO                 |
      523|GEC01.INFO_REQUERIDA_VALORES_HIJOS|
      818|GEC01.CONCEPTO                    |
     1545|GEC01.GC_CONCEPTOS_ESPANA_CIRC    |
     1639|GEC01.SECTORES                    |
     1902|GEC01.GC_CONCEPTOS_BCRA_CIRC      |
     1909|GEC01.SUBCONCEPTO                 |
     2729|GEC01.INFO_REQUERIDA              |
     3103|GEC01.GC_CONCEPTOS_SAC_CIRC       |
     6631|GEC01.INFO_REQUERIDA_VALORES      |
    34939|GEC01.CIRCUITO                    |
   144882|GEC01.GC_EMPRESAS                 |
   221911|GEC01.CIRC_INFO_REQUE             |
   276298|GEC01.GC_EMPRESA_GESTIONES        |
  4512873|GEC01.GC_INDIVIDUO_GESTIONES      |
  4790064|GEC01.GC_GESTIONES                |
  4791831|GEC01.GESTIONES                   |
  4904128|GEC01.GC_INDIVIDUOS               |
 36482239|GEC01.INFORMACION_ADJUNTA         |
 57564352|GEC01.GESTION_ESTADOS             |
---------|----------------------------------|


/* --CREATE TABLE GEC01.CIRC_INFO_REQUE
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
) */

/* --CREATE TABLE GEC01.CIRCUITO
(
  COD_ENTIDAD               CHAR(4 BYTE)        NOT NULL,
  IDE_CIRCUITO              NUMBER(5)           NOT NULL,
  COD_CIRCUITO              NUMBER(5)           NOT NULL,
  FEC_VIG_DDE_CIRC          DATE                NOT NULL,
  FEC_VIG_HTA_CIRC          DATE                NOT NULL,
  DESC_CIRC                 VARCHAR2(1000 BYTE),
  DESC_DETALL_CIRC          VARCHAR2(1000 BYTE),
  TPO_GESTION               NUMBER(4)           NOT NULL,
  COD_PROD                  NUMBER(4)           NOT NULL,
  COD_SUBPROD               NUMBER(4)           NOT NULL,
  COD_CPTO                  NUMBER(4)           NOT NULL,
  COD_SUBCPTO               NUMBER(4)           NOT NULL,
  TMP_ASIGN_ESPECIAL        NUMBER(4)           NOT NULL,
  TMP_PEDIDO_INFO           NUMBER(4)           NOT NULL,
  TMP_CIRC                  NUMBER(6)           NOT NULL,
  INDI_GEST_PEND            CHAR(1 BYTE)        NOT NULL,
  INDI_MAIL_DEMORA          CHAR(1 BYTE)        NOT NULL,
  INDI_CIERR_AUTM           CHAR(1 BYTE)        NOT NULL,
  INDI_AUTORIZA_ITEM        CHAR(1 BYTE)        NOT NULL,
  COD_RECIBO                NUMBER(5)           NOT NULL,
  EST_CIRC                  CHAR(1 BYTE)        NOT NULL,
  USER_ALT_CIRC             VARCHAR2(8 BYTE)    NOT NULL,
  FEC_ALT_CIRC              DATE                NOT NULL,
  USER_MODF_CIRC            VARCHAR2(8 BYTE)    NOT NULL,
  FEC_MODF_CIRC             DATE                NOT NULL,
  INDI_MODIF_DATOS          CHAR(1 BYTE)        DEFAULT 0                     NOT NULL,
  INDI_RECEP_RESP           CHAR(1 BYTE)        DEFAULT NULL                  NOT NULL,
  INDI_SUCURSALES           CHAR(1 BYTE)        NOT NULL,
  INDI_DEJAR_PEND           CHAR(1 BYTE)        DEFAULT 'O'                   NOT NULL,
  COD_TPO_RESOL             CHAR(1 BYTE)        DEFAULT 'F',
  UND_TIEMPO                CHAR(1 BYTE)        DEFAULT 'D',
  TPO_VIS_GEST              CHAR(1 BYTE)        DEFAULT 'T'                   NOT NULL,
  INDI_SUMA_TMP_AUTZ        CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  COD_CONFORME              NUMBER(5)           DEFAULT 0                     NOT NULL,
  INDI_JERARQUIA_AUTZ       CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_SECUENCIA_AUTZ       CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_ASIG_PARALELO        CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_AUTORIZ_DEVOL        CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_MODF_FEC             CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  CIRCUITO_PREDECESOR       NUMBER(5),
  INDI_CRIT                 CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_INFO_MULTIVALUADA    CHAR(1 BYTE)        DEFAULT 'S'                   NOT NULL,
  INDI_ENVIAR_MAIL_RECEP    CHAR(1 BYTE),
  INDI_ENVIAR_SMS_RECEP     CHAR(1 BYTE),
  INDI_ENVIAR_MAIL_RESP     CHAR(1 BYTE),
  INDI_ENVIAR_SMS_RESP      CHAR(1 BYTE),
  INDI_ENVIAR_MAIL_DEMORA   CHAR(1 BYTE),
  COD_MODELO_MSJ            NUMBER(2),
  ID_CLASIF_SELECT          INTEGER             DEFAULT 2                     NOT NULL,
  ID_PARRAFO_CUERPO_MAIL    INTEGER,
  INDI_USO_MONTO            CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  INDI_ENVIAR_CARTA_RESP    CHAR(1 BYTE)        DEFAULT 'N'                   NOT NULL,
  COD_COMPROBANTE_CLIENTE   NUMBER,
  INDI_ENVIAR_MAIL_PROV     CHAR(1 BYTE),
  INDI_REAPERTURA           CHAR(1 BYTE)        DEFAULT 'R',
  LONG_COMENTARIO_RECEP     NUMBER              DEFAULT 4000,
  LONG_COMENTARIO_CLIENTE   NUMBER              DEFAULT 3000,
  AVISO_VENC_GEST           NUMBER(2),
  INDI_ENVIAR_MAIL_NO_AUTZ  CHAR(1 BYTE)        DEFAULT 'N',
  COD_ENTIDAD_AFECT         NUMBER              DEFAULT 1                     NOT NULL
)
*/

/* --CREATE TABLE GEC01.CLASIF_CRM
(
  COD_ENTIDAD       CHAR(4 BYTE)                NOT NULL,
  TPO_PERS          CHAR(1 BYTE)                NOT NULL,
  COD_CRM           VARCHAR2(2 BYTE)            NOT NULL,
  DESC_CRM          VARCHAR2(30 BYTE),
  ID_CLASIF_SELECT  INTEGER
)
*/

/* --CREATE TABLE GEC01.CONCEPTO
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
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_BCRA
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
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_BCRA_CIRC
(
  COD_ENTIDAD   CHAR(4 BYTE)                    NOT NULL,
  COD_CPTO      NUMBER(4)                       NOT NULL,
  IDE_CIRCUITO  NUMBER(8)                       NOT NULL,
  USER_ALT      VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_ALT       DATE                            DEFAULT SYSDATE               NOT NULL,
  USER_MODF     VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_MODF      DATE                            DEFAULT SYSDATE               NOT NULL,
  ESTADO        VARCHAR2(1 BYTE)                DEFAULT 'A'
)
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_ESPANA
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
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_ESPANA_CIRC
(
  COD_ENTIDAD   CHAR(4 BYTE)                    NOT NULL,
  COD_CPTO      NUMBER(4)                       NOT NULL,
  IDE_CIRCUITO  NUMBER(8)                       NOT NULL,
  USER_ALT      VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_ALT       DATE                            DEFAULT SYSDATE               NOT NULL,
  USER_MODF     VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_MODF      DATE                            DEFAULT SYSDATE               NOT NULL,
  ESTADO        VARCHAR2(1 BYTE)                DEFAULT 'A'
)
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_SAC
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
*/

/* --CREATE TABLE GEC01.GC_CONCEPTOS_SAC_CIRC
(
  COD_ENTIDAD   CHAR(4 BYTE)                    NOT NULL,
  COD_CPTO      NUMBER(4)                       NOT NULL,
  IDE_CIRCUITO  NUMBER(8)                       NOT NULL,
  USER_ALT      VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_ALT       DATE                            DEFAULT SYSDATE               NOT NULL,
  USER_MODF     VARCHAR2(8 BYTE)                DEFAULT 'VINICIAL'            NOT NULL,
  FEC_MODF      DATE                            DEFAULT SYSDATE               NOT NULL,
  ESTADO        VARCHAR2(1 BYTE)                DEFAULT 'A'
)
*/

/* --CREATE TABLE GEC01.GC_EMPRESA_GESTIONES
(
  COD_ENTIDAD         CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_SECTOR  CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_NRO     NUMBER(8)                 NOT NULL,
  TPO_DOC_EMPR        CHAR(1 BYTE)              NOT NULL,
  NRO_DOC_EMPR        NUMBER(11)                NOT NULL,
  SEC_DOC_EMPR        NUMBER(2)                 NOT NULL
)
*/

/* --CREATE TABLE GEC01.GC_EMPRESAS
(
  COD_ENTIDAD            CHAR(4 BYTE)           NOT NULL,
  TPO_DOC_EMPR           CHAR(1 BYTE)           NOT NULL,
  NRO_DOC_EMPR           NUMBER(11)             NOT NULL,
  SEC_DOC_EMPR           NUMBER(2)              NOT NULL,
  TPO_EMPR               CHAR(4 BYTE),
  FEC_INI_ACTIV_EMPR     DATE                   NOT NULL,
  COD_ACTIV_EMPR         CHAR(3 BYTE)           NOT NULL,
  NOM_COMER_EMPR         VARCHAR2(100 BYTE)     NOT NULL,
  RAZON_SOCIAL_EMPR      VARCHAR2(100 BYTE)     NOT NULL,
  IDE_NUP_EMPR           CHAR(8 BYTE)           DEFAULT 0                     NOT NULL,
  APE_RESP_EMPR          VARCHAR2(30 BYTE)      NOT NULL,
  NOM_RESP_EMPR          VARCHAR2(30 BYTE)      NOT NULL,
  TPO_DOC2_EMPR          CHAR(1 BYTE),
  NRO_DOC2_EMPR          NUMBER(11)             DEFAULT 0                     NOT NULL,
  COD_SUC_EMPR           NUMBER(4),
  COD_SEGM_EMPR          CHAR(3 BYTE)           NOT NULL,
  COD_CRM_EMPR           VARCHAR2(2 BYTE)       NOT NULL,
  SEMF_INGRESO_CRM       CHAR(2 BYTE),
  SEMF_RENTA_CRM         CHAR(2 BYTE),
  SEMF_RIESGO_CRM        CHAR(4 BYTE),
  RENTABILIDAD_PROMEDIO  NUMBER(14,2),
  COLOR_SEMAFORO         CHAR(1 BYTE),
  COLOR_SEMAF_RIESGO     VARCHAR2(2 BYTE)
)
*/

/* --CREATE TABLE GEC01.GC_GESTIONES
(
  COD_ENTIDAD               CHAR(4 BYTE)        NOT NULL,
  IDE_GESTION_SECTOR        CHAR(4 BYTE)        NOT NULL,
  IDE_GESTION_NRO           NUMBER(8)           NOT NULL,
  COD_SEGM                  CHAR(3 BYTE)        NOT NULL,
  SEMF_INGRESO_CRM          VARCHAR2(2 BYTE),
  SEMF_RENTA_CRM            VARCHAR2(2 BYTE),
  SEMF_RIESGO_CRM           CHAR(4 BYTE),
  CALLE_DOM_CONTC           VARCHAR2(50 BYTE),
  NRO_DOM_CONTC             NUMBER(5)           DEFAULT 0                     NOT NULL,
  PISO_DOM_CONTC            CHAR(2 BYTE),
  DPTO_DOM_CONTC            CHAR(4 BYTE),
  CPOST_DOM_CONTC           CHAR(8 BYTE),
  LOC_DOM_CONTC             VARCHAR2(50 BYTE),
  PCIA_DOM_CONTC            NUMBER(2)           DEFAULT 0,
  PAIS_DOM_CONTC            CHAR(3 BYTE),
  DDN_TEL_DOM_CONTC         NUMBER(6)           DEFAULT 0                     NOT NULL,
  NRO_TEL_DOM_CONTC         NUMBER(15)          DEFAULT 0                     NOT NULL,
  DDN_FAX_CONTC             NUMBER(6)           DEFAULT 0                     NOT NULL,
  NRO_FAX_CONTC             NUMBER(15)          DEFAULT 0                     NOT NULL,
  HRA_DOM_CONTC             VARCHAR2(100 BYTE),
  DDN_CELULAR_CONTC         NUMBER(6)           DEFAULT 0                     NOT NULL,
  NRO_CELULAR_CONTC         NUMBER(15)          DEFAULT 0                     NOT NULL,
  DDN_TEL_LABORAL           NUMBER(6)           DEFAULT 0                     NOT NULL,
  NRO_TEL_LABORAL           NUMBER(15)          DEFAULT 0                     NOT NULL,
  HRA_TEL_LABORAL           VARCHAR2(100 BYTE),
  DDN_FAX_LABORAL           NUMBER(6)           DEFAULT 0                     NOT NULL,
  NRO_FAX_LABORAL           NUMBER(15)          DEFAULT 0                     NOT NULL,
  APLIC_CTA                 CHAR(4 BYTE),
  COD_SUC_CTA               NUMBER(3),
  NRO_CTA                   NUMBER(16)          DEFAULT 0                     NOT NULL,
  NRO_FIRM_CTA              NUMBER(5)           DEFAULT 0                     NOT NULL,
  COD_MONE_CTA              CHAR(3 BYTE),
  COD_PROD_CTA              CHAR(2 BYTE),
  COD_SUBPRO_CTA            CHAR(4 BYTE),
  NRO_TARJETA               NUMBER(20)          DEFAULT 0                     NOT NULL,
  LINK_RESUMEN              VARCHAR2(200 BYTE),
  DIRE_MAIL_CONTC           VARCHAR2(100 BYTE),
  DIRE_MAIL2_CONTC          VARCHAR2(100 BYTE),
  CARTERA                   NUMBER(2),
  APLIC_CTA_GOLD            CHAR(4 BYTE),
  COD_SUC_CTA_GOLD          NUMBER(3),
  NRO_CTA_GOLD              NUMBER(16),
  NRO_FIRM_CTA_GOLD         NUMBER(5),
  COD_MONE_CTA_GOLD         CHAR(3 BYTE),
  COD_PROD_CTA_GOLD         CHAR(2 BYTE),
  COD_SUBPRO_CTA_GOLD       CHAR(4 BYTE),
  COD_RAMO_CTA              NUMBER(2),
  NRO_CERTIFICADO_CTA       NUMBER(9),
  RENTABILIDAD_PROMEDIO     NUMBER(14,2),
  COLOR_SEMAFORO            CHAR(1 BYTE),
  COLOR_SEMAF_RIESGO        VARCHAR2(2 BYTE),
  INDI_ENVIOS_MYA           NUMBER(2),
  DIRE_MAIL_OPCIONAL        VARCHAR2(100 BYTE),
  NRO_PAQUETE               NUMBER(20),
  COD_PAQUETE               NUMBER(4),
  INDI_PLAN_SUELDO          CHAR(1 BYTE),
  NRO_CONVENIO_PAQUETE      VARCHAR2(7 BYTE),
  EMP_CELULAR_CONTC         CHAR(4 BYTE),
  TPO_MSJ_ASOC_RTA          NUMBER(5),
  MONTO_DEUDA_REF           NUMBER(14,3)        DEFAULT NULL,
  NO_ENVIAR_NOTIFICACIONES  CHAR(1 BYTE),
  ID_EJECUTIVO              VARCHAR2(50 BYTE)   DEFAULT NULL
)
*/

/* --CREATE TABLE GEC01.GC_INDIVIDUO_GESTIONES
(
  COD_ENTIDAD         CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_SECTOR  CHAR(4 BYTE)              NOT NULL,
  IDE_GESTION_NRO     NUMBER(8)                 NOT NULL,
  TPO_DOC_INDI        CHAR(1 BYTE)              NOT NULL,
  NRO_DOC_INDI        NUMBER(11)                NOT NULL,
  FEC_NACI_INDI       DATE                      NOT NULL,
  MAR_SEX_INDI        CHAR(1 BYTE)              NOT NULL
)
*/

/* --CREATE TABLE GEC01.GC_INDIVIDUOS
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
*/

/* --CREATE TABLE GEC01.GESTION_ESTADOS
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
*/

/* --CREATE TABLE GEC01.GESTIONES
(
  COD_ENTIDAD             CHAR(4 BYTE)          NOT NULL,
  IDE_GESTION_SECTOR      CHAR(4 BYTE)          NOT NULL,
  IDE_GESTION_NRO         NUMBER(8)             NOT NULL,
  IDE_CIRCUITO            NUMBER(5)             NOT NULL,
  INDI_TIPO_CIRC          CHAR(1 BYTE)          DEFAULT 0                     NOT NULL,
  INDI_DECISION_COMER     CHAR(1 BYTE)          DEFAULT 'N'                   NOT NULL,
  INDI_REPLTEO            CHAR(1 BYTE)          DEFAULT 'N'                   NOT NULL,
  INDI_RTA_INMED          CHAR(1 BYTE)          NOT NULL,
  INDI_IMPRE_CART         CHAR(1 BYTE),
  IMP_AUTZ_SOLICITADO     NUMBER(13,2)          DEFAULT 0                     NOT NULL,
  COD_MONE_SOLICITADO     CHAR(3 BYTE),
  IMP_AUTZ_AUTORIZADO     NUMBER(13,2)          DEFAULT 0                     NOT NULL,
  COD_MONE_AUTORIZADO     CHAR(3 BYTE),
  IMP_AUTZ_RESOLUCION     NUMBER(13,2)          DEFAULT 0                     NOT NULL,
  COD_MONE_RESOLUCION     CHAR(3 BYTE),
  TPO_PERS                CHAR(1 BYTE)          NOT NULL,
  COD_CRM                 VARCHAR2(2 BYTE)      NOT NULL,
  COMEN_CLIENTE           VARCHAR2(4000 BYTE),
  IDE_GEST_SECTOR_RELAC   CHAR(6 BYTE),
  IDE_GEST_NRO_RELAC      NUMBER(8),
  FEC_GESTION_ALTA        DATE,
  COD_BANDEJA_ACTUAL      VARCHAR2(30 BYTE),
  FEC_BANDEJA_ACTUAL      DATE,
  COD_SECTOR_ACTUAL       CHAR(4 BYTE),
  INIC_USER_ALTA          CHAR(5 BYTE)          DEFAULT 'NNNNN'               NOT NULL,
  INDI_MAIL               CHAR(1 BYTE),
  INDI_PRIORIDAD          NUMBER(5)             DEFAULT 0                     NOT NULL,
  FEC_MAX_RESOL           DATE                  DEFAULT SYSDATE,
  COD_CONFORME            NUMBER(5)             DEFAULT 0                     NOT NULL,
  COD_USER_ACTUAL         VARCHAR2(8 BYTE),
  COD_GRUPO_EMPRESA       NUMBER(1)             DEFAULT 2                     NOT NULL,
  COD_TIPO_FAVORABILIDAD  NUMBER(2),
  FEC_AVISO_VENC          DATE
)
*/

/* --CREATE TABLE GEC01.INFO_REQUERIDA
(
  COD_ENTIDAD           CHAR(4 BYTE)            NOT NULL,
  COD_INFO_REQUE        NUMBER(5)               NOT NULL,
  DESC_INFO_REQUE       VARCHAR2(400 BYTE)      NOT NULL,
  COD_TPO_DAT           NUMBER(5)               NOT NULL,
  LONG_INFO_REQUE       NUMBER(13)              NOT NULL,
  CANT_DEC_INFO_REQUE   NUMBER(1)               NOT NULL,
  RANG_DDE_INFO_REQUE   NUMBER(32,2)            NOT NULL,
  RANG_HTA_INFO_REQUE   NUMBER(32,2)            NOT NULL,
  FEC_DDE_INFO_REQUE    DATE                    NOT NULL,
  FEC_HTA_INFO_REQUE    DATE                    NOT NULL,
  EST_INFO_REQUE        CHAR(1 BYTE)            NOT NULL,
  USER_ALT_INFO_REQUE   VARCHAR2(8 BYTE)        NOT NULL,
  USER_MODF_INFO_REQUE  VARCHAR2(8 BYTE)        NOT NULL,
  FEC_ALT_INFO_REQUE    DATE                    NOT NULL,
  FEC_MODF_INFO_REQUE   DATE                    NOT NULL,
  COD_TPO_CAMPO         NUMBER(5)               DEFAULT 1                     NOT NULL,
  COD_FUNCION_ASOC      NUMBER(5)               DEFAULT 0                     NOT NULL,
  COD_INFO_RELAC        NUMBER(5),
  TPO_INFO_ESPECIAL     NUMBER(2)               DEFAULT 0                     NOT NULL,
  SECTOR_OWNER          CHAR(4 BYTE)            NOT NULL,
  TEXTO_AYUDA           VARCHAR2(200 BYTE),
  COD_INFO_CONDIC       NUMBER(5),
  COD_VALOR_CONDIC      VARCHAR2(5 BYTE)
)
*/

/* --CREATE TABLE GEC01.INFO_REQUERIDA_VALORES
(
  COD_ENTIDAD             CHAR(4 BYTE)          NOT NULL,
  COD_INFO_REQUE          NUMBER(5)             NOT NULL,
  COD_VALOR               VARCHAR2(5 BYTE),
  NRO_VALOR               NUMBER(5),
  DESC_VALOR              VARCHAR2(80 BYTE)     NOT NULL,
  EST_VALOR               CHAR(1 BYTE)          NOT NULL,
  USER_ALT_VALOR          VARCHAR2(8 BYTE)      NOT NULL,
  FEC_ALT_VALOR           DATE                  NOT NULL,
  USER_MODF_VALOR         VARCHAR2(8 BYTE)      NOT NULL,
  FEC_MODF_VALOR          DATE                  NOT NULL,
  COD_INFO_REQUE_VALORES  VARCHAR2(5 BYTE)
)
*/

/* --CREATE TABLE GEC01.INFO_REQUERIDA_VALORES_HIJOS
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
*/

/* --CREATE TABLE GEC01.INFORMACION_ADJUNTA
(
  COD_ENTIDAD           CHAR(4 BYTE)            NOT NULL,
  IDE_GESTION_SECTOR    CHAR(4 BYTE)            NOT NULL,
  IDE_GESTION_NRO       NUMBER(8)               NOT NULL,
  COD_ACTOR             NUMBER(2)               NOT NULL,
  NRO_ORDEN             NUMBER(5)               NOT NULL,
  INDI_TPO_INFO         NUMBER(1)               NOT NULL,
  COD_INFO_DOC_REQUE    NUMBER(5)               NOT NULL,
  DATO_INFO_DOC_REQUE   VARCHAR2(2000 BYTE),
  LINK_INF_ADJUNTA      VARCHAR2(200 BYTE),
  FEC_INF_ADJUNTA       DATE                    NOT NULL,
  USER_INF_ADJUNTA      VARCHAR2(50 BYTE)       NOT NULL,
  COD_SECT_INF_ADJUNTA  CHAR(4 BYTE),
  NOM_ARCHIVO_ORIGINAL  VARCHAR2(100 BYTE),
  NOM_ARCHIVO           VARCHAR2(100 BYTE),
  NOM_DIRECTORIO        VARCHAR2(100 BYTE),
  TAMANO                NUMBER,
  SECUENCIA             NUMBER(2)               DEFAULT 0                     NOT NULL
)
*/

/* --CREATE TABLE GEC01.PRODUCTO
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
*/

/* --CREATE TABLE GEC01.SECTORES
(
  COD_ENTIDAD            CHAR(4 BYTE)           NOT NULL,
  COD_SECTOR             CHAR(4 BYTE)           NOT NULL,
  DESC_SECT              VARCHAR2(200 BYTE)     NOT NULL,
  COD_GRUPO              CHAR(4 BYTE)           NOT NULL,
  INDI_ACTRAL            CHAR(1 BYTE)           NOT NULL,
  COD_PCIA               NUMBER(2)              NOT NULL,
  MAIL_SECTOR            VARCHAR2(80 BYTE),
  EST_SECT               CHAR(1 BYTE)           NOT NULL,
  USER_ALT_SECT          VARCHAR2(8 BYTE)       NOT NULL,
  FEC_ALT_SECT           DATE                   NOT NULL,
  USER_MODF_SECT         VARCHAR2(8 BYTE)       NOT NULL,
  FEC_MODF_SECT          DATE                   NOT NULL,
  ENVIAR_MAIL            CHAR(1 BYTE)           DEFAULT 'S'                   NOT NULL,
  INDI_INFO_ASIG_ESP     CHAR(1 BYTE)           DEFAULT 'S'                   NOT NULL,
  COD_PAIS               CHAR(3 BYTE)           NOT NULL,
  INDI_RESOL_INFO_ADJ    CHAR(1 BYTE)           DEFAULT 'N'                   NOT NULL,
  COD_CTRO_COSTOS        VARCHAR2(10 BYTE),
  INDI_MAIL_BANDEJA      CHAR(1 BYTE)           DEFAULT 'N',
  INDI_USO_CARTA         CHAR(1 BYTE)           DEFAULT 'N'                   NOT NULL,
  SECTOR_OWNER           CHAR(4 BYTE)           NOT NULL,
  COD_GRUPO_EMPRESA      NUMBER(1)              DEFAULT 2,
  INDI_ADMIN             CHAR(1 BYTE)           DEFAULT 'N'                   NOT NULL,
  GEST_X_PAG_BANDEJA     NUMBER(4)              DEFAULT 20                    NOT NULL,
  COD_SUCURSAL           NUMBER(4),
  COD_ZONA               CHAR(4 BYTE),
  TIPO_SECTOR            NUMBER(4)              NOT NULL,
  CLASS_PDF              VARCHAR2(255 BYTE),
  DIAS_GESTIONES_ANT     NUMBER(4),
  COD_SECTOR_GEN         CHAR(4 BYTE),
  INDI_COMPROMISO_GOLD   CHAR(1 BYTE)           DEFAULT 'N'                   NOT NULL,
  INDI_ENVIAR_MAIL_RESP  CHAR(1 BYTE),
  INDI_ENVIAR_SMS_RESP   CHAR(1 BYTE),
  INDI_NO_ENVIAR_MYA     CHAR(1 BYTE)           DEFAULT 'N',
  INDI_INSTANCIAS_POST   CHAR(1 BYTE)           DEFAULT 'N'
)
*/

/* --CREATE TABLE GEC01.SUBCONCEPTO
(
  COD_ENTIDAD          CHAR(4 BYTE)             NOT NULL,
  COD_SUBCPTO          NUMBER(4)                NOT NULL,
  DESC_SUBCPTO         VARCHAR2(80 BYTE)        NOT NULL,
  DESC_DETALL_SUBCPTO  VARCHAR2(1000 BYTE),
  EST_SUBCPTO          CHAR(1 BYTE)             NOT NULL,
  USER_ALT_SUBCPTO     VARCHAR2(8 BYTE)         NOT NULL,
  FEC_ALT_SUBCPTO      DATE                     NOT NULL,
  USER_MODF_SUBCPTO    VARCHAR2(8 BYTE)         NOT NULL,
  FEC_MODF_SUBCPTO     DATE                     NOT NULL,
  SECTOR_OWNER         CHAR(4 BYTE)             NOT NULL
)
*/

/* --CREATE TABLE GEC01.SUBPRODUCTO
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
*/

/* --CREATE TABLE GEC01.TIPO_GESTION
(
  COD_ENTIDAD           CHAR(4 BYTE)            NOT NULL,
  TPO_GESTION           NUMBER(4)               NOT NULL,
  DESC_TPO_GEST         VARCHAR2(50 BYTE)       NOT NULL,
  DESC_DETALL_TPO_GEST  VARCHAR2(1000 BYTE)     NOT NULL,
  EST_TPO_GEST          CHAR(1 BYTE)            NOT NULL,
  USER_ALT_TPO_GEST     VARCHAR2(8 BYTE)        NOT NULL,
  FEC_ALT_TPO_GEST      DATE                    NOT NULL,
  USER_MODF_TPO_GEST    VARCHAR2(8 BYTE)        NOT NULL,
  FEC_MODF_TPO_GEST     DATE                    NOT NULL
)
*/

/* --CREATE TABLE GEC01.TIPO_MEDIOS
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
*/

/* --
*/

