	      01 CUOTASQ.
            10 REC_CUENTA           PIC X(12).
            10 REC_OFICINA          PIC X(4).
            10 REC_ENTIDAD          PIC X(4).
            10 REC_FELIQ            PIC X(10).
            10 REC_NUMREC           PIC S9(5)V USAGE COMP-3.
            10 REC_INDPECO          PIC X(1).
            10 REC_UGYINCOR         PIC X(1).
            10 REC_FEC_ESPEPAGO     PIC X(10).
            10 REC_FEC_EMIRECIB     PIC X(10).
            10 REC_SALTEOR          PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_FEINILIQ         PIC X(10).
            10 REC_TAE              PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_SITDEUOB         PIC X(2).
            10 REC_TIP_RECIBO       PIC X(1).
            10 REC_IND_FAC_ONLINE     PIC X(1).
            10 REC_INT_ACEL_FAC       PIC S9(13)V9(4) COMP-3.
            10 REC_INT_ACEL_REC       PIC S9(13)V9(4) COMP-3.
            10 REC_CODCICLO         PIC X(2).
            10 REC_FEPROCOM         PIC X(10).
            10 REC_IND_DESGCAPI     PIC X(1).
            10 REC_CAPINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CAPRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_CAP     PIC X(3).
            10 REC_IND_DESGINTE     PIC X(1).
            10 REC_INTINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_INTRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_INT     PIC X(3).
            10 REC_IND_DESGCOMI     PIC X(1).
            10 REC_COMINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_COMRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_COM     PIC X(3).
            10 REC_IND_DESGGAST     PIC X(1).
            10 REC_GASINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_GASRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_GAS     PIC X(3).
            10 REC_IMPSUBVE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_SUB     PIC X(3).
            10 REC_IND_DESGSEGU     PIC X(1).
            10 REC_SEGINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_SEGRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_NUM_SEGURO       PIC S9(9)V USAGE COMP-3.
            10 REC_CODCONLI_SEG     PIC X(3).
            10 REC_IND_DESGIMPU     PIC X(1).
            10 REC_IMPINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IMPRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IMP_BASE         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_POR_ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_COD_CONCEIMP     PIC X(3).
            10 REC_IND_LIQIMPUE     PIC X(1).
            10 REC_CODCONLI_IMP     PIC X(3).
            10 REC_IMPROVI          PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_FEULPROV         PIC X(10).
            10 REC_FERETEN          PIC X(10).
            10 REC_SECURET          PIC S9(5)V USAGE COMP-3.
            10 REC_UGIRETEN         PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_UGNPLPEN           PIC S9(5)V USAGE COMP-3.
            10 REC_COD_EMPCOBEX     PIC X(8).
            10 REC_FEC_ENVCOBEX     PIC X(10).
            10 REC_CODCONLI_COBEX   PIC X(3).
            10 REC_IMP_COMCOBEXINI  PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IMP_COMCOBEXREC  PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_FEC_BAJCOBEX     PIC X(10).
            10 REC_IND_DESGCOBE     PIC X(1).
            10 REC_CODCONLI_MOR     PIC X(3).
            10 REC_IMP_MORCAL       PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IMP_MORREC       PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IND_DESGMOR      PIC X(1).
            10 REC_TAS_MOR          PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_IND_CAMB_TASA_MOR  PIC X(1).
            10 REC_CODCONLI_CPS     PIC X(3).
            10 REC_IMP_CPSCAL       PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IMP_CPSREC       PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IND_DESGCPS      PIC X(1).
            10 REC_TAS_CPS          PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_FEC_CALCPS       PIC X(10).
            10 REC_IND_CAMB_TASA_CPS  PIC X(1).
            10 REC_SITDEUCT         PIC X(2).
            10 REC_FEC_SITCONT      PIC X(10).
            10 REC_TIPOTIT          PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_TIPOTOT          PIC S9(3)V9(6) USAGE COMP-3.
            10 REC_UGYESCON         PIC X(1).
            10 REC_UGYCVINT         PIC X(8).
            10 REC_FECALMOR         PIC X(10).
            10 REC_FEC_HASTAMOR     PIC X(10).
            10 REC_IND_MORACOND     PIC X(1).
            10 REC_IMP_CAPITALIZ    PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_CPZ     PIC X(3).
            10 REC_IMP_DIFERIDO     PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_DIF     PIC X(3).
            10 REC_IMP_BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_BAL     PIC X(3).
            10 REC_SDO_BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_BALLOOM_FAC      PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_TIP_CAMB_FELIQ      PIC S9(6)V9(5)  USAGE COMP-3.
            10 REC_IND_PAGO_ANTIC      PIC X(1).
            10 REC_INT_ORDEN           PIC S9(13)V9(4) COMP-3.
            10 REC_REA_ORDEN           PIC S9(13)V9(4) COMP-3.
            10 REC_EJERCICIO_CAST      PIC X(10).
            10 REC_IND_SEGCESAN        PIC X(1).
            10 REC_TIP_TMC_APL     PIC X(1).
            10 REC_IND_LIQ_COBEX   PIC X(1).
            10 REC_IMP_CAST_CUOTA  PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_IND_CAST_TOT    PIC X(1).
            10 REC_COEFICI_INDEX       PIC X(4).
            10 REC_FACTOR_INDEX        PIC S9(03)V9(6) USAGE COMP-3.
            10 REC_IMPAJUST_SAL        PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_AJUSAL     PIC X(3).
            10 REC_IMPAJUST_CAP        PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_AJUCAP     PIC X(3).
            10 REC_IMPAJUST_INT        PIC S9(13)V9(4) USAGE COMP-3.
            10 REC_CODCONLI_AJUINT     PIC X(3).
            10 REC_ENTIDAD_UMO         PIC X(4).
            10 REC_CENTRO_UMO          PIC X(4).
            10 REC_USERID_UMO          PIC X(8).
            10 REC_NETNAME_UMO         PIC X(8).
            10 REC_TIMESTAMP           PIC X(26).
            10 REC_CFTNA               PIC S9(03)V9(6) USAGE COMP-3.
            10 REC_CFTNA_SIMP          PIC S9(03)V9(6) USAGE COMP-3.