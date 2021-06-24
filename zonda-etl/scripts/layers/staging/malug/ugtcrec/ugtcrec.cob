          02  REC.
            10 CUENTA           PIC X(12).
            10 OFICINA          PIC X(4).
            10 ENTIDAD          PIC X(4).
            10 FELIQ            PIC X(10).
            10 NUMREC           PIC S9(5)V USAGE COMP-3.
            10 INDPECO          PIC X(1).
            10 UGYINCOR         PIC X(1).
            10 FEC-ESPEPAGO     PIC X(10).
            10 FEC-EMIRECIB     PIC X(10).
            10 SALTEOR          PIC S9(13)V9(4) USAGE COMP-3.
            10 FEINILIQ         PIC X(10).
            10 TAE              PIC S9(3)V9(6) USAGE COMP-3.
            10 SITDEUOB         PIC X(2).
            10 TIP-RECIBO       PIC X(1).
            10 IND-FAC-ONLINE     PIC X(1).
            10 INT-ACEL-FAC       PIC S9(13)V9(4) COMP-3.
            10 INT-ACEL-REC       PIC S9(13)V9(4) COMP-3.
            10 CODCICLO         PIC X(2).
            10 FEPROCOM         PIC X(10).
            10 IND-DESGCAPI     PIC X(1).
            10 CAPINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CAPRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-CAP     PIC X(3).
            10 IND-DESGINTE     PIC X(1).
            10 INTINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 INTRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-INT     PIC X(3).
            10 IND-DESGCOMI     PIC X(1).
            10 COMINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 COMRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-COM     PIC X(3).
            10 IND-DESGGAST     PIC X(1).
            10 GASINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 GASRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-GAS     PIC X(3).
            10 IMPSUBVE         PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-SUB     PIC X(3).
            10 IND-DESGSEGU     PIC X(1).
            10 SEGINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 SEGRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 NUM-SEGURO       PIC S9(9)V USAGE COMP-3.
            10 CODCONLI-SEG     PIC X(3).
            10 IND-DESGIMPU     PIC X(1).
            10 IMPINIRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 IMPRECRE         PIC S9(13)V9(4) USAGE COMP-3.
            10 IMP-BASE         PIC S9(13)V9(4) USAGE COMP-3.
            10 POR-ALICUOTA     PIC S9(3)V9(6) USAGE COMP-3.
            10 COD-CONCEIMP     PIC X(3).
            10 IND-LIQIMPUE     PIC X(1).
            10 CODCONLI-IMP     PIC X(3).
            10 IMPROVI          PIC S9(13)V9(4) USAGE COMP-3.
            10 FEULPROV         PIC X(10).
            10 FERETEN          PIC X(10).
            10 SECURET          PIC S9(5)V USAGE COMP-3.
            10 UGIRETEN         PIC S9(13)V9(4) USAGE COMP-3.
            10 UGNPLPEN           PIC S9(5)V USAGE COMP-3.
            10 COD-EMPCOBEX     PIC X(8).
            10 FEC-ENVCOBEX     PIC X(10).
            10 CODCONLI-COBEX   PIC X(3).
            10 IMP-COMCOBEXINI  PIC S9(13)V9(4) USAGE COMP-3.
            10 IMP-COMCOBEXREC  PIC S9(13)V9(4) USAGE COMP-3.
            10 FEC-BAJCOBEX     PIC X(10).
            10 IND-DESGCOBE     PIC X(1).
            10 CODCONLI-MOR     PIC X(3).
            10 IMP-MORCAL       PIC S9(13)V9(4) USAGE COMP-3.
            10 IMP-MORREC       PIC S9(13)V9(4) USAGE COMP-3.
            10 IND-DESGMOR      PIC X(1).
            10 TAS-MOR          PIC S9(3)V9(6) USAGE COMP-3.
            10 IND-CAMB-TASA-MOR        PIC X(1).
            10 CODCONLI-CPS     PIC X(3).
            10 IMP-CPSCAL       PIC S9(13)V9(4) USAGE COMP-3.
            10 IMP-CPSREC       PIC S9(13)V9(4) USAGE COMP-3.
            10 IND-DESGCPS      PIC X(1).
            10 TAS-CPS          PIC S9(3)V9(6) USAGE COMP-3.
            10 FEC-CALCPS       PIC X(10).
            10 IND-CAMB-TASA-CPS    PIC X(1).
            10 SITDEUCT         PIC X(2).
            10 FEC-SITCONT      PIC X(10).
            10 TIPOTIT          PIC S9(3)V9(6) USAGE COMP-3.
            10 TIPOTOT          PIC S9(3)V9(6) USAGE COMP-3.
            10 UGYESCON         PIC X(1).
            10 UGYCVINT         PIC X(8).
            10 FECALMOR         PIC X(10).
            10 FEC-HASTAMOR     PIC X(10).
            10 IND-MORACOND     PIC X(1).
            10 IMP-CAPITALIZ    PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-CPZ     PIC X(3).
            10 IMP-DIFERIDO     PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-DIF     PIC X(3).
            10 IMP-BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-BAL     PIC X(3).
            10 SDO-BALLOOM      PIC S9(13)V9(4) USAGE COMP-3.
            10 BALLOOM-FAC      PIC S9(13)V9(4) USAGE COMP-3.
            10 TIP-CAMB-FELIQ      PIC S9(6)V9(5)  USAGE COMP-3.
            10 IND-PAGO-ANTIC      PIC X(1).
            10 INT-ORDEN           PIC S9(13)V9(4) COMP-3.
            10 REA-ORDEN           PIC S9(13)V9(4) COMP-3.
            10 EJERCICIO-CAST      PIC X(10).
            10 IND-SEGCESAN        PIC X(1).
            10 TIP-TMC-APL     PIC X(1).
            10 IND-LIQ-COBEX   PIC X(1).
            10 IMP-CAST-CUOTA  PIC S9(13)V9(4) USAGE COMP-3.
            10 IND-CAST-TOT    PIC X(1).
            10 COEFICI-INDEX       PIC X(4).
            10 FACTOR-INDEX        PIC S9(03)V9(6) USAGE COMP-3.
            10 IMPAJUST-SAL        PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-AJUSAL     PIC X(3).
            10 IMPAJUST-CAP        PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-AJUCAP     PIC X(3).
            10 IMPAJUST-INT        PIC S9(13)V9(4) USAGE COMP-3.
            10 CODCONLI-AJUINT     PIC X(3).
            10 ENTIDAD-UMO         PIC X(4).
            10 CENTRO-UMO          PIC X(4).
            10 USERID-UMO          PIC X(8).
            10 NETNAME-UMO         PIC X(8).
            10 TIMESTAMP           PIC X(26).
            10 CFTNA               PIC S9(03)V9(6) USAGE COMP-3.
            10 CFTNA-SIMP          PIC S9(03)V9(6) USAGE COMP-3.