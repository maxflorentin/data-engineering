          02  DRB.
            10 ENTIDAD             PIC X(4).
            10 OFICINA             PIC X(4).
            10 CUENTA              PIC X(12).
            10 UGCPLINI            PIC X(4).
            10 UGTVNINI            PIC X(1).
            10 UGYFAINI            PIC X.
            10 UGTFMINI            PIC X(1).
            10 UGPININI            PIC S9(3)V9(6) COMP-3.
            10 UGIDPINI            PIC S9(13)V9(4) COMP-3.
            10 UGCCRINI            PIC X(4).
            10 UGPCMINI            PIC S9(3)V9(6) COMP-3.
            10 UGNPLINI            PIC S9(5) COMP-3.
            10 UGQININI            PIC S9(5) COMP-3.
            10 UGCDIPAG_INI        PIC S9(2) COMP-3.
            10 UGYSBACT            PIC X(1).
            10 UGTSBINI            PIC X(2).
            10 UGTSBACT            PIC X(2).
            10 UGYSBINI            PIC X(1).
            10 UGCONINI            PIC X(3).
            10 UGCONACT            PIC X(4).
            10 UGISBINI            PIC S9(13)V9(4) COMP-3.
            10 UGISBACT            PIC S9(13)V9(4) COMP-3.
            10 UGNPLPEN            PIC S9(9)V      COMP-3.
            10 UGNPLVEN            PIC S9(9)V      COMP-3.
            10 UGIQUIAS            PIC S9(13)V9(4) COMP-3.
            10 UGIQUINS            PIC S9(13)V9(4) COMP-3.
            10 UGPISACT            PIC S9(3)V9(6) COMP-3.
            10 UGPISINI            PIC S9(3)V9(6) COMP-3.
            10 UGPQSINI            PIC S9(3)V9(6) COMP-3.
            10 UGPQSACT            PIC S9(3)V9(6) COMP-3.
            10 UGENTINI            PIC X(4).
            10 UGENTACT            PIC X(4).
            10 UGCPLAZO            PIC X(4).
            10 UGYFARET            PIC X(1).
            10 UGYINAPL            PIC X(1).
            10 UGYFANCR            PIC X(1).
            10 IND_CUOTEXTR        PIC X(1).
            10 UGYDIFER            PIC X(1).
            10 UGPINACT            PIC S9(3)V9(6) COMP-3.
            10 UGPINREB            PIC S9(3)V9(6) COMP-3.
            10 UGTDICAL            PIC X(1).
            10 DIASPERIODO         PIC X(1).
            10 UGTVNACT            PIC X(1).
            10 UGTFRIBM            PIC X(1).
            10 UGTFMACT            PIC X(1).
            10 UGTCLAUS            PIC X(3).
            10 TIP_REDONDEO        PIC X(1).
            10 NUM_REDONDEO        PIC S9(13)V9(4) COMP-3.
            10 UGCODINT            PIC X(3).
            10 UGCODCAP            PIC X(3).
            10 UGCODSUB            PIC X(3).
            10 UGCODCOM            PIC X(3).
            10 UGFLIMIT            PIC X(10).
            10 UGNINDIF            PIC S9(5) COMP-3.
            10 UGQINQUO            PIC S9(5) COMP-3.
            10 UGNINMAR            PIC S9(5) COMP-3.
            10 UGTFRSYS            PIC X(2).
            10 UGCPECTI            PIC X(4).
            10 UGCPERCA            PIC X(4).
            10 UGCCRCAP            PIC X(4).
            10 UGCCRINT            PIC X(4).
            10 UGCPERIN            PIC X(4).
            10 UGPDFCAR            PIC S9(3)V9(6) COMP-3.
            10 UGPDFAMO            PIC S9(3)V9(6) COMP-3.
            10 UGPINCOM            PIC S9(3)V9(6) COMP-3.
            10 UGFULVTO            PIC X(10).
            10 UGFPRVTO            PIC X(10).
            10 UGFIIQUA            PIC X(10).
            10 UGFDESUP            PIC X(10).
            10 UGFULDIA            PIC X(10).
            10 UGFULDIA_ORIG       PIC X(10).
            10 UGFPPAGO            PIC X(10).
            10 UGIQUOTA            PIC S9(13)V9(4) COMP-3.
            10 UGIQUOTA_INI        PIC S9(13)V9(4) COMP-3.
            10 UGICMBAS            PIC S9(13)V9(4) COMP-3.
            10 UGIQUINT            PIC S9(13)V9(4) COMP-3.
            10 UGIQUCOM            PIC S9(13)V9(4) COMP-3.
            10 IMPDISPU            PIC S9(13)V9(4) COMP-3.
            10 UGIQUIAB            PIC S9(13)V9(4) COMP-3.
            10 UGSPDREB            PIC S9(13)V9(4) COMP-3.
            10 UGSIIREB            PIC S9(13)V9(4) COMP-3.
            10 UGSDPREB            PIC S9(13)V9(4) COMP-3.
            10 UGQPGRAO            PIC S9(13)V9(4) COMP-3.
            10 UGQPGINI            PIC S9(13)V9(4) COMP-3.
            10 UGYAMTP3            PIC X(1).
            10 UGYNOQUA            PIC X(1).
            10 IND_AJUSINHA        PIC X(1).
            10 TIPOTASA            PIC X(1).
            10 IND_AJUSCUOT        PIC X(1).
            10 COD_INTCAREN        PIC X(1).
            10 PER_DIFERIDO        PIC X(4).
            10 POR_REFINANC        PIC S9(3)V9(6) COMP-3.
            10 IND_CAPIT_PROG      PIC X(1).
            10 POR_CAPIT_PROG      PIC S9(3)V9(6) COMP-3.
            10 POR_CAPRESID        PIC S9(3)V9(6) COMP-3.
            10 VARCUO              PIC X(1).
            10 LININCUO            PIC S9(3) COMP-3.
            10 POR_PREMIO          PIC S9(3)V9(6) COMP-3.
            10 TIP_LIQUCANC        PIC X(1).
            10 SALDO_PRORROGA      PIC S9(13)V9(4) USAGE COMP-3.
            10 TASA_PRORROGA       PIC S9(3)V9(6) COMP-3.
            10 IND_FIN_IVA_ARRFIN  PIC X(1).
            10 IMPINIRE            PIC S9(13)V9(4) COMP-3.
            10 INTIMPINIRE         PIC S9(13)V9(4) COMP-3.
            10 TIPIMP              PIC S9(3)V9(6) COMP-3.
            10 CUO_DOBLE_1         PIC X(2).
            10 CUO_DOBLE_2         PIC X(2).
            10 TIPINT_EMP_BSAN     PIC S9(3)V9(6) COMP-3.
            10 PER_CONV_TASA       PIC S9(2) COMP-3.
            10 COD_BALLOOM         PIC X(1).
            10 CONT_BALLOOM        PIC S9(5)V USAGE COMP-3.
            10 NUM_CUOBALL         PIC S9(5)V USAGE COMP-3.
            10 NUM_PROBALL         PIC S9(5)V USAGE COMP-3.
            10 SDO_BALLOOM_INI     PIC S9(13)V9(4) USAGE COMP-3.
            10 UGCDIPAG_ACT        PIC S9(2) USAGE COMP-3.
            10 NUM_DIVIDENDO       PIC S9(5)V USAGE COMP-3.
            10 FACTOR_MUTUO        PIC S9(13)V9(4) USAGE COMP-3.
            10 IND_VTO_IRR             PIC X(1).
            10 COD_REFRECUO            PIC X(4).
            10 IND_PRO_CUO             PIC X(1).
            10 IND_PRO_CUO_SVTO        PIC X(1).
            10 IND_PTMO_ACEL           PIC X(1).
            10 STAMP_UMO           PIC X(26).
            10 ENTIDAD_UMO         PIC X(4).
            10 CENTRO_UMO          PIC X(4).
            10 USERID_UMO          PIC X(8).
            10 NETNAME_UMO         PIC X(8).