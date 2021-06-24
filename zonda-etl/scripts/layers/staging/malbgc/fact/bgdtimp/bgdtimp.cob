      ******************************************************************
      * DCLGEN TABLE(BGGTIMP)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGGTIMP))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGGTIMP COMMAND THAT MADE THE FOLLOWING STATEMENTS  *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGGTIMP                     *
      ******************************************************************
       01 IMP.
           10 ENTIDAD                        PIC X(04).
           10 CENTRO-ALTA                    PIC X(04).
           10 CUENTA                         PIC X(12).
           10 DIVISA                         PIC X(03).
           10 SECUIMP                        PIC S9(05) COMP-3.
           10 SECUIMP-ADICI                  PIC S9(01) COMP-3.
           10 CONCEPTO                       PIC X(04).
           10 FECHA-IMP                      PIC X(10).
           10 PRIORIDAD                      PIC X(02).
           10 IMPORTE-IMPAGO                 PIC S9(13)V9(4) COMP-3.
           10 CODOPER-IMPAGO                 PIC X(04).
           10 BASE-IMPUESTO                  PIC S9(13)V9(2) COMP-3.
           10 IMPUESTO                       PIC S9(13)V9(4) COMP-3.
           10 CODOPE-IMPUEST                 PIC X(04).
           10 CPTO-IMPUESTO                  PIC X(04).
           10 IND-DEC-DIV                    PIC X(01).
           10 FECHA-COBRO                    PIC X(10).
           10 IMPORT-TOTCOB                  PIC S9(13)V9(4) COMP-3.
           10 IMPAGO-COBRO                   PIC S9(13)V9(4) COMP-3.
           10 IMPU-COBRO                     PIC S9(13)V9(4) COMP-3.
           10 ESTADO                         PIC X(01).
           10 COD-OPER-PPAL                  PIC X(4).
           10 ENTIDAD-UMO                    PIC X(04).
           10 CENTRO-UMO                     PIC X(04).
           10 USERID-UMO                     PIC X(08).
           10 NETNAME-UMO                    PIC X(08).
           10 TIMEST-UMO                     PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 27      *
      ******************************************************************