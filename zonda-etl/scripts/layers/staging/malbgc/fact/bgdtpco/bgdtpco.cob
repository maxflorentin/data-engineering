      ******************************************************************
      * DCLGEN TABLE(BGTCPCO)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCPCO))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCPCO COMMAND THAT MADE THE FOLLOWING STATEMENTS  *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCPCO                     *
      ******************************************************************
       02 PCO.
           10 PLAN                  PIC X(04).
           10 CONCEPTO              PIC X(04).
           10 FECHA-DESDE           PIC X(10).
           10 FECHA-HASTA           PIC X(10).
           10 MVTOS-LIBR-RIOM       PIC S9(05) USAGE COMP-3.
           10 PERIOD-COM            PIC X(01).
           10 PERIOD-COBR           PIC X(01).
           10 DIA-NAT-COBR          PIC S9(02) USAGE COMP-3.
           10 ENTIDAD-UMO           PIC X(04).
           10 CENTRO-UMO            PIC X(04).
           10 USERID-UMO            PIC X(08).
           10 NETNAME-UMO           PIC X(08).
           10 TIMEST-UMO            PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************