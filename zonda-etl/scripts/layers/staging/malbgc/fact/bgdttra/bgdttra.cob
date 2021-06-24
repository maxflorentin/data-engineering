      ******************************************************************
      * DCLGEN TABLE(BGTCTRA)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCTRA))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCTRA COMMAND THAT MADE THE FOLLOWING STATEMENTS  *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCTRA                     *
      ******************************************************************
       01 TRA.
           10 CONCEPTO                   PIC X(4).
           10 COMISION                   PIC X(4).
           10 FECHA-DESDE                PIC X(10).
           10 SALDO-HASTA                PIC S9(13)V9(4) USAGE COMP-3.
           10 FECHA-HASTA                PIC X(10).
           10 COMI-IM                    PIC S9(13)V9(4) USAGE COMP-3.
           10 COMI-PO                    PIC S9(3)V9(5) USAGE COMP-3.
           10 COMI-MAX                   PIC S9(13)V9(4) USAGE COMP-3.
           10 COMI-MIN                   PIC S9(13)V9(4) USAGE COMP-3.
           10 ENTIDAD-UMO                PIC X(4).
           10 CENTRO-UMO                 PIC X(4).
           10 USERID-UMO                 PIC X(8).
           10 NETNAME-UMO                PIC X(8).
           10 TIMEST-UMO                 PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 14      *
      ******************************************************************