       ******************************************************************
      * DCLGEN TABLE(BGDTRIO)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGDTRIO))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGDTRIO COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGDTRIO                     *
      ******************************************************************
       02 RIO.
           10 PLAN                            PIC X(04).
           10 CONCEPTO                        PIC X(04).
           10 TIP-OPERACION                   PIC X(01).
           10 ZONA                            PIC X(01).
           10 DIVISA                          PIC X(03).
           10 FECHA-DESDE                     PIC X(10).
           10 FECHA-HASTA                     PIC X(10).
           10 COMISION                        PIC X(04).
           10 ENTIDAD-UMO                     PIC X(04).
           10 CENTRO-UMO                      PIC X(04).
           10 USERID-UMO                      PIC X(08).
           10 NETNAME-UMO                     PIC X(08).
           10 TIMEST-UMO                      PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 13      *
      ******************************************************************