       ******************************************************************
      * DCLGEN TABLE(BGTCBAN)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCBAN))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCBAN COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCBAN                     *
      ******************************************************************
       01 BAN.
           10 PLAN                              PIC X(04).
           10 CONCEPTO                          PIC X(04).
           10 TITULARIDAD                       PIC X(02).
           10 RED                               PIC X(04).
           10 FECHA-DESDE                       PIC X(10).
           10 FECHA-HASTA                       PIC X(10).
           10 COMISION                          PIC X(04).
           10 ENTIDAD-UMO                       PIC X(04).
           10 CENTRO-UMO                        PIC X(04).
           10 USERID-UMO                        PIC X(08).
           10 NETNAME-UMO                       PIC X(08).
           10 TIMEST-UMO                        PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************.