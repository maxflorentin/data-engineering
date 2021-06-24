       ******************************************************************
      * DCLGEN TABLE(BGTCCOE)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCCOE))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCCOE COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCCOE                     *
      ******************************************************************
       01 COE.
           10 ENTIDAD                         PIC X(04).
           10 CENTRO-ALTA                     PIC X(04).
           10 CUENTA                          PIC X(12).
           10 DIVISA                          PIC X(03).
           10 CONCEPTO                        PIC X(04).
           10 FECHA-DESDE                     PIC X(10).
           10 PLAN                            PIC X(04).
           10 ESTADO                          PIC X(01).
           10 FECHA-HASTA                     PIC X(10).
           10 FECHA-CAMBIO-EST                PIC X(10).
           10 IND-COMESPECIF                  PIC X(01).
           10 FECHA-ULTLIQ                    PIC X(10).
           10 FECHA-PROLIQ                    PIC X(10).
           10 POR-REDUC                       PIC S9(3)V9(5) COMP-3 .
           10 POR-SECUND                      PIC S9(3)V9(5) COMP-3.
           10 ENTIDAD-SECUND                  PIC X(04).
           10 CENTRO-SECUND                   PIC X(04).
           10 CUENTA-SECUND                   PIC X(12).
           10 DIVISA-SECUND                   PIC X(03).
           10 IND-BOF                         PIC X(01).
           10 PERIOD-COM                      PIC X(01).
           10 PERIOD-COBR                     PIC X(01).
           10 CPO-LIBRE                       PIC S9(3) COMP-3 .
           10 COMI-IM                         PIC S9(13)V9(4) COMP-3.
           10 COMI-PO                         PIC S9(03)V9(5) COMP-3.
           10 COMI-MIN                        PIC S9(13)V9(4) COMP-3.
           10 COMI-MAX                        PIC S9(13)V9(4) COMP-3.
           10 DIV-COM                         PIC X(03).
           10 DIA-NAT-COBR                    PIC S9(3) COMP-3.
           10 DIAS-CALC-PROP                  PIC S9(3) COMP-3.
           10 ENTIDAD-UMO                     PIC X(04).
           10 CENTRO-UMO                      PIC X(04).
           10 USERID-UMO                      PIC X(08).
           10 NETNAME-UMO                     PIC X(08).
           10 TIMEST-UMO                      PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************

