       ******************************************************************
      * DCLGEN TABLE(BGGTPAB)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGGTPAB))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGGTPAB COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGDTOBS                     *
      ******************************************************************
       05 PAB.
           10 NUM-CONVENIO                 PIC S9(07) USAGE COMP-3.
           10 CONCEPTO                     PIC X(04).
           10 PORC-SUSCRIPTOR              PIC S9(3)V9(5) USAGE COMP-3.
           10 PORC-ENTIDAD                 PIC S9(3)V9(5) USAGE COMP-3.
           10 PORC-CLIENTE                 PIC S9(3)V9(5) USAGE COMP-3.
           10 ENTIDAD                      PIC X(04).
           10 CENTRO-ALTA                  PIC X(04).
           10 CUENTA                       PIC X(12).
           10 DIVISA                       PIC X(03).
           10 INDESTA                      PIC X(01).
           10 FECHA-EST                    PIC X(10).
           10 ENTIDAD-UMO                  PIC X(04).
           10 CENTRO-UMO                   PIC X(04).
           10 USERID-UMO                   PIC X(08).
           10 NETNAME-UMO                  PIC X(08).
           10 TIMEST-UMO                   PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 16      *
      ******************************************************************