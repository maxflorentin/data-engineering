      ******************************************************************
      * DCLGEN TABLE(BGTCMCO)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCMCO))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCMCO COMMAND THAT MADE THE FOLLOWING STATEMENTS  *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCMCO                     *
      ******************************************************************
       02 MCO.
           10 NUM-CONVENIO     PIC S9(07) USAGE COMP-3.
           10 CONVENIO         PIC X(04).
           10 PRODUCTO         PIC X(02).
           10 SUBPRODU         PIC X(04).
           10 SUSCRIPTOR       PIC X(11).
           10 ENTIDAD          PIC X(04).
           10 CENTRO-ALTA      PIC X(04).
           10 CUENTA           PIC X(12).
           10 DIVISA           PIC X(03).
           10 INDESTA          PIC X(01).
           10 FECHA-EST        PIC X(10).
           10 NUM-MES          PIC S9(03) USAGE COMP-3.
           10 SALDO-MEDIO      PIC S9(13)V9(2) USAGE COMP-3.
           10 SALDO-MED-CV     PIC S9(13)V9(4) USAGE COMP-3.
           10 TIPO-CONVENIO    PIC X(01).
           10 NUM-ASOCIADOS    PIC S9(05) USAGE COMP-3.
           10 DIAS-VIGENCIA    PIC S9(03) USAGE COMP-3.
           10 ENTIDAD-UMO      PIC X(04).
           10 CENTRO-UMO       PIC X(04).
           10 USERID-UMO       PIC X(08).
           10 NETNAME-UMO      PIC X(08).
           10 TIMEST-UMO       PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************