       ******************************************************************
      * DCLGEN TABLE(BGTCCOM)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGTCCOM))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGTCCOM COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGTCCOM                     *
      ******************************************************************
       01 COM.
           10 CONCEPTO             PIC X(04).
           10 COMISION             PIC X(04).
           10 FECHA-DESDE          PIC X(10).
           10 FECHA-HASTA          PIC X(10).
           10 ESTADO               PIC X(01).
           10 FECHA-ESTADO         PIC X(10).
           10 PERIOD-COM           PIC X(01).
           10 DIVISA               PIC X(03).
           10 PERIOD-COBR          PIC X(01).
           10 CPO-LIBRE            PIC S9(03) USAGE COMP-3.
           10 COMI-IM              PIC S9(13)V9(04) USAGE COMP-3.
           10 COMI-PO              PIC S9(03)V9(05) USAGE COMP-3.
           10 COMI-MIN             PIC S9(13)V9(04) USAGE COMP-3.
           10 COMI-MAX             PIC S9(13)V9(04) USAGE COMP-3.
           10 DIA-NAT-COBR         PIC S9(02) USAGE COMP-3.
           10 IND-TRAMOS           PIC X(01).
           10 IND-BOF              PIC X(01).
           10 DIAS-CALC-PROP       PIC S9(03) USAGE COMP-3.
           10 ENTIDAD-UMO          PIC X(04).
           10 CENTRO-UMO           PIC X(04).
           10 USERID-UMO           PIC X(08).
           10 NETNAME-UMO          PIC X(08).
           10 TIMEST-UMO           PIC X(26).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 12      *
      ******************************************************************
                
                
                01 BGTCCOM.
