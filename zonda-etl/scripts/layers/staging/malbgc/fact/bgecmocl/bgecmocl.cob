       ******************************************************************
      * DCLGEN TABLE(BGECMOCL)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGECMOCL))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGECMOCL COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGECMOCL                     *
      ******************************************************************
       02 OCL.
           10 ENTIDAD                      PIC X(4).
           10  CENTRO-ALTA                  PIC X(4).
           10  CUENTA                       PIC X(12).
           10  DIVISA                       PIC X(3).
           10  ENTIDAD-AGRUPADA             PIC X(4).
           10  CENTRO-ALTA-AGRUPADA         PIC X(4).
           10  CUENTA-AGRUPADA              PIC X(12).
           10  DIVISA-AGRUPADA              PIC X(3).
           10  ENTIDAD-PAQ                  PIC X(4).
           10  CENTRO-ALTA-PAQ              PIC X(4).
           10  PAQUETE                      PIC X(12).
           10  PIVOTE                       PIC X(1).
           10  PLAN                         PIC X(4).
           10  IND-PAQUETE                  PIC X(1).
           10  DIVISA-BIS                   PIC X(3).
           10  CONCEPTO                     PIC X(4).
           10  COMISION                     PIC X(4).
           10  IND-GRUPO                    PIC X(1).
           10  CODIGO                       PIC X(4).
           10  IMPORTE                      PIC S9(13)V9(2) COMP-3.
           10  FECHA-OPER                   PIC X(10).
           10  CHEQUE                       PIC S9(9) COMP-3.
           10  MINIBANCO                    PIC X(2).
           10  CANAL                        PIC X(2).
           10  ZONA                         PIC X(1).
           10  OPERACION                    PIC X(1).
           10  ENTIDAD-UMO                  PIC X(4).
           10  CENTRO-UMO                   PIC X(4).
           10  USERID-UMO                   PIC X(8).
           10  CAJERO-UMO                   PIC X(1).
           10  NETNAME-UMO                  PIC X(8).
           10  TIMEST-UMO                   PIC X(26).
           10  FILLER                       PIC X(32).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 34      *
      ******************************************************************

