       ******************************************************************
      * DCLGEN TABLE(BGECLCO)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGECLCO))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGECLCO COMMAND THAT MADE THE FOLLOWING STATEMENTS   *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGECLCO                     *
      ******************************************************************
       02 LCO.
           10 ENTIDAD                        PIC X(04).
           10 CENTRO-ALTA                    PIC X(04).
           10 CUENTA                         PIC X(12).
           10 DIVISA                         PIC X(03).
           10 ENTIDAD-CARGO                  PIC X(04).
           10 CENTRO-CARGO                   PIC X(04).
           10 CUENTA-CARGO                   PIC X(12).
           10 DIVISA-CARGO                   PIC X(03).
           10 REFER-LIQ                      PIC 9(09) COMP-3.
           10 PRODUCTO                       PIC X(02).
           10 SUBPRODU                       PIC X(04).
           10 IND-SOBREGIRO                  PIC X(01).
           10 PRODUCTO-CARGO                 PIC X(02).
           10 SUBPRODU-CARGO                 PIC X(04).
           10 IND-SOBREGIRO-CARGO            PIC X(01).
           10 PLAN                           PIC X(04).
           10 CONCEPTO                       PIC X(04).
           10 CONCEPTO-IMPUESTO              PIC X(04).
           10 COMISION                       PIC X(04).
           10 ZONA                           PIC X(01).
           10 OPERACION                      PIC X(01).
           10 BASE-CALCULO                   PIC S9(13)V9(4) COMP-3.
           10 NUM-UNID                       PIC S9(09) COMP-3.
           10 MOV-LIBRES                     PIC 9(03).
           10 COMI-IM                        PIC S9(13)V9(4) COMP-3.
           10 COMI-PO                        PIC S9(03)V9(5) COMP-3.
           10 COMI-MIN                       PIC S9(13)V9(4) COMP-3.
           10 COMI-MAX                       PIC S9(13)V9(4) COMP-3.
           10 IMPORTE                        PIC S9(13)V9(4) COMP-3.
           10 DIV-IMPORTE                    PIC X(03).
           10 FECHA-MOV                      PIC X(10).
           10 FECHA                          PIC X(10).
           10 IMPORTE-CAP                    PIC S9(13)V9(2) COMP-3.
           10 RESUL-CAP                      PIC X(1).
           10 CODIGO                         PIC X(04).
           10 IND-COM-ESPECIAL               PIC X(01).
           10 NRO-CHEQUE                     PIC X(08).
           10 BOLETA                         PIC 9(08).
           10 CENTRO-CONTAB                  PIC X(04).
           10 CENTRO-CONTAB-CARGO            PIC X(04).
           10 PRODUCTO-CONTAB                PIC X(02).
           10 SUBPRODU-CONTAB                PIC X(04).
           10 IMPORTE-TOTAL                  PIC S9(13)V9(2) COMP-3.
           10 FECHA-ULTLIQ                   PIC X(10).
           10 FECHA-PROLIQ                   PIC X(10).
           10 CODIGO-DESCRIPCION             PIC X(34).
           10 TARJETA                        PIC X(19).
           10 TITULARIDAD                    PIC X(02).
           10 RED                            PIC X(04).
           10 NUM-CONVENIO                   PIC S9(7) COMP-3.
           10 FILLER                         PIC X(48).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 51      *
      ******************************************************************