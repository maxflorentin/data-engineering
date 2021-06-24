      ******************************************************************
      * DCLGEN TABLE(BGECCDEP)                                          *
      *        LIBRARY(OPI.CPF.DCLGEN(BGECCDEP))                        *
      *        ACTION(REPLACE)                                         *
      *        APOST                                                   *
      * ... IS THE BGECCDEP COMMAND THAT MADE THE FOLLOWING STATEMENTS  *
      ******************************************************************

      ******************************************************************
      * COBOL DECLARATION FOR TABLE OPIGD1.BGECCDEP                     *
      ******************************************************************
       01 DEP.
           02 ENTIDAD                    PIC X(4).
           02 CENTRO-ALTA                PIC X(4).
           02 CUENTA                     PIC X(12).
           02 DIVISA                     PIC X(3).
           02 CONCEPTO                   PIC X(4).
           02 TIPO-PLAZA                 PIC X(1).
           02 IND-PRESENCIA              PIC X(1).
           02 PRODUCTO                   PIC X(2).
           02 SUBPRODU                   PIC X(4).
           02 FECHA-PROCESO              PIC X(10).
           02 BOLETA                     PIC 9(8).
           02 IMPORTE-CHEQUE             PIC S9(13)V9(2) USAGE COMP-3. 
           02 COD-POSTAL                 PIC 9(4). 
           02 NRO-CHEQUE                 PIC X(8).
           02 BANCO-CHEQUE               PIC 9(3).    
           02 SUCU-CHEQUE                PIC X(3).
           02 CTA-CHEQUE                 PIC X(11).
           02 PLAZO                      PIC X(2).
           02 COD-ORIGEN                 PIC X(2).
           02 CENTRO-ORIGEN              PIC X(4).
           02 IMPORTE-FIJO               PIC S9(13)V9(2) USAGE COMP-3. 
           02 PORCENTAJE                 PIC S9(3)V9(5) USAGE COMP-3.
           02 IMPORTE-MINIMO             PIC S9(13)V9(2) USAGE COMP-3. 
           02 IMPORTE-MAXIMO             PIC S9(13)V9(2) USAGE COMP-3. 
           02 IMPORTE-COMISION           PIC S9(13)V9(2) USAGE COMP-3. 
           02 IND-MINIMO                 PIC X(1).
           02 IND-MAXIMO                 PIC X(1).
           02 FECHA-COMISION             PIC X(10).
           02 IND-TASA-ESPECIAL          PIC X(1).
           02 TASA-ESPECIAL              PIC S9(3)V9(5) USAGE COMP-3.
           02 CODIGO                     PIC X(4).
           02 IMPORTE-IMPUESTO           PIC S9(13)V9(2) USAGE COMP-3.
           02 RESULTADO                  PIC X(1).
           02 ENTIDAD-CARGO              PIC X(4).
           02 CENTRO-ALTA-CARGO          PIC X(4).
           02 CUENTA-CARGO               PIC X(12).
           02 DIVISA-CARGO               PIC X(3).
           02 PRODUCTO-CARGO             PIC X(2).
           02 SUBPRODU-CARGO             PIC X(4).
           02 NRO-EMPRESA                PIC 9(5).
           02 FILLER                     PIC X(36).
      ******************************************************************
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 14      *
      ******************************************************************

            