01 BGECCDEP.                                                   
                                                               
   02 CDEP-CCC.                                                
      03 CDEP-ENTIDAD            PIC X(4).                     
      03 CDEP-CENTRO-ALTA        PIC X(4).                     
      03 CDEP-CUENTA             PIC X(12).                    
   02 CDEP-DIVISA                PIC X(3).                     
   02 CDEP-CONCEPTO              PIC X(4).                     
   02 CDEP-TIPO-PLAZA            PIC X(1).                     
   02 CDEP-IND-PRESENCIA         PIC X(1).                     
   02 CDEP-PRODUCTO              PIC X(2).                     
   02 CDEP-SUBPRODU              PIC X(4).                     
   02 CDEP-FECHA-PROCESO         PIC X(10).                    
   02 CDEP-BOLETA                PIC 9(8).                     
   02 CDEP-IMPORTE-CHEQUE        PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-COD-POSTAL            PIC 9(4).                     
   02 CDEP-NRO-CHEQUE            PIC X(8).                     
   02 CDEP-BANCO-CHEQUE          PIC 9(3).                     
   02 CDEP-SUCU-CHEQUE           PIC X(3).                     
   02 CDEP-CTA-CHEQUE            PIC X(11).                    
   02 CDEP-PLAZO                 PIC X(2).                     
   02 CDEP-COD-ORIGEN            PIC X(2).                     
   02 CDEP-CENTRO-ORIGEN         PIC X(4).                     
   02 CDEP-IMPORTE-FIJO          PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-PORCENTAJE            PIC S9(3)V9(5) USAGE COMP-3.  
   02 CDEP-IMPORTE-MINIMO        PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-IMPORTE-MAXIMO        PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-IMPORTE-COMISION      PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-IND-MINIMO            PIC X(1).                     
   02 CDEP-IND-MAXIMO            PIC X(1).                     
   02 CDEP-FECHA-COMISION        PIC X(10).                    
   02 CDEP-IND-TASA-ESPECIAL     PIC X(1).                     
      88 CDEP-IND-TASA-ESP-SI    VALUE 'S'.                    
      88 CDEP-IND-TASA-ESP-NO    VALUE 'N'.                    
   02 CDEP-TASA-ESPECIAL         PIC S9(3)V9(5) USAGE COMP-3.  
   02 CDEP-CODIGO                PIC X(4).                     
   02 CDEP-IMPORTE-IMPUESTO      PIC S9(13)V9(2) USAGE COMP-3. 
   02 CDEP-RESULTADO             PIC X(1).                     
      88 CDEP-RESULTADO-OK       VALUE 'O'.
      88 CDEP-RESULTADO-ERROR    VALUE 'E'.
   02 CDEP-CCC-CARGO.                      
      03 CDEP-ENTIDAD-CARGO      PIC X(4). 
      03 CDEP-CENTRO-ALTA-CARGO  PIC X(4). 
      03 CDEP-CUENTA-CARGO       PIC X(12).
   02 CDEP-DIVISA-CARGO          PIC X(3). 
   02 CDEP-PRODUCTO-CARGO        PIC X(2). 
   02 CDEP-SUBPRODU-CARGO        PIC X(4). 
   02 CDEP-NRO-EMPRESA           PIC 9(5). 
   02 FILLER                     PIC X(36).