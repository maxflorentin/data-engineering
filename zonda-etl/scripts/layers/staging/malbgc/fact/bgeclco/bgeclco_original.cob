 02  BGECLCO.                                                  
                                                               
     05  LCO-CCC.                                              
         10  LCO-ENTIDAD               PIC  X(04).             
         10  LCO-CENTRO-ALTA           PIC  X(04).             
         10  LCO-CUENTA                PIC  X(12).             
     05  LCO-DIVISA                    PIC  X(03).             
                                                               
     05  LCO-CCC-CARGO.                                        
         10  LCO-ENTIDAD-CARGO         PIC  X(04).             
         10  LCO-CENTRO-CARGO          PIC  X(04).             
         10  LCO-CUENTA-CARGO          PIC  X(12).             
     05  LCO-DIVISA-CARGO              PIC  X(03).             
                                                               
     05  LCO-REFER-LIQ                 PIC  9(09)       COMP-3.
     05  LCO-PRODUCTO                  PIC  X(02).             
     05  LCO-SUBPRODU                  PIC  X(04).             
     05  LCO-IND-SOBREGIRO             PIC  X(01).             
     05  LCO-PRODUCTO-CARGO            PIC  X(02).             
     05  LCO-SUBPRODU-CARGO            PIC  X(04).             
     05  LCO-IND-SOBREGIRO-CARGO       PIC  X(01).             
     05  LCO-PLAN                      PIC  X(04).             
     05  LCO-CONCEPTO                  PIC  X(04).             
     05  LCO-CONCEPTO-IMPUESTO         PIC  X(04).             
     05  LCO-COMISION                  PIC  X(04).             
     05  LCO-ZONA                      PIC  X(01).             
     05  LCO-OPERACION                 PIC  X(01).             
     05  LCO-BASE-CALCULO              PIC S9(13)V9(4) COMP-3. 
     05  LCO-NUM-UNID                  PIC S9(09)      COMP-3. 
     05  LCO-MOV-LIBRES                PIC  9(03).             
     05  LCO-COMI-IM                   PIC S9(13)V9(4) COMP-3. 
     05  LCO-COMI-PO                   PIC S9(03)V9(5) COMP-3. 
     05  LCO-COMI-MIN                  PIC S9(13)V9(4) COMP-3. 
     05  LCO-COMI-MAX                  PIC S9(13)V9(4) COMP-3. 
     05  LCO-IMPORTE                   PIC S9(13)V9(4) COMP-3. 
     05  LCO-DIV-IMPORTE               PIC  X(03).             
     05  LCO-FECHA-MOV                 PIC  X(10).             
     05  LCO-FECHA                     PIC  X(10).             
     05  LCO-IMPORTE-CAP               PIC  S9(13)V9(2) COMP-3.
     05  LCO-RESUL-CAP                 PIC  X(1).              
         88  LCO-RESUL-CAP-OK               VALUE 'O'.         
         88  LCO-RESUL-CAP-ERROR            VALUE 'E'.         
     05  LCO-CODIGO                    PIC  X(04).             
     05  LCO-IND-COM-ESPECIAL          PIC X(01).              
         88 LCO-IND-COM-ESP-SI         VALUE 'S'.              
         88 LCO-IND-COM-ESP-NO         VALUE 'N'.              
     05  LCO-NRO-CHEQUE                PIC  X(08).             
     05  LCO-BOLETA                    PIC  9(08).             
     05  LCO-CENTRO-CONTAB             PIC  X(04).             
     05  LCO-CENTRO-CONTAB-CARGO       PIC  X(04).             
     05  LCO-PRODUCTO-CONTAB           PIC  X(02).             
     05  LCO-SUBPRODU-CONTAB           PIC  X(04).             
     05  LCO-IMPORTE-TOTAL             PIC  S9(13)V9(2) COMP-3.
     05  LCO-FECHA-ULTLIQ              PIC  X(10).             
     05  LCO-FECHA-PROLIQ              PIC  X(10).             
     05  LCO-CODIGO-DESCRIPCION        PIC  X(34).             
     05  LCO-TARJETA                   PIC  X(19).             
     05  LCO-TITULARIDAD               PIC  X(02).             
     05  LCO-RED                       PIC  X(04).             
     05  LCO-NUM-CONVENIO              PIC  S9(7) COMP-3.      
     05  LCO-FILLER                    PIC  X(48).             
