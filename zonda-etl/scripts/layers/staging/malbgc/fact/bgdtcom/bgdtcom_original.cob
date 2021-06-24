01  BGTCCOM.                                                     
                                                                 
    05 COM-CLAVE.                                                
                                                                 
       10 COM-CONCEPTO         PIC  X(04).                       
       10 COM-COMISION         PIC  X(04).                       
       10 COM-FECHA-DESDE      PIC  X(10).                       
                                                                 
    05 COM-DATOS.                                                
                                                                 
       10 COM-FECHA-HASTA      PIC  X(10).                       
       10 COM-ESTADO           PIC  X(01).                       
          88 COM-CT-ESTADO-A                    VALUE 'A'.       
          88 COM-CT-ESTADO-D                    VALUE 'D'.       
       10 COM-FECHA-ESTADO     PIC  X(10).                       
       10 COM-PERIOD-COM       PIC  X(01).                       
          88 COM-PERIOD-COM-P                   VALUE 'P'.       
          88 COM-PERIOD-COM-T                   VALUE 'T'.       
          88 COM-PERIOD-COM-C                   VALUE 'C'.       
          88 COM-PERIOD-COM-J                   VALUE 'J'.       
       10 COM-DIVISA           PIC  X(03).                       
       10 COM-PERIOD-COBR      PIC  X(01).                       
          88 COM-PERIOD-COBR-D                  VALUE 'D'.       
          88 COM-PERIOD-COBR-W                  VALUE 'W'.       
          88 COM-PERIOD-COBR-Q                  VALUE 'Q'.       
          88 COM-PERIOD-COBR-M                  VALUE 'M'.       
          88 COM-PERIOD-COBR-B                  VALUE 'B'.       
          88 COM-PERIOD-COBR-T                  VALUE 'T'.       
          88 COM-PERIOD-COBR-S                  VALUE 'S'.       
          88 COM-PERIOD-COBR-A                  VALUE 'A'.       
          88 COM-PERIOD-COBR-V                  VALUE 'V'.       
       10 COM-CPO-LIBRE        PIC S9(03)       USAGE COMP-3.    
       10 COM-COMI-IM          PIC S9(13)V9(04) USAGE COMP-3.    
       10 COM-COMI-PO          PIC S9(03)V9(05) USAGE COMP-3.    
       10 COM-COMI-MIN         PIC S9(13)V9(04) USAGE COMP-3.    
       10 COM-COMI-MAX         PIC S9(13)V9(04) USAGE COMP-3.    
       10 COM-DIA-NAT-COBR     PIC S9(02)       USAGE COMP-3.    
       10 COM-IND-TRAMOS       PIC  X(01).                       
          88 COM-IND-TRAMOS-CON                 VALUE 'C'.   
          88 COM-IND-TRAMOS-SIN                 VALUE 'S'.   
       10 COM-IND-BOF          PIC  X(01).                   
       10 COM-DIAS-CALC-PROP   PIC S9(03)       USAGE COMP-3.
                                                             
    05 COM-STAMP.                                            
                                                             
       10 COM-ENTIDAD-UMO      PIC  X(04).                   
       10 COM-CENTRO-UMO       PIC  X(04).                   
       10 COM-USERID-UMO       PIC  X(08).                   
       10 COM-NETNAME-UMO      PIC  X(08).                   
       10 COM-TIMEST-UMO       PIC  X(26).