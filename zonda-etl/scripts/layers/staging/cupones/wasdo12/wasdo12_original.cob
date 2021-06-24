*---+----------------------------------------------------*                
      * TOMADO DE NUEVA VERSION DE COPY AGRANDADO                               
      * (DE CAPGEMINI - SDO3000346 - B022471)                                   
      *--------------------------------------------------------*                
      ******* FORMATO DEL ACUMULADO DE PAGOS POR BIMESTRE   ****                
      ******* SIN REGISTRO DE CONTROL                       ****                
      *--------------------------------------------------------*                
      ******* LONG.REG..... .= 80                                               
      ******* TIPO.REG.......= FIJO                                             
      ******* CLAVE PPAL: 01,27                                                 
      *--------------------------------------------------------*                
      * FECHA: 06/08/2012                                                       
      * MODIFICACION: ID 4107 - OBSOLENCENCIA, SE MODIFICA                      
      *               LA LONGITUD DE LOS FICHEROS WASDO10,                      
      *               WASDO20 Y WASDO12                                         
      * REF: CAP01-INI / CAP01-FIN                                              
      *--------------------------------------------------------*                
      *ISBAN-0001-I                                                             
      * MODIFICACION: SE AGREGA NRO.DE RELACION DE CPTES                        
      *               PARA PAGO SIMULTANEO ($ Y U$S)                            
      * FECHA: 30/08/2012. MONICA DONNIAN                                       
      *ISBAN-0001-F                                                             
      *--------------------------------------------------------*                
      *SANTEC-0001-I                                                            
      * MODIFICACION: SE AGREGA MARCA DE PAGO EN EFECTIVO                       
      * FECHA: 05/08/2019. SERGIO RUSSOLO                                       
      *SANTEC-0001-F                                                            
      *--------------------------------------------------------*                
      *SANTEC-0002-I                                                            
      * MODIFICACION: SE AGREGA MARCA GENERACION DE BOLETO ONLINE.              
      *               SE UTILIZARA PARA DETECTAR UN PAGO EN PESOS PERO          
      *               SE REGISTRA COMO PAGO EN DOLAR POR COM. A 6664            
      * FECHA: 02/01/2020. SERGIO RUSSOLO                                       
      *SANTEC-0002-F                                                            
      *--------------------------------------------------------*                
      *ISBAN-0007 TAS-PRISMA                                                    
      *--------------------------------------------------------*                
       01  WASDO12.                                                             
         03  WASDO12-CLAVE.                                                     
      *                                                         001-015         
           05  WASDO12-CUENTA-CREDITO.                                          
             07  WASDO12-TIPO-CUENTA       PIC  9(01).                          
             07  WASDO12-NRO-CUENTA        PIC  9(14).                          
                                                                                
           05  WASDO12-FECHA-PAGO.                                              
      *                                                         016-023         
             07  WASDO12-F-PAGO-AA1        PIC  9(02).                          
             07  WASDO12-F-PAGO-AA2        PIC  9(02).                          
             07  WASDO12-F-PAGO-MM         PIC  9(02).                          
             07  WASDO12-F-PAGO-DD         PIC  9(02).                          
                                                                                
           05  WASDO12-NRO-SEQ           PIC  9(07) COMP-3.                     
      *                                                         024-027         
         03 WASDO12-IMPORTE              PIC  9(15)V99 COMP-3.                  
      *                                                         028-036         
         03 WASDO12-EMPRESA              PIC  9(05).                            
      *                                                         037-041         
         03 WASDO12-SUC-RECAUD           PIC  9(03).                            
      *                                                         042-044         
         03 WASDO12-ORIGEN-MOV           PIC  X(04).                            
      *     ( 'ADOM' DEBITO DIRECTO )                           045-048         
      *     ( 'AREC' RECAUDACIONES  )                                           
                                                                                
         03 WASDO12-MONEDA               PIC  9(02).                            
      *                                                         049-050         
           88 WASDO12-AUSTRALES VALUE 00.                                       
           88 WASDO12-DOLARES VALUE 01.                                         
                                                                                
         03 WASDO12-NRO-COMPROBANTE      PIC  X(08).                            
      *                                                         051-058         
         03 WASDO12-ESTADO                  PIC X(01).                          
            88 ACEPTADO                     VALUE 'A'.                          
            88 RECHAZADO                    VALUE 'R'.                          
            88 REENVIADO                    VALUE 'V'.                          
            88 ENVIADO                      VALUE 'E'.                          
            88 REVERSA                      VALUE 'Z'.                          
      *ISBAN-0007-I                                                             
            88 TAS-PRISMA                   VALUE 'T'.                          
            88 TAS-PRISMA-REVERSA           VALUE 'Q'.                          
      *ISBAN-0007-F                                                             
      *                                                         059-059         
         03 WASDO12-ESTADO-FECHA         PIC  9(08).                            
      *                                                         060-067         
         03 WASDO12-CARTERA              PIC  9(02).                            
      *                                                         068-069         
         03 WASDO12-IDENT-OPE.                                                  
           05  WASDO12-IDENT-CANAL       PIC X(2).                              
           05  WASDO12-IDENT-FECHA       PIC 9(7) COMP-3.                       
           05  WASDO12-IDENT-NRO         PIC 9(9) COMP-3.                       
      *ISBAN-0001-I                                                             
         03  WASDO12-NRO-REL-SIMUL       PIC  X(08).                            
      *ISBAN-0001-F                                                             
      *ISBAN-0001-I                                                             
      *SANTEC-0001-I                                                            
         03  WASDO12-IND-PAGO-EFECTIVO   PIC X(01).                             
      *                                                         089-089         
      *SANTEC-0002-I                                                            
         03  WASDO12-IND-BOLETO-ONLINE   PIC X(02).                             
      *        'BO' --> BOLETO ONLINE                                           
      *        '  ' --> PAGO EN LA MONEDA QUE SE REGISTRA                       
      *                                                         090-091         
         03  WASDO12-IND-PAGO-BOLETO     PIC X(01).                             
      *        'X' --> PAGO CON BOLETO ONLINE                                   
      *        ' ' --> PAGO EN LA MONEDA QUE SE REGISTRA                        
      *                                                         092-092         
      * CAP01-INI                                                               
      *++03  FILLER                      PIC X(70).                             
      *  03  FILLER                      PIC X(62).                             
      *  03  FILLER                      PIC X(61).                             
         03  FILLER                      PIC X(58).                             
      * CAP01-FIN                                                               
      *SANTEC-0001-F                                                            
      *SANTEC-0002-F                                                            
      *ISBAN-0001-F                                                             
      *          