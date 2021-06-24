01  BGTCIMP.                                                  
                                                              
    05  IMP-CLAVE.                                            
                                                              
        10  IMP-CCC.                                          
                                                              
            15  IMP-ENTIDAD           PIC X(04).              
            15  IMP-CENTRO-ALTA       PIC X(04).              
            15  IMP-CUENTA            PIC X(12).              
                                                              
        10  IMP-DIVISA                PIC X(03).              
        10  IMP-SECUIMP               PIC S9(05)       COMP-3.
        10  IMP-SECUIMP-ADICI         PIC S9(01)       COMP-3.
                                                              
    05  IMP-DATOS-ALTA.                                       
                                                              
        10  IMP-CONCEPTO              PIC X(04).              
        10  IMP-FECHA-IMP             PIC X(10).              
        10  IMP-PRIORIDAD             PIC X(02).              
        10  IMP-IMPORTE-IMPAGO        PIC S9(13)V9(4) COMP-3. 
        10  IMP-CODOPER-IMPAGO        PIC X(04).              
        10  IMP-BASE-IMPUESTO         PIC S9(13)V9(2) COMP-3. 
        10  IMP-IMPUESTO              PIC S9(13)V9(4) COMP-3. 
        10  IMP-CODOPE-IMPUEST        PIC X(04).              
        10  IMP-CPTO-IMPUESTO         PIC X(04).              
        10  IMP-IND-DEC-DIV           PIC X.                  
                                                              
            88  IMP-88-DIV-SIN-DEC           VALUE '0'.       
            88  IMP-88-DIV-DOS-DEC           VALUE '2'.       
            88  IMP-88-DIV-CUA-DEC           VALUE '4'.       

                                                             
    05  IMP-DATOS-RESOL.                                     
                                                             
        10  IMP-FECHA-COBRO           PIC X(10).             
        10  IMP-IMPORT-TOTCOB         PIC S9(13)V9(4) COMP-3.
        10  IMP-IMPAGO-COBRO          PIC S9(13)V9(4) COMP-3.
        10  IMP-IMPU-COBRO            PIC S9(13)V9(4) COMP-3.    
                                                             
    05  IMP-DATOS-SITU.                                      
                                                             
        10  IMP-ESTADO                  PIC X.               
            88 IMP-ESTADO-ACTIVO             VALUE 'A'.      
            88 IMP-ESTADO-ACTIVO-OPS         VALUE 'O'.      
            88 IMP-ESTADO-PAGADO             VALUE 'P'.      
            88 IMP-ESTADO-ANULADO            VALUE 'N'.      
                                                             
    05 IMP-NUEVO-PAQUETE.                                    
       10 IMP-COD-OPER-PPAL            PIC X(4).             
                                                             
                                                                  
    05  IMP-STAMP.                                           
                                                             
        10 IMP-ENTIDAD-UMO             PIC X(04).            
        10 IMP-CENTRO-UMO              PIC X(04).            
        10 IMP-USERID-UMO              PIC X(08).
        10 IMP-NETNAME-UMO             PIC X(08).
        10 IMP-TIMEST-UMO              PIC X(26).
                                                    
