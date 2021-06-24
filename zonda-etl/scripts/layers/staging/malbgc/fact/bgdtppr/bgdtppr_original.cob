01  BGTCPPR.                                            
                                                        
    05 PPR-CLAVE.                                       
                                                        
       10 PPR-PLAN                PIC X(04).            
       10 PPR-CONCEPTO            PIC X(04).            
       10 PPR-PCO-ECU-PER         PIC X(01).            
       10 PPR-PCO-ECU-SOP         PIC X(01).            
          88 PPR-PCO-ECU-SOP-PAP              VALUE 'P'.
          88 PPR-PCO-ECU-SOP-BAN              VALUE 'B'.
          88 PPR-PCO-ECU-SOP-DIS              VALUE 'D'.
       10 PPR-FECHA-DESDE         PIC X(10).            
                                                        
    05 PPR-DATOS.                                       
                                                        
       10 PPR-FECHA-HASTA         PIC X(10).            
       10 PPR-COMISION            PIC X(04).            
                                                        
    05 PPR-STAMP.                                       
                                                        
       10 PPR-ENTIDAD-UMO         PIC X(4).             
       10 PPR-CENTRO-UMO          PIC X(4).             
       10 PPR-USERID-UMO          PIC X(8).             
       10 PPR-NETNAME-UMO         PIC X(8).             
       10 PPR-TIMEST-UMO          PIC X(26).