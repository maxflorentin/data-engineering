01  BGTCTRA.                                                   
                                                               
    05 TRA-CLAVE.                                              
                                                               
       10 TRA-CONCEPTO         PIC X(4).                       
       10 TRA-COMISION         PIC X(4).                       
       10 TRA-FECHA-DESDE      PIC X(10).                      
       10 TRA-SALDO-HASTA      PIC S9(13)V9(4) USAGE COMP-3.   
                                                               
    05 TRA-DATOS.                                              
                                                               
       10 TRA-FECHA-HASTA      PIC X(10).                      
       10 TRA-COMI-IM          PIC S9(13)V9(4) USAGE COMP-3.   
       10 TRA-COMI-PO          PIC S9(3)V9(5) USAGE COMP-3.    
       10 TRA-COMI-MAX         PIC S9(13)V9(4) USAGE COMP-3.   
       10 TRA-COMI-MIN         PIC S9(13)V9(4) USAGE COMP-3.   
                                                               
    05 TRA-STAMP.                                              
                                                               
       10 TRA-ENTIDAD-UMO      PIC X(4).                       
       10 TRA-CENTRO-UMO       PIC X(4).                       
       10 TRA-USERID-UMO       PIC X(8).                       
       10 TRA-NETNAME-UMO      PIC X(8).                       
       10 TRA-TIMEST-UMO       PIC X(26).