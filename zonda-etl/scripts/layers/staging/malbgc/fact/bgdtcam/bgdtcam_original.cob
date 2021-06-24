01  BGTCCAM.                                                     
                                                                 
    05 CAM-CLAVE.                                                
       10 CAM-CAMPANIA              PIC X(04).                   
       10 CAM-FECHA-DESDE           PIC X(10).                   
                                                                 
    05 CAM-CLAVE-ALTERNATIVA.                                    
       10 CAM-PRODUCTO              PIC X(02).                   
       10 CAM-SUBPRODU              PIC X(04).                   
                                                                 
    05 CAM-DATOS.                                                
       10 CAM-FECHA-HASTA           PIC X(10).                   
       10 CAM-PLAZO                 PIC S9(03) USAGE COMP-3.     
       10 CAM-FECHA-VTO             PIC X(10).                   
       10 CAM-TARIFA                PIC X(04).                   
       10 CAM-PERIOD-LIQ            PIC X(01).                   
          88 CAM-PERIOD-LIQ-DIAR               VALUE 'D'.        
          88 CAM-PERIOD-LIQ-SEMA               VALUE 'W'.        
          88 CAM-PERIOD-LIQ-QUIN               VALUE 'Q'.        
          88 CAM-PERIOD-LIQ-MENS               VALUE 'M'.        
          88 CAM-PERIOD-LIQ-BIME               VALUE 'B'.        
          88 CAM-PERIOD-LIQ-TRIM               VALUE 'T'.        
          88 CAM-PERIOD-LIQ-SEME               VALUE 'S'.        
          88 CAM-PERIOD-LIQ-ANUA               VALUE 'A'.        
          88 CAM-PERIOD-LIQ-VENC               VALUE 'V'.        
       10 CAM-DIVISA                PIC X(03).                   
       10 CAM-PLAN                  PIC X(04).                   
       10 CAM-DESCRIPCION           PIC X(40).                   
       10 CAM-CLASE-LIQ             PIC X(01).                   
          88 CAM-CLASE-LIQ-CONT                VALUE 'C'.        
          88 CAM-CLASE-LIQ-SINT                VALUE 'S'.                                                         
          88 CAM-CLASE-LIQ-PRESTAMO            VALUE 'P'.                                                          
       10 CAM-CLASE-TAF             PIC X(01).                   
          88 CAM-CLASE-TAF-NORM                VALUE 'N'.        
          88 CAM-CLASE-TAF-PROR                VALUE 'P'.        
       10 CAM-PERIODO-TAR           PIC X(01).                   
          88 CAM-PERIOD-TAR-MENS               VALUE 'M'.
          88 CAM-PERIOD-TAR-BIME               VALUE 'B'.
          88 CAM-PERIOD-TAR-TRIM               VALUE 'T'.
          88 CAM-PERIOD-TAR-SEME               VALUE 'S'.
          88 CAM-PERIOD-TAR-ANUA               VALUE 'A'.
          88 CAM-PERIOD-TAR-LIQU               VALUE 'L'.
          88 CAM-PERIOD-TAR-COEF               VALUE 'C'.
       10 CAM-INDESTA               PIC X(01).           
          88 CAM-INDESTA-OFER                  VALUE 'O'.
          88 CAM-INDESTA-DESO                  VALUE 'D'.
          88 CAM-INDESTA-CANCE                 VALUE 'C'.
       10 CAM-FECHA-ESTADO          PIC X(10).           
                                                         
    05 CAM-STAMP.                                        
       10 CAM-ENTIDAD-UMO           PIC X(04).           
       10 CAM-CENTRO-UMO            PIC X(04).           
       10 CAM-USERID-UMO            PIC X(08).           
       10 CAM-NETNAME-UMO           PIC X(08).           
       10 CAM-TIMEST-UMO            PIC X(26).           
