      ******************************************************************        
      *                     TRIAD                                      *        
      *    DESCRIPCION: STANDARD REPORT RECORD - HEADER                *        
      ******************************************************************        
      *    COPY FICO  : TRDRSTSR                                       *        
      *    COPY LAKE  : RRHST                                          *        
      *    LONGUITUD  :                                                *        
      ******************************************************************        
           05  RR-HE-CNTL.                                                      
               10  RR-HE-LENGTH                    PIC S9(08) BINARY.           
                   88  RR-HE-LENGTH-SET            VALUE +149.          T914    
                                                                                
           05  RR-HE-HEADER.                                                    
               10  RR-HE-SUBJECT-CODE              PIC  X(01).                  
                   88  RR-HE-SUBJ-HEADER           VALUE 'H'.                   
               10  RR-HE-TENANT-ID                 PIC S9(08) BINARY.           
               10  RR-HE-SPID                      PIC S9(03) BINARY.           
               10  RR-HE-PROC-MODE                 PIC  X(10).                  
                   88 RR-HE-PRODUCTION             VALUE 'P         '.          
      ***  NOT USED FOR T9. SET TO BLANK. S/B 9(05).                            
               10  RR-HE-COMP-ERR-HE-EXPT-ID       PIC  X(05).                  
               10  RR-HE-PROC-DATE-CYMD            PIC  9(08) BINARY.           
               10  RR-HE-CUSTOMER-ID               PIC  X(20).                  
               10  RR-HE-PREV-CUSTOMER-ID          PIC  X(20).                  
               10  RR-HE-ACCOUNT-ID                PIC  X(20).                  
               10  RR-HE-PREV-ACCOUNT-ID           PIC  X(20).                  
               10  RR-HE-CALL-TYPE                 PIC  X(01).                  
                   88  RR-HE-CALL-OPEN             VALUE LOW-VALUES.            
                   88  RR-HE-CALL-CLOSE            VALUE HIGH-VALUES.           
                   88  RR-HE-CALL-UNSET            VALUE SPACES.                
                   88  RR-HE-COLLECTIONS-CALL VALUE 'C'.                        
                   88  RR-HE-EVENT-CALL            VALUE 'E'.                   
                   88  RR-HE-EVENT-COLL-CALL       VALUE 'T'.                   
                   88  RR-HE-CONF-CALL             VALUE '1' '2' '3'            
                                                         '4' '5' '6'            
                                                         '7' '8' '9'            
                                                         '0' 'H' 'I'            
                                                         'J' 'K' 'L'.           
                   88  RR-HE-REVW-CALL             VALUE 'R'.                   
                   88  RR-HE-REVW-COLL-CALL        VALUE 'W'.                   
                   88  RR-HE-NEW-CALL              VALUE 'N'.                   
               10  RR-HE-SAMPL-FTR                 PIC  S9(06) BINARY.          
               10  RR-HE-CO-PROD-TYPE              PIC   9(01).                 
               10  RR-HE-VERIFICATION-ID           PIC   X(33).                 
                                                                                
           05  ST-REC-CNTL.                                                     
               10  ST-LENGTH                      PIC S9(08) BINARY.            
                   88  ST-LENGTH-VALUE            VALUE +6930.                  
                                                                                
           05  ST-SCORE-TEST.                                                   
               10  ST-SUBJECT-CODE                PIC  X(01).                   
                   88  ST-SUBJ-SCOR-TEST          VALUE 'S'.                    
               10  ST-SUBSEGMENT-CODE             PIC  X(01).                   
                   88  ST-SUBSEG-SCOR-TEST        VALUE 'T'.                    
               10  ST-SCORE-TYPE                  PIC  9(02).                   
               10  ST-SCORE-METHOD                PIC  9(01).                   
                   88  ST-CURR                    VALUE 1.                      
                   88  ST-REV                     VALUE 2.                      
                   88  ST-LOAN                    VALUE 3.                      
                   88  ST-MTGE                    VALUE 4.                      
                   88  ST-DEP                     VALUE 5.                      
                   88  ST-CUST                    VALUE 6.                      
               10  ST-SCORE-MB-IND                PIC X(01).                    
                   88  ST-SCORE-NOT-MB-CONTROLLED VALUE '0'.                    
                   88  ST-SCORE-MB-CONTROLLED         VALUE '1'.                
               10  ST-SCORE-EXCLUSION-IND         PIC S9(02) BINARY.            
               10  ST-SCRD-ID                     PIC S9(04) BINARY.            
               10  ST-RAW-SCORE                   PIC S9(07) BINARY.            
               10  ST-ALIGNED-SCORE               PIC S9(07) BINARY.            
               10  ST-CHAR-COUNT                  PIC S9(04) BINARY.            
               10  ST-CA-ENTRY              OCCURS    100 TIMES                 
                                        INDEXED   BY   ST-CA-IDX.               
                   15  ST-CA-CHAR-NUM             PIC S9(04) BINARY.            
                   15  ST-DATA-TYPE               PIC  X(01).                   
                       88  ST-DATA-TYPE-NUM                  VALUE 'N'.         
                       88  ST-DATA-TYPE-CHAR                 VALUE 'C'.         
                   15  ST-CA-ATTR-VALUE           PIC S9(09) BINARY.            
                   15  ST-CA-ATTR-VALUE-X         REDEFINES                     
                       ST-CA-ATTR-VALUE           PIC  X(04).                   
                   15  ST-CA-WGHT                 PIC S9(09) BINARY.            
                   15  ST-CA-WGHT-DIFF            PIC S9(09) BINARY.            
                   15  ST-ADVERSE-ACTION-CODE PIC      9(04).                   
                   15  ST-ADVERSE-ACTION-DESC PIC      X(50).                   
               10  FILLER                         PIC  X(06).                   
