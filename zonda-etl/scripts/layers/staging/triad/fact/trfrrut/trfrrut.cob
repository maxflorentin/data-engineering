      ******************************************************************        
      *                     TRIAD                                      *        
      *    DESCRIPCION: STANDARD REPORT RECORD - HEADER                *        
      ******************************************************************        
      *    COPY FICO  : TRDRPRMR                                       *        
      *    COPY LAKE  : RRHUT                                          *        
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
               10  RR-HE-COMP-EST-EXPT-ID          PIC  X(05).                  
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
                                                                                
           05  RR-HE-LENGTH                       PIC S9(08) BINARY.            
               88  RR-HE-LENGTH-VALUE             VALUE +32006.                 
           05  RR-HE-SUBJECT-CODE                 PIC  X(01).                   
               88  RR-HE-SUBJ-RET-SCORE           VALUE 'U'.                    
           05  RR-HE-SUBSEGMENT-CODE              PIC  X(01).                   
               88  RR-HE-SUBSEG-TEST-RETURN       VALUE 'T'.                    
           05  RR-HE-TEST-DATA                    PIC X(32000)                  
                                                  VALUE LOW-VALUES.             
           05 UT-PRODUCT-SCORE-ENTRY OCCURS 5 TIMES                             
                       INDEXED BY UT-P-S-IDX.                                   
                       20  UT-PRODUCT-SCORE-TYPE      PIC  X(02).               
                               88  UT-PROD-SCORE-CA         VALUE 'CA'.         
                               88  UT-PROD-SCORE-RV         VALUE 'RV'.         
                               88  UT-PROD-SCORE-LN         VALUE 'LN'.         
                               88  UT-PROD-SCORE-MG         VALUE 'MG'.         
                               88  UT-PROD-SCORE-DP         VALUE 'DP'.         
                               88  UT-PROD-SCORE-UNUSED VALUE SPACE.            
                       20  UT-PRODUCT-SCORE-DATA      OCCURS 20 TIMES           
                           INDEXED BY UT-P-SCR-IDX.                             
                           25  UT-PROD-MB-IMPORTED-IND                          
                                                      PIC  X(01).               
                               88  UT-PROD-MB-IMPORTED VALUE '1'.               
                               88  UT-PROD-MB-NOT-IMPORTED                      
                                                           VALUE '0'.           
                           25  UT-PROD-SCRD-ID        PIC S9(04) BINARY.        
                               88  UT-PROD-SCORE-EXCL      VALUE ZERO.          
                           25  UT-PROD-RAW-SCORE      PIC S9(07) BINARY.        
                           25  UT-PROD-ALIGNED-SCORE                            
                                                      PIC S9(07) BINARY.        
                               88  UT-PROD-EXCL-SCORE      VALUE +1 THRU        
                                                                +50.            
                           25  UT-PROD-MN-TBL.                                  
                               30  UT-PROD-MN-TBL-ENTRY                         
                                                  OCCURS 7 TIMES                
                                    INDEXED BY UT-PROD-MN-IDX.                  
                                   35  UT-PROD-MN-CHAR-NUM                      
                                                      PIC S9(04) BINARY.        
                                   35  UT-PROD-MN-WGHT-DIFF                     
                                                      PIC S9(09) BINARY.        
                                   35  UT-PROD-MN-ADVERSE-ACT-CODE              
                                                      PIC S9(04) BINARY.        
                                   35  UT-PROD-MN-ADV-ACT-REASON                
                                                      PIC  X(50).               
