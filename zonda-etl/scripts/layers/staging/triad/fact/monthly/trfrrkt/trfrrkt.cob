           02  RKT.                                                      
               10  LENGTH                    PIC S9(08) BINARY.                                                          
               10  SUBJECT-CODE              PIC  X(01).                  
               10  TENANT-ID                 PIC S9(08) BINARY.           
               10  SPID                      PIC S9(03) BINARY.           
               10  PROC-MODE                 PIC  X(10).                  
               10  COMP-EST-EXPT-ID          PIC  X(05).                  
               10  PROC-DATE-CYMD            PIC  9(08) BINARY.           
               10  CUSTOMER-ID               PIC  X(20).                  
               10  PREV-CUSTOMER-ID          PIC  X(20).                  
               10  ACCOUNT-ID                PIC  X(20).                  
               10  PREV-ACCOUNT-ID           PIC  X(20).                  
               10  CALL-TYPE                 PIC  X(01).                                    
               10  SAMPL-FTR                 PIC  S9(06) BINARY.          
               10  CO-PROD-TYPE              PIC   9(01).                 
               10  VERIFICATION-ID           PIC   X(33).                 
               10  KEYS-LENGTH               PIC S9(08) BINARY.              
               10  KT-KEYS-SUBJECT-CODE      PIC  X(01).
               10  KT-KEYS-SUBSEGMENT-CODE   PIC  X(01).
               10  KT-KEYS-MAX-ENTRIES       PIC S9(04) BINARY.
               10  KT-KEYS-NUM-ENTRIES       PIC S9(04) BINARY.
               10  KT-KEYS-ENTRIES           OCCURS 999 TIMES
                                               INDEXED BY KEYS-IDX.       
                   20  KT-KEYS-DATA-TYPE     PIC  X(01).
                   20  KT-KEYS-VALUE-N       PIC S9(08) BINARY.
               10  FILLER                    PIC  X(05).
                                                                                
