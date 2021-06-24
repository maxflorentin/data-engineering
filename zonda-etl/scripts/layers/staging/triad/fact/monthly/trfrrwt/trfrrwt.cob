      ******************************************************************        
      *                     TRIAD                                      *        
      *    DESCRIPCION: STANDARD REPORT RECORD - HEADER                *        
      ******************************************************************        
      *    COPY FICO  : TRDRPRWT                                       *        
      *    COPY LAKE  : RRHWT                                          *        
      *    LONGUITUD  : 9088                                           *        
      ******************************************************************        
                                                                                
           05  RR-HE-CNTL.                                                      
               10  RR-HE-LENGTH                    PIC S9(08) BINARY.           
                   88  WT-LENGTH-SET               VALUE +149.          T914    
                                                                                
           05  WT-HEADER.                                                       
               10  RR-HE-SUBJECT-CODE              PIC  X(01).                  
                   88  WT-SUBJ-HEADER              VALUE 'H'.                   
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
                                                                                
           05  WT-WORK-SEGMENT.                                                 
      *----------------------------------------------------------------*        
      *        DATE FIELDS                                             *        
      *----------------------------------------------------------------*        
               10 WT-WORK-LENGTH                   PIC S9(08) BINARY.           
                  88 WT-STD-LENGTH                 VALUE +8939.         T914    
               10  WT-SUBJECT-CODE                 PIC  X(01).                  
                   88  WT-SUBJ-WORK                VALUE 'W'.                   
               10  WT-SUBSEGMENT-CODE              PIC  X(01).                  
                   88  WT-SUBSEG-TEST              VALUE 'T'.                   
      *----------------------------------------------------------------*        
      *        FROM PP20 FIELDS                                        *        
      *----------------------------------------------------------------*        
               10 WT-DATE.                                                      
                   15  WT-DTE-PROCESS-CYMD         PIC S9(08).                  
                   15  FILLER REDEFINES WT-DTE-PROCESS-CYMD.                    
                       20  WT-DTE-PROCESS-CYM                                   
                                                   PIC 9(06).                   
                       20  WT-DTE-PROCESS-D        PIC 9(02).                   
                   15  WT-DTE-PROCESS-YD           PIC S9(07).                  
               10  WT-PROC-CODE                    PIC X(04).                   
                   88  WT-BOJ-CALL                 VALUE 'BOJ '.                
                   88  WT-COLLECTIONS-CALL         VALUE 'COLL'.                
                   88  WT-EVENT-CALL               VALUE 'EVNT'.                
                   88  WT-EVENT-COLL-CALL          VALUE 'EVNC'.                
                   88  WT-CON1-CALL                VALUE 'CON1'.                
                   88  WT-CON2-CALL                VALUE 'CON2'.                
                   88  WT-CON3-CALL                VALUE 'CON3'.                
                   88  WT-CON4-CALL                VALUE 'CON4'.                
                   88  WT-CON5-CALL                VALUE 'CON5'.                
                   88  WT-CON6-CALL                VALUE 'CON6'.                
                   88  WT-CON7-CALL                VALUE 'CON7'.                
                   88  WT-CON8-CALL                VALUE 'CON8'.                
                   88  WT-CON9-CALL                VALUE 'CON9'.                
                   88  WT-CON0-CALL                VALUE 'CON0'.                
                   88  WT-CON11-CALL               VALUE 'CONH'.                
                   88  WT-CON12-CALL               VALUE 'CONI'.                
                   88  WT-CON13-CALL               VALUE 'CONJ'.                
                   88  WT-CON14-CALL               VALUE 'CONK'.                
                   88  WT-CON15-CALL               VALUE 'CONL'.                
                   88  WT-CONF-CALL                VALUE 'CON1'                 
                                                         'CON2'                 
                                                         'CON3'                 
                                                         'CON4'                 
                                                         'CON5'                 
                                                         'CON6'                 
                                                         'CON7'                 
                                                         'CON8'                 
                                                         'CON9'                 
                                                         'CON0'                 
                                                         'CONH'                 
                                                         'CONI'                 
                                                         'CONJ'                 
                                                         'CONK'                 
                                                         'CONL'.                
                   88  WT-REVW-CALL                VALUE 'REVW'.                
                   88  WT-REVW-COLL-CALL           VALUE 'REVC'.                
                   88  WT-NEW-CALL                 VALUE 'NEW '.                
                   88  WT-EOJ-CALL                 VALUE 'EOJ '.                
                   88  WT-BOJ-EST-CALL             VALUE 'BOJE'.                
                   88  WT-EOJ-EST-CALL             VALUE 'EOJE'.                
               10  WT-EVENT-CODE                   PIC S9(03).                  
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION RESULTANT FIEDS.                    *        
      *----------------------------------------------------------------*        
               10 WT-INITIAL-CALC-FIELDS.                                       
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION CURR-HEENT ACCOUNT DATA.            *        
      *----------------------------------------------------------------*        
                   15 WT-CCA-16-OCCURS             OCCURS 16 TIMES              
                       INDEXED BY WT-CCA-16-IDX.                                
                      20 WT-CCA-AGREED-LIMIT      PIC S9(09) BINARY.            
                      20 WT-CCA-BALANCE           PIC S9(09) BINARY.            
                      20 WT-CCA-DAYS-IN-DBT-CURR-HE                             
                                                  PIC S9(03) BINARY.            
                      20 WT-CCA-DAYS-IN-EXCS-CURR-HE                            
                                                  PIC S9(03) BINARY.            
                      20 WT-CCA-TIME-ON-BOOKS-CA                                
                                                  PIC S9(03) BINARY.            
                   15 WT-CCA-12-OCCURS            OCCURS 12 TIMES               
                       INDEXED BY WT-CCA-12-IDX.                                
                      20 WT-CCA-CRDT-BAL-SUM      PIC S9(09) BINARY.            
                      20 WT-CCA-CTO-SUM           PIC S9(09) BINARY.            
                      20 WT-CCA-DTO-RG-SUM        PIC S9(09) BINARY.            
                      20 WT-CCA-DTO-SUM           PIC S9(09) BINARY.            
                      20 WT-CCA-SAL-SUM           PIC S9(09) BINARY.            
                   15 WT-CCA-NUM-OPEN-ACCTS       PIC S9(02).                   
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION COLLECTIONS DATA.                   *        
      *----------------------------------------------------------------*        
                   15 WT-CCO-DAYS-SINCE-LAST-CREDIT                             
                                                  PIC S9(03) BINARY.            
                   15 WT-CCO-TIME-ON-BOOKS        PIC S9(03) BINARY.            
                   15 WT-COLL-BALANCE             PIC S9(09) BINARY.            
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION CUSTOMER DATA.                      *        
      *----------------------------------------------------------------*        
                   15 WT-CCU-12-OCCURS            OCCURS 12 TIMES               
                       INDEXED BY WT-CCU-12-IDX.                                
                      20 WT-CCU-DBT-INT-SUM       PIC S9(09) BINARY.            
                      20 WT-CCU-FEES-SUM          PIC S9(09) BINARY.            
                   15 WT-CCU-PRODUCT-HOLDINGS     PIC S9(03) BINARY.            
                   15 WT-CCU-TIME-ON-BOOKS        PIC S9(03) BINARY.            
                   15 WT-CCU-WORST-BLOCK-CODE     PIC  X(04).                   
                   15 WT-CCU-WORST-SEVERITY       PIC  9(02).                   
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION DEPOSIT ACCOUNT DATA.               *        
      *----------------------------------------------------------------*        
                   15 WT-CDP-TIME-ON-BOOKS-DP     OCCURS  12 TIMES              
                       INDEXED BY WT-CDP-TOB-IDX                                
                                                  PIC S9(03) BINARY.            
                   15 WT-CDP-12-OCCURS            OCCURS 12 TIMES               
                       INDEXED BY WT-CDP-12-IDX.                                
                      20 WT-CDP-BAL-EOM-SUM       PIC S9(09) BINARY.            
                      20 WT-CDP-CTO-SUM           PIC S9(09) BINARY.            
                      20 WT-CDP-DTO-RG-SUM        PIC S9(09) BINARY.            
                      20 WT-CDP-DTO-SUM           PIC S9(09) BINARY.            
                      20 WT-CDP-SAL-SUM           PIC S9(09) BINARY.            
                   15 WT-CDP-BAL-SUM              PIC S9(09) BINARY.            
                   15 WT-CDP-FR-BAL-SUM           PIC S9(09) BINARY.            
                   15 WT-CDP-LK-BAL-SUM           PIC S9(09) BINARY.            
                   15 WT-CDP-NUM-OPEN-ACCTS       PIC S9(02).                   
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION LOAN-ACCOUNT-DATA.                  *        
      *----------------------------------------------------------------*        
                   15 WT-CLN-12-OCCURS             OCCURS 12 TIMES              
                       INDEXED BY WT-CLN-12-IDX.                                
                      20 WT-CLN-TIME-ON-BOOKS-LN                                
                                                  PIC S9(03) BINARY.            
                   15 WT-CLN-ARR-HEEARS-SUM       PIC S9(09) BINARY.            
                   15 WT-CLN-BAL-SUM              PIC S9(09) BINARY.            
                   15 WT-CLN-NUM-DELQ             PIC S9(02).                   
                   15 WT-CLN-NUM-OOO-NVR-PD       PIC S9(02).                   
                   15 WT-CLN-NUM-OPEN-ACCTS       PIC S9(02).                   
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION MORTGAGE-ACCOUNT-DATA.              *        
      *----------------------------------------------------------------*        
                   15 WT-CCMG-12-OCCURS             OCCURS 12 TIMES             
                       INDEXED BY WT-CCMG-12-IDX.                               
                      20 WT-CMG-TIME-ON-BOOKS-MG                                
                                                  PIC S9(09) BINARY.            
                   15 WT-CMG-12-OCCURS            OCCURS 12 TIMES               
                       INDEXED BY WT-CMG-12-IDX.                                
                       20 WT-CMG-INSTLMNT-SUM     PIC S9(09) BINARY.            
                   15 WT-CMG-ARR-HEEARS-SUM       PIC S9(09) BINARY.            
                   15 WT-CMG-BAL-SUM              PIC S9(09) BINARY.            
                   15 WT-CMG-NUM-DELQ             PIC S9(02).                   
                   15 WT-CMG-NUM-OPEN-ACCTS       PIC S9(02).                   
      *----------------------------------------------------------------*        
      *        INITIAL CALCULATION REVOLVING CREDIT ACCOUNT DATA       *        
      *----------------------------------------------------------------*        
                   15 WT-CCRV-12-OCCURS             OCCURS 12 TIMES             
                       INDEXED BY WT-CCRV-12-IDX.                               
                      20 WT-CRV-BALANCE           PIC S9(09) BINARY.            
                      20 WT-CRV-LIMIT             PIC S9(09) BINARY.            
                      20 WT-CRV-TIME-ON-BOOKS-RV                                
                                                  PIC S9(03) BINARY.            
                   15 WT-CRV-12-OCCURS            OCCURS 12 TIMES               
                       INDEXED BY WT-CRV-12-IDX.                                
                      20 WT-CRV-PYMTS-SUM         PIC S9(09) BINARY.            
                   15 WT-CRV-ARR-HEEARS-SUM       PIC S9(09) BINARY.            
                   15 WT-CRV-NUM-DELQ             PIC S9(02).                   
                   15 WT-CRV-NUM-OVLM             PIC S9(02).                   
                   15 WT-CRV-NUM-OPEN-ACCTS       PIC S9(02).                   
      *----------------------------------------------------------------*        
      *        INTERMEDIATE CALCULATION RESULTANT FIELDS               *        
      *----------------------------------------------------------------*        
               10 WT-INTERMEDIATE-CALC-FIELDS.                                  
                   15 WT-CCA-DAYS-CRD-DRM-CURR-HE OCCURS  16 TIMES              
                       INDEXED BY WT-CCA-DRM-IDX                                
                                                  PIC S9(03) BINARY.            
                                                                                
                   15 WT-CCA-AGRD-LMT-SUM         PIC S9(09) BINARY.            
                   15 WT-CCA-AGRD-LMT-UNUSD-SUM PIC S9(09) BINARY.              
                   15 WT-CCA-BAL-SUM              PIC S9(09) BINARY.            
                   15 WT-CCA-DBT-BAL-SUM          PIC S9(09) BINARY.            
                   15 WT-CCA-CTO-TOPTAIL-L6M      PIC S9(09) BINARY.            
                   15 WT-CCA-CTO-TOPTAIL-L12M     PIC S9(09) BINARY.            
                   15 WT-CCA-CTO-TOT-L6M          PIC S9(09) BINARY.            
                   15 WT-CCA-CTO-TOT-L12M         PIC S9(09) BINARY.            
                   15 WT-CCADP-TIME-ON-BOOKS-OLDEST                             
                                                  PIC S9(03) BINARY.            
                   15 WT-CCADP-CTO-AVG-L12M       PIC S9(09) BINARY.            
                   15 WT-CCA-DTO-RG-TOT-L6M       PIC S9(09) BINARY.            
                   15 WT-CCA-DTO-RG-TOT-L12M      PIC S9(09) BINARY.            
                   15 WT-CCA-DTO-TOT-L6M          PIC S9(09) BINARY.            
                   15 WT-CCA-DTO-TOT-L12M         PIC S9(09) BINARY.            
                   15 WT-CCA-EXCESS               PIC S9(09) BINARY.            
                   15 WT-CCA-MAX-DAYS-IN-EXCS-CURR-HE                           
                                                  PIC S9(03) BINARY.            
                   15 WT-CCA-NUM-IN-EXCS          PIC S9(02).                   
                   15 WT-CCA-SAL-TOT-L6M          PIC S9(09) BINARY.            
                   15 WT-CCA-SAL-TOT-L12M         PIC S9(09) BINARY.            
                   15 WT-CCA-TIME-ON-BOOKS-OLDEST                               
                                                  PIC S9(03) BINARY.            
                   15 WT-CCA-WORST-CYC-DELQ-CURR-HE                             
                                                  PIC S9(01).                   
                   15 WT-CCO-BAR-BALANCE          PIC S9(09) BINARY.            
                   15 WT-CCU-ARR-HEEARS           PIC S9(09) BINARY.            
                   15 WT-CCU-DBT-BAL-EX-MG-SUM    PIC S9(09) BINARY.            
                   15 WT-CCU-DBT-BAL-SUM          PIC S9(09) BINARY.            
                   15 WT-CCU-DBT-INT-TOT-L6M      PIC S9(09) BINARY.            
                   15 WT-CCU-DBT-INT-TOT-L12M     PIC S9(09) BINARY.            
                   15 WT-CCU-EXCESS               PIC S9(09) BINARY.            
                   15 WT-CCU-FEES-TOT-L6M         PIC S9(09) BINARY.            
                   15 WT-CCU-FEES-TOT-L12M        PIC S9(09) BINARY.            
                   15 WT-CCU-WORST-CYC-DELQ-CURR-HE PIC S9(01).                 
                   15 WT-CDP-CTO-TOPTAIL-L6M      PIC S9(09) BINARY.            
                   15 WT-CDP-CTO-TOPTAIL-L12M     PIC S9(09) BINARY.            
                   15 WT-CDP-CTO-TOT-L6M          PIC S9(09) BINARY.            
                   15 WT-CDP-CTO-TOT-L12M         PIC S9(09) BINARY.            
                   15 WT-CDP-DTO-RG-TOT-L6M       PIC S9(09) BINARY.            
                   15 WT-CDP-DTO-RG-TOT-L12M      PIC S9(09) BINARY.            
                   15 WT-CDP-DTO-TOT-L6M          PIC S9(09) BINARY.            
                   15 WT-CDP-DTO-TOT-L12M         PIC S9(09) BINARY.            
                   15 WT-CDP-TIME-ON-BOOKS-OLDEST                               
                                                  PIC S9(03) BINARY.            
                   15 WT-CLN-TIME-ON-BOOKS-OLDEST                               
                                                  PIC S9(03) BINARY.            
                   15 WT-CMG-TIME-ON-BOOKS-OLDEST                               
                                                  PIC S9(03) BINARY.            
                   15 WT-CRV-BAL-SUM              PIC S9(09) BINARY.            
                   15 WT-CRV-DBT-BAL-SUM          PIC S9(09) BINARY.            
                   15 WT-CRV-EXCESS               PIC S9(09) BINARY.            
                   15 WT-CRV-LMT-SUM              PIC S9(09) BINARY.            
                   15 WT-CRV-LMT-UNUSD-SUM        PIC S9(09) BINARY.            
                   15 WT-CRV-LMT-USD-SUM          PIC S9(09) BINARY.            
                   15 WT-CRV-TIME-ON-BOOKS-OLDEST                               
                                                  PIC S9(03) BINARY.            
      *----------------------------------------------------------------*        
      *        FIELDS USED FOR SCORING                                 *        
      *----------------------------------------------------------------*        
               10 WT-SCORE-FIELDS.                                              
                   15 WT-SCORE-12-OCCURS            OCCURS 12 TIMES             
                       INDEXED BY WT-SCR-12-IDX.                                
                      20 WT-CCA-INACTIVE-FLAG     PIC S9(01).                   
                      20 WT-CRV-INACTIVE-FLAG     PIC S9(01).                   
                                                                                
                   15 WT-CLN-TOT-PAYMENTS-L6M     PIC S9(09) BINARY.            
                   15 WT-CMG-TOT-PAYMENTS-L6M     PIC S9(09) BINARY.            
                   15 WT-CRV-TOT-PAYMENTS-L6M     PIC S9(09) BINARY.            
                   15 WT-CCA-MAX-PCT-DAYS-IN-XS-L6M                             
                                                  PIC S9(03) BINARY.            
                   15 WT-CCA-MONTHS-SINCE-ACTIVE                                
                                                  PIC S9(03) BINARY.            
                   15 WT-CLN-ALL-ACCOUNT-CLOSED PIC S9(03) BINARY.              
                   15 WT-CRV-MONTHS-SINCE-ACTIVE                                
                                                  PIC S9(03) BINARY.            
                                                                                
      *----------------------------------------------------------------*        
      *        AGGREGATED FIELDS USED FOR SCORECARD CHARACTERISTICS    *        
      *----------------------------------------------------------------*        
               10 WT-AGG-FIELDS.                                                
                   15 WT-ACA-12-OCCURS            OCCURS 12 TIMES               
                      INDEXED BY WT-ACA-12-IDX.                                 
                      20 WT-ACA-DAYS-IN-CREDIT    PIC S9(09) BINARY.            
                      20 WT-ACA-DAYS-IN-EXCESS                                  
                                                  PIC S9(09) BINARY.            
                      20 WT-ACA-VAL-CUST-CREDITS                                
                                                  PIC S9(09) BINARY.            
                      20 WT-ACA-VAL-CUST-DEBITS PIC S9(09) BINARY.              
                      20 WT-ACA-VAL-AUTO-DEBITS PIC S9(09) BINARY.              
                      20 WT-ACA-AGREED-LIMIT      PIC S9(09) BINARY.            
                      20 WT-ACA-DEBIT             PIC S9(09) BINARY.            
                      20 WT-ACA-EXCESS            PIC S9(09) BINARY.            
                      20 WT-ACA-AVG-BALANCE       PIC S9(09) BINARY.            
                      20 WT-ACA-MAX-BALANCE       PIC S9(09) BINARY.            
                      20 WT-ACA-MIN-BALANCE       PIC S9(09) BINARY.            
                      20 WT-ACA-EOM-BALANCE       PIC S9(09) BINARY.            
                      20 WT-ARV-VAL-CASH-ADV      PIC S9(09) BINARY.            
                      20 WT-ARV-VAL-MERCH-SALES PIC S9(09) BINARY.              
                      20 WT-ARV-AMOUNT-DUE        PIC S9(09) BINARY.            
                      20 WT-ARV-VAL-ARR-HEEARS    PIC S9(09) BINARY.            
                      20 WT-ARV-VAL-PAYMENTS      PIC S9(09) BINARY.            
                      20 WT-ARV-VAL-PURCH         PIC S9(09) BINARY.            
                      20 WT-ARV-CASH-BALANCE      PIC S9(09) BINARY.            
                      20 WT-ARV-CYCLE-BALANCE     PIC S9(09) BINARY.            
                      20 WT-ARV-LIMIT             PIC S9(09) BINARY.            
                      20 WT-ARV-NSF               PIC S9(04) BINARY.            
                      20 WT-ARV-ACTIVE            PIC S9(01).                   
                      20 WT-AMG-ARR-HEEARS-AMOUNT PIC S9(09) BINARY.            
                      20 WT-AMG-PAYMENT-AMOUNT    PIC S9(09) BINARY.            
                      20 WT-AMG-BALANCE           PIC S9(09) BINARY.            
                      20 WT-ALN-ARR-HEEARS-AMOUNT PIC S9(09) BINARY.            
                      20 WT-ALN-PAYMENT-AMOUNT    PIC S9(09) BINARY.            
                      20 WT-ALN-BALANCE           PIC S9(09) BINARY.            
                      20 WT-ADP-BALANCE           PIC S9(09) BINARY.            
                      20 WT-ADP-BALANCE-INSTANT PIC S9(09) BINARY.              
                   15 WT-ACA-24-OCCURS            OCCURS 24 TIMES               
                      INDEXED BY WT-ACA-24-IDX.                                 
                      20 WT-ARV-CYCLES-DELQ       PIC S9(09) BINARY.            
                      20 WT-AMG-NUM-MISSED-PAYMENTS                             
                                                  PIC S9(09) BINARY.            
                      20 WT-ALN-NUM-MISSED-PAYMENTS                             
                                                  PIC S9(09) BINARY.            
                   15 WT-ACA-TOB                  PIC S9(07)     BINARY.        
                   15 WT-ADP-NUM-INSTANT          PIC S9(09) BINARY.            
                   15 WT-ADP-TOB                  PIC S9(07)     BINARY.        
                   15 WT-ALN-ORIGINAL-LOAN-AMOUNT                               
                                                  PIC S9(09) BINARY.            
                   15 WT-ALN-TOB                  PIC S9(07)     BINARY.        
                   15 WT-AMG-TOB                  PIC S9(07)     BINARY.        
                   15 WT-ARV-TOB                  PIC S9(07)     BINARY.        
                   15 WT-ARV-NUM-3-CYC-LF         PIC  9(03)     BINARY.        
                   15 WT-ARV-NUM-4P-CYC-LF        PIC  9(03)     BINARY.        
                   15 WT-ACU-TOB                  PIC S9(07)     BINARY.        
                                                                                
               10 WT-POPULATED-FROM-PR20-FIELDS.                                
                   15 WT-COLL-RECLASS-REASON      PIC S9(02).                   
               10 WT-CRFA-EVENT-TRIGGER-VALS.                                   
                   15 WT-CRFA-EVENT-TRIGGER-VAL OCCURS 28 TIMES                 
                       INDEXED BY WT-CRFA-IDX                                   
                                                  PIC S9(07) BINARY.            
      *----------------------------------------------------------------*        
      *        SCORE ARR-HEAYS                                         *        
      *              ARR-HEAYS ARE AGED WHEN NEW SCORES ARE CALCULATED *        
      *              ANY PROCESS NEEDING SCORE DATA SHOULD REFERENCE   *        
      *              THESE FIELDS RATHER THAN FIL FIELDS, WHICH ARE    *        
      *              NOT AGED.                                         *        
      *----------------------------------------------------------------*        
                                                                                
               10  WT-CU-SCORE-TYPE-DATA OCCURS 20 TIMES                        
                   INDEXED BY WT-STD-IDX.                                       
                   15  WT-CU-SCRD-ID              PIC S9(04) BINARY.            
                   15  WT-CU-RAW-SCORE            PIC S9(07) BINARY.            
                   15  WT-CU-ALIGNED-SCORE        PIC S9(07) BINARY.            
                   15  WT-CU-CA-SCRD-ID           PIC S9(04) BINARY.            
                   15  WT-CU-CA-RAW-SCORE         PIC S9(07) BINARY.            
                   15  WT-CU-CA-ALIGNED-SCORE     PIC S9(07) BINARY.            
                   15  WT-CU-RV-SCRD-ID           PIC S9(04) BINARY.            
                   15  WT-CU-RV-RAW-SCORE         PIC S9(07) BINARY.            
                   15  WT-CU-RV-ALIGNED-SCORE     PIC S9(07) BINARY.            
                   15  WT-CU-LN-SCRD-ID           PIC S9(04) BINARY.            
                   15  WT-CU-LN-RAW-SCORE         PIC S9(07) BINARY.            
                   15  WT-CU-LN-ALIGNED-SCORE     PIC S9(07) BINARY.            
                   15  WT-CU-MG-SCRD-ID           PIC S9(04) BINARY.            
                   15  WT-CU-MG-RAW-SCORE         PIC S9(07) BINARY.            
                   15  WT-CU-MG-ALIGNED-SCORE     PIC S9(07) BINARY.            
                   15  WT-CU-DP-SCRD-ID           PIC S9(04) BINARY.            
                   15  WT-CU-DP-RAW-SCORE         PIC S9(07) BINARY.            
                   15  WT-CU-DP-ALIGNED-SCORE     PIC S9(07) BINARY.            
      *            HISTORIC SCORE INFO USED BY SCORE PERF TALLY         T914    
                   15  WT-CU-SCRD-ID-HIST         PIC S9(04)  BINARY.   T914    
                   15  WT-CU-RAW-SCORE-HIST       PIC S9(07)  BINARY.   T914    
                   15  WT-CU-ALIGNED-SCORE-HIST PIC S9(07)    BINARY.   T914    
                   15  WT-CU-CA-SCRD-ID-HIST      PIC S9(04)  BINARY.   T914    
                   15  WT-CU-CA-RAW-SCORE-HIST    PIC S9(07)  BINARY.   T914    
                   15  WT-CU-CA-ALIGNED-SCORE-HIST                      T914    
                                                  PIC S9(07)  BINARY.   T914    
                   15  WT-CU-RV-SCRD-ID-HIST      PIC S9(04)  BINARY.   T914    
                   15  WT-CU-RV-RAW-SCORE-HIST    PIC S9(07)  BINARY.   T914    
                   15  WT-CU-RV-ALIGNED-SCORE-HIST                              
                                                  PIC S9(07)  BINARY.   T914    
                   15  WT-CU-LN-SCRD-ID-HIST      PIC S9(04)  BINARY.   T914    
                   15  WT-CU-LN-RAW-SCORE-HIST    PIC S9(07)  BINARY.   T914    
                   15  WT-CU-LN-ALIGNED-SCORE-HIST                      T914    
                                                  PIC S9(07)  BINARY.   T914    
                   15  WT-CU-MG-SCRD-ID-HIST      PIC S9(04)  BINARY.   T914    
                   15  WT-CU-MG-RAW-SCORE-HIST    PIC S9(07)  BINARY.   T914    
                   15  WT-CU-MG-ALIGNED-SCORE-HIST                      T914    
                                                  PIC S9(07)  BINARY.   T914    
                   15  WT-CU-DP-SCRD-ID-HIST      PIC S9(04)  BINARY.   T914    
                   15  WT-CU-DP-RAW-SCORE-HIST    PIC S9(07)  BINARY.   T914    
                   15  WT-CU-DP-ALIGNED-SCORE-HIST                      T914    
                                                  PIC S9(07)  BINARY.   T914    
               10  WT-OTHER-SCORE-FIELDS.                                       
                   15  WT-BAR-FACTORS OCCURS 20 TIMES                           
                       INDEXED BY WT-BAR-IDX.                                   
                      20  WT-CU-BAR-FACTOR       PIC S9(2)V9(7) BINARY.         
        
      *----------------------------------------------------------------*        
      *        SYSTEM GENERATED WORK FIELDS                            *        
      *----------------------------------------------------------------*        
               10  WT-SYS-GEN-FIELDS.                                           
                                                                                
      *----------------------------------------------------------------*        
      *        SPID                                                    *        
      *----------------------------------------------------------------*        
                   15  WT-SPID                           PIC S9(04).            
                                                                                
                   15  WT-SMPL-FTR                       PIC S9(04)             
                                                         BINARY.                
      *----------------------------------------------------------------*        
      *        EXCLUSION INDICATORS                                    *        
      *----------------------------------------------------------------*        
                   15  WT-BYPASS-EXCLUSION-IND           PIC S9(04).            
                       88  WT-NOT-BYPASS-CUSTOMER        VALUE ZERO.            
                       88  WT-BYPASS-CUSTOMER            VALUE  +1              
                                                          THRU +99.             
                   15  WT-CRFA-EXCLUSION-IND             PIC S9(04)             
                                                    OCCURS 4 TIMES.             
                       88  WT-CRFA-NOT-EXCLUDED          VALUE ZERO.            
                       88  WT-CRFA-TA-EXCLUDED           VALUE +1               
                                                          THRU +99.             
                       88  WT-CRFA-STGY-999-EXCL         VALUE +99.             
                       88  WT-CRFA-BYPASS-EXCL           VALUE +101             
                                                          THRU +199.            
                       88  WT-CRFA-EXCLUDED              VALUE +1               
                                                          THRU +199.            
                   15  WT-CRFA-DECR-EXCL-IND             PIC S9(04)             
                                                    OCCURS 4 TIMES.             
                       88  WT-CRFA-NOT-DECR-EXCLUDED     VALUE ZERO.            
                       88  WT-CRFA-DECR-EXCLUDED         VALUE +1               
                                                          THRU +99.             
                   15  WT-CRFA-INCR-EXCL-IND             PIC S9(04)             
                                                    OCCURS 4 TIMES.             
                       88  WT-CRFA-NOT-INCR-EXCLUDED     VALUE ZERO.            
                       88  WT-CRFA-INCR-EXCLUDED         VALUE +1               
                                                          THRU +99.             
                   15  WT-ECOL-EXCLUSION-IND             PIC S9(04).            
                       88  WT-ECOL-NOT-EXCLUDED          VALUE ZERO.            
                       88  WT-ECOL-TA-EXCLUDED           VALUE +1               
                                                          THRU +99.             
                       88  WT-ECOL-STGY-999-EXCL         VALUE +99.             
                       88  WT-ECOL-BYPASS-EXCL           VALUE +101             
                                                          THRU +199.            
                       88  WT-ECOL-EXCLUDED              VALUE +1               
                                                          THRU +199.            
                   15  WT-ECOL-STGY-EXCL-IND             PIC S9(04).            
                       88  WT-ECOL-NOT-STGY-EXCL         VALUE ZERO.            
                       88  WT-ECOL-STGY-EXCL             VALUE +1               
                                                          THRU +99.             
                   15  WT-CDA-EXCL-IND OCCURS 25 TIMES                          
                       INDEXED BY WT-CDA-EX-IDX.                                
                       20  WT-CDA-EXCLUSION-IND          PIC S9(04).            
                           88  WT-CDA-NOT-EXCLUDED       VALUE ZERO.            
                           88  WT-CDA-TRIGGER-EXCL       VALUE +98.             
                           88  WT-CDA-TA-EXCLUDED        VALUE +1               
                                                          THRU +99.             
                           88  WT-CDA-STGY-999-EXCL      VALUE +99.             
                           88  WT-CDA-BYPASS-EXCL        VALUE +101             
                                                          THRU +199.            
                           88  WT-CDA-EXCLUDED           VALUE +1               
                                                          THRU +199.            
                   15  WT-AUTH-EXCLUSION-IND             PIC S9(04).            
                       88  WT-AUTH-NOT-EXCLUDED          VALUE ZERO.            
                       88  WT-AUTH-TA-EXCLUDED           VALUE +1               
                                                          THRU +99.             
                       88  WT-AUTH-STGY-999-EXCL         VALUE +99.             
                       88  WT-AUTH-BYPASS-EXCL           VALUE +101             
                                                          THRU +199.            
                       88  WT-AUTH-EXCLUDED              VALUE +1               
                                                          THRU +199.            
                                                                                
      *----------------------------------------------------------------*        
      *        TENANT CONTROL INDICATORS                               *        
      *----------------------------------------------------------------*        
                   15  WT-SCORING-THIS-CALL-IND          PIC  X(01).            
                        88  WT-SCORING-THIS-CALL         VALUE '1'.             
                        88  WT-NO-SCORING-THIS-CALL      VALUE '0'.             
                   15  WT-CRFA-TENANT-CNTL-IND           PIC  X(01)             
                                                       OCCURS 4 TIMES.          
                        88  WT-CRFA-TENANT-CNTL          VALUE '1'.             
                        88  WT-CRFA-TRIAD-CNTL           VALUE '0'.             
                   15  WT-ECOL-TENANT-CNTL-IND           PIC  X(01).            
                        88  WT-ECOL-TENANT-CNTL          VALUE '1'.             
                        88  WT-ECOL-TRIAD-CNTL           VALUE '0'.             
                   15  WT-CDA-CLT-CNTL-IND OCCURS 15 TIMES                      
                       INDEXED BY WT-CDA-CC-IDX.                                
                       20  WT-CDA-TENANT-CNTL-IND        PIC  X(01).            
                           88  WT-CDA-TENANT-CNTL        VALUE '1'.             
                           88  WT-CDA-TRIAD-CNTL         VALUE '0'.             
                   15  WT-AUTH-TENANT-CNTL-IND           PIC  X(01).            
                       88  WT-AUTH-TENANT-CNTL           VALUE '1'.             
                       88  WT-AUTH-TRIAD-CNTL            VALUE '0'.             
      *----------------------------------------------------------------*        
      *    TRIGGERS                                                    *        
      *----------------------------------------------------------------*        
                   15  WT-CDA-TRIGGER                 PIC S9(04) BINARY.        
                   15  WT-CDA-TRIGGER-KEY-NUM         PIC S9(04) BINARY.        
                   15  WT-RECLASS-TRIGGER             PIC S9(04) BINARY.        
                   15  WT-EVENT-TRIGGER               PIC S9(02).               
                   15  WT-INT-STGY-TRIGGER            PIC S9(04) BINARY.        
      *----------------------------------------------------------------*        
      *    PRODUCT LEVEL CURR-HEENT EXPOSURE FIELDS                    *        
      *----------------------------------------------------------------*        
                   15  WT-CURR-HE-EXPOSURE-CA        PIC S9(07) BINARY.         
                   15  WT-CURR-HE-EXPOSURE-RV        PIC S9(07) BINARY.         
                   15  WT-CURR-HE-EXPOSURE-LN        PIC S9(07) BINARY.         
                   15  WT-CURR-HE-EXPOSURE-MG        PIC S9(07) BINARY.         
      *----------------------------------------------------------------*        
      *    PROPOSED EXPOSURE                                           *        
      *----------------------------------------------------------------*        
                   15  WT-CRFA-PROPOSED-EXPOSURE     PIC S9(09) BINARY.         
      *----------------------------------------------------------------*        
      *    BLOCK CODE PERFORMANCE CATEGORY INFORMATION                 *        
      *----------------------------------------------------------------*        
               10  WT-BLOCK-CODE-DATA.                                          
                   15  WT-PRODUCT-BLOCK-CODES.                                  
                       20  WT-CA-BLOCK-CODE-GRP OCCURS 16 TIMES                 
                            INDEXED BY WT-CA-IDX.                               
                           25  WT-CA-PERF-CAT OCCURS 12 TIMES                   
                               INDEXED BY WT-CA-BK-IDX                          
                                                          PIC  9(03).           
                       20  WT-RV-BLOCK-CODE-GRP OCCURS 12 TIMES                 
                           INDEXED BY WT-RV-IDX.                                
                           25  WT-RV-PERF-CAT OCCURS 12 TIMES                   
                               INDEXED BY WT-RV-BK-IDX                          
                                                          PIC  9(03).           
                       20  WT-MG-BLOCK-CODE-GRP OCCURS 12 TIMES                 
                           INDEXED BY WT-MG-IDX.                                
                           25  WT-MG-PERF-CAT OCCURS 12 TIMES                   
                               INDEXED BY WT-MG-BK-IDX                          
                                                          PIC  9(03).           
                       20  WT-LN-BLOCK-CODE-GRP OCCURS 12 TIMES                 
                           INDEXED BY WT-LN-IDX.                                
                           25  WT-LN-PERF-CAT OCCURS 12 TIMES                   
                               INDEXED BY WT-LN-BK-IDX                          
                                                          PIC  9(03).           
      *----------------------------------------------------------------*        
      *        FROM PR20 FIELDS NEED FOR KEY #373 CALCULATION          *        
      *----------------------------------------------------------------*        
               10 WT-CFC-REVIEW-TYPE OCCURS 15 TIMES                            
                                                          PIC  9(02).           
      *----------------------------------------------------------------*        
      *    BLAZE ADVISOR FLM ALTERNATE LOOP PROJECT WORK FIELD         *        
      *----------------------------------------------------------------*        
               10  WT-CDA-FLMA-BA-WRK-AREA                  PIC X(1000).        
               10  FILLER                                   PIC X(09).          
                                                                                
