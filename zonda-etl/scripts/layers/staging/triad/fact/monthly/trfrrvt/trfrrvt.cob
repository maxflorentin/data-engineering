           05  RVT.
               10  RR-HE-LENGTH             PIC S9(08) BINARY.
               10  RR-HE-SUBJECT-CODE       PIC X(01).
               10  RR-HE-TENANT-ID          PIC S9(08) BINARY.
               10  RR-HE-SPID               PIC S9(03) BINARY.
               10  RR-HE-PROC-MODE          PIC X(10).
               10  RR-HE-COMP-EST-EXPT-ID   PIC X(05).
               10  RR-HE-PROC-DATE-CYMD     PIC 9(08) BINARY.
               10  RR-HE-CUSTOMER-ID        PIC X(20).
               10  RR-HE-PREV-CUSTOMER-ID   PIC X(20).
               10  RR-HE-ACCOUNT-ID         PIC X(20).
               10  RR-HE-PREV-ACCOUNT-ID    PIC X(20).
               10  RR-HE-CALL-TYPE          PIC X(01).
               10  RR-HE-SAMPL-FTR          PIC S9(06) BINARY.
               10  RR-HE-CO-PROD-TYPE       PIC 9(01).
               10  RR-HE-VERIFICATION-ID    PIC X(33).
               10  VT-LENGTH                PIC S9(08) BINARY.
               10  VT-SUBJECT-CODE          PIC X(01).
               10  VT-SUBSEGMENT-CODE       PIC X(01).
               10  VT-PROC-CODE             PIC X(04).
               10  VT-PROC-DATE-CYMD        PIC 9(08).
               10  VT-EVENT-CODE            PIC 9(03).
               10  VT-RR-MODE               PIC X(01).
               10  RET-CODE                 PIC 9(04).
               10  RET-ERROR-MSG1           PIC X(120).
               10  RET-ERROR-MSG2           PIC X(120).
               10  TENANT-ID                PIC 9(08) BINARY.
               10  SPID                     PIC 9(03).
               10  EXCLUSION-IND            PIC 9(02).
               10 CU-CAT                    PIC 9(02).
               10 CA-CAT OCCURS 16 TIMES PIC 9(02).
               10 RV-CAT OCCURS 12 TIMES PIC 9(02).
               10 MG-CAT OCCURS 12 TIMES PIC 9(02).
               10 LN-CAT OCCURS 12 TIMES PIC 9(02).
               10  CDA-FLMA-BA-WRK-AREA  PIC X(1000).
               10  KEY-VALUE OCCURS 999 TIMES.
                   15  KEY-N             PIC S9(07) BINARY.
               10  COLL-DA-EXCLUSION-IND PIC 9(02).
               10  COLL-STGY-EXCLUSION-IND PIC 9(01).
               10  COLL-TENANT-CNTL-IND  PIC X(01).
               10  COLL-RANDOM-NUMBER    PIC 9(01).
               10 COLL-DGT-LWRLMT      PIC S9(04) BINARY.
               10 COLL-DGT-UPRLMT      PIC S9(04) BINARY.
               10  COLL-SCEN-ID        PIC 9(04).
               10  COLL-SCEN-SEGMENT   PIC X(05).
               10  COLL-SCEN-MONITORING PIC X(01).
               10  COLL-STGY-ID         PIC 9(03).
               10  COLL-STGY-ROW        PIC S9(08) BINARY.
               10  COLL-INT-TRIGGER     PIC S9(04) BINARY.
               10  COLL-ACTION-CTR      PIC 9(01).
               10  COLL-FIXED-ACTION-1 PIC X(05).
               10  COLL-FIXED-ACTION-2 PIC X(05).
               10  COLL-TIMED-ACTION-1 PIC X(06).
               10  COLL-TIMED-ACTION-2 PIC X(04).
               10  COLL-TIMED-ACTION-3 PIC X(05).
               10  COLL-CATCH-UP-CONTROL PIC X(01).
               10  COLL-TIMED-ACTION-4 PIC X(05).
               10  COLL-TIMED-ACTION-5 PIC X(05).
               10  COLL-NEXT-CALL-DAYS PIC 9(02).
               10  COLL-RECLASS-REASON  PIC 9(02).
               10  COLL-DATE-BILL-EQV  PIC 9(08).
               10  DATE-FIRST-COLLS-DA PIC 9(08).
               10  COLL-BALANCE-INITIAL PIC S9(09) BINARY.
               10  COLL-OOO-TYPE       PIC 9(01).
               10  COLL-DELQ           PIC 9(02).
               10  COLL-AMT-ARREARS    PIC S9(09) BINARY.
               10  COLL-AMT-EXCESS-OVLM PIC S9(09) BINARY.
               10  COLL-BALANCE        PIC S9(09) BINARY.
               10  COLL-BALANCE-ACTUAL PIC S9(09) BINARY.
               10  COLL-LIMIT          PIC S9(09) BINARY.
               10  COLL-PTP            PIC 9(01).
               10  COLL-TELEPHONE-IND  PIC 9(01).
               10  COLL-ADDRESS-IND    PIC 9(01).
               10  COLL-BLOCK-CODE     PIC X(04).
               10  COLL-WORST-CYC-DELQ PIC 9(02).
               10  COLL-TOTAL-OOO-AMT  PIC S9(09) BINARY.
               10  COLL-NUM-OOO        PIC 9(03).
               10  CDA-ACTIONS         OCCURS 25 TIMES.
                   15  CDA-DA-ID       PIC X(03).
                   15  CDA-STGY-TYPE   PIC X(03).
                   15  CDA-DA-EXCLUSION-IND PIC 9(02).
                   15  CDA-TENANT-CNTL-IND  PIC X(01).
                   15  CDA-RANDOM-NUMBER    PIC 9(01).
                   15 CDA-DGT-LWRLMT       PIC S9(04) BINARY.
                   15 CDA-DGT-UPRLMT       PIC S9(04) BINARY.
                   15  CDA-SCEN-ID         PIC 9(04).
                   15  CDA-SCEN-SEGMENT    PIC X(05).
                   15  CDA-SCEN-TYPE       PIC X(01).
                   15  CDA-INT-TRIGGER     PIC 9(04) BINARY.
                   15  CDA-STGY-ID         PIC 9(03).
                   15  CDA-STGY-ROW        PIC S9(08) BINARY.
                   15  CDA-ACTION          OCCURS 25 TIMES PIC X(05).
                   15  CDA-TRIGGER-OUTCOME PIC X(01).
                   15  CDA-TRIGGER-NUMBER  PIC S9(03) BINARY.
               10  CDA-BA-ACTIONS          PIC X(200).
               10  AUTH-DA-EXCLUSION-IND   PIC 9(02).
               10  AUTH-TENANT-CNTL-IND    PIC X(01).
               10  AUTH-RANDOM-NUMBER      PIC 9(01).
               10 AUTH-DGT-LWRLMT      PIC S9(04) BINARY.
               10 AUTH-DGT-UPRLMT      PIC S9(04) BINARY.
               10  AUTH-STGY-ID OCCURS 4 PIC 9(03).
               10  AUTH-TEST-DIGITS      PIC 9(04).
               10  SCORE-DATA   OCCURS 20 TIMES
                               INDEXED BY SCORE-IDX.
                   15  SCRD-TYPE               PIC 9(02).
                   15  MB-IMPORTED-IND         PIC X(01).
                   15  SCORE-EXCLUSION-CAT     PIC X(01).
                   15  SCRD-ID                 PIC S9(04) BINARY.
                   15  RAW-SCORE               PIC S9(07) BINARY.
                   15  ALIGNED-SCORE           PIC S9(07) BINARY.
                   15  BAL-AT-RISK             PIC S9(09) BINARY.
                   15  BAR-FACTOR              PIC S9(01)V9(07) BINARY.
                   15  MN-TBL-ENTRY OCCURS 7 TIMES
                                           INDEXED BY MN-IDX.
                       20  MN-CHAR-NUM     PIC S9(04) BINARY.
                       20  MN-WGHT-DIFF    PIC S9(09) BINARY.
                       20  MN-ADVERSE-ACT-CODE PIC S9(04) BINARY.
                       20  MN-ADVERSE-ACT-REASON PIC X(50).
