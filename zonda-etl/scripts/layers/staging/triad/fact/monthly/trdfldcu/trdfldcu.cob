             02  DCU.
                 10  LENGTH             PIC S9(08) BINARY.
                 10  TENANT-ID          PIC S9(08) BINARY.
                 10  CUSTOMER-ID        PIC  X(20).
                 10  REC-ID             PIC  X(02).
                 10  REC-ID-NUM         PIC  9(02).
                 10  REC-SEQ-NUM        PIC  9(02).
                 10  TENANT-ID1                  PIC  9(08)
                                                BINARY.
                 10  CUSTOMER-ID1                PIC  X(20).
                 10  PREV-CUSTOMER-ID           PIC  X(20).
                 10  DATE-FIRST-REL             PIC  9(08)
                                                BINARY.
                 10  CUST-TYPE                  PIC  9(03)
                                                BINARY.
                 10  CUST-STATUS                PIC  9(03)
                                                BINARY.
                 10  CUST-SPR-TYPE              PIC  9(01).
                 10  ASSIGNED-SPID              PIC  9(03)
                                                BINARY.
                 10  MAX-EXPOSURE               PIC S9(07)
                                                BINARY.
                 10  NUM-CURR-ACCT              PIC  9(02).
                 10  NUM-REV-ACCT               PIC  9(02).
                 10  NUM-MTGE-ACCT              PIC  9(02).
                 10  NUM-LOAN-ACCT              PIC  9(02).
                 10  NUM-DEP-ACCT               PIC  9(02).
                 10  MK-SEG-USED-SW             PIC  9(01).
                     88  MK-SEG-USED            VALUE 1.
                     88  MK-SEG-NOT-USED        VALUE ZERO.
                 10  DATE-OF-BIRTH              PIC  9(08)
                                                BINARY.
                 10  DATE-LAST-RESTRCTRE        PIC  9(08)
                                                BINARY.
                 10  DATE-LAST-DECL-APPL        PIC  9(08)
                                                BINARY.
                 10  APP-SCORE                  PIC S9(04)
                                                BINARY.
                 10  CUSTOMER-RATING            PIC  9(03)
                                                BINARY.
                 10  PHONE-ADDR-IND             PIC  9(01).
                 10  EMAIL-IND                  PIC  9(01).
                 10  NO-MAIL-IND                PIC  9(01).
                 10  INTERNET-BANKING-IND       PIC  9(01).
                 10  MOBILE-BANKING-IND         PIC  9(01).
                 10  SPID                       PIC  9(03)
                                                   BINARY.
                 10  TEST-DIGITS OCCURS 4   PIC  9(04)
                                               BINARY.
                 10  TRIAD-CAT                  PIC  9(02).
                 10  LIMIT-DATA OCCURS 15 TIMES
                     INDEXED BY CU-LIM-IDX.
                     25  LIMIT                  PIC S9(09)
                                                BINARY.
                     25  DATE-LIMIT-ASSIGN      PIC  9(08)
                                                BINARY.
                     25  LIMIT-MAX              PIC S9(09)
                                                BINARY.
                     25  LIMIT-MAX-INC          PIC S9(09)
                                                BINARY.
                     25  APPORTIONMNT-EXCL-IND  PIC  9(01).
                     25  LIMIT-OFFER            PIC  9(01).
                     25  LIMIT-AUTH             PIC  9(01).
                     25  LIMIT-PRODUCT          PIC  9(01).
                     25  LIMIT-REPORTING-TYPE   PIC  X(02).
                     25  LIMIT-OFFER-CODE       PIC  X(10).
                     25  LIMIT-OFFER-VAL        PIC S9(09)
                                                BINARY.
                     25  LIMIT-OFFER-DATE       PIC  9(08)
                                                BINARY.
                     25  LIMIT-OFFER-TAKEUP-DATE
                                                PIC  9(08)
                                                BINARY.
                     25  LIMIT-OFFER-TAKEUP-VAL PIC S9(09)
                                                   BINARY.
                 10  CDA-DATA OCCURS 25 TIMES
                            INDEXED BY CU-CDA-IDX.
                     25  CDA-DA-ID          PIC X(03).
                     25  CDA-STGY-TYPE      PIC X(03).
                     25  CDA-FEEDBACK-CODE  PIC X(05).
                     25  CDA-FEEDBACK-DATE  PIC  9(08)
                                                BINARY.
                     25  CDA-FEEDBACK-COUNT PIC S9(09)
                                                BINARY.
                     25  CDA-FEEDBACK-AMOUNT
                                                   PIC S9(09)
                                                   BINARY.
                 10  PROFIT                         PIC S9(09)
                                                    BINARY.
                 10  PROVISION                      PIC S9(09)
                                                    BINARY.
                 10  SIGNFCNT-LMT OCCURS 6 TIMES
                                                   PIC S9(09)
                                                   BINARY.
                 10  CUSTOMER-CLASSIFICATION
                                     OCCURS 2 TIMES PIC  9(01).
                 10  GEOGRAPHIC-CODE                PIC  9(04)
                                                    BINARY.
                 10  BRANCH-NUMBER                  PIC  9(04)
                                                    BINARY.
                 10  AMT-GUARANTEED-OFFERS          PIC S9(09)
                                                    BINARY.
                 10  PROVED-INCOME                  PIC S9(09) BINARY.
                 10  CF-EXCLUSION-IND               PIC  9(01).
                 10  CDA-EXCLUSION-IND              PIC  9(01).
                 10  USER-NEGATIVE-TRIGGER1     PIC S9(09)
                                                BINARY.
                 10  USER-NEGATIVE-TRIGGER2     PIC S9(09)
                                                BINARY.
                 10  USER-NEGATIVE-TRIGGER3     PIC S9(09)
                                                BINARY.
                 10  USER-NEGATIVE-TRIGGER4     PIC S9(09)
                                                BINARY.
                 10  USER-RECALC-TRIGGER1       PIC S9(09)
                                                BINARY.
                 10  USER-RECALC-TRIGGER2       PIC S9(09)
                                                BINARY.
                 10  USER-POSITIVE-TRIGGER1     PIC S9(09)
                                                BINARY.
                 10  USER-POSITIVE-TRIGGER2     PIC S9(09)
                                                BINARY.
                 10  USER-POSITIVE-TRIGGER3     PIC S9(09)
                                                BINARY.
                 10  USER-POSITIVE-TRIGGER4     PIC S9(09)
                                                   BINARY.
                 10  USER-SCORE-CAT OCCURS 5
                                                   PIC  9(04)
                                                   BINARY.
                 10  ESTIMATOR-SMPL-FCTR            PIC S9(06)
                                                    BINARY.
                 10  CA-INACTIVITY-PARAMETER        PIC S9(09)
                                                    BINARY.
                 10  RV-INACTIVITY-PARAMETER        PIC S9(09)
                                                    BINARY.
                 10  CA-PERF-EXCL-BALANCE           PIC S9(09)
                                                    BINARY.
                 10  RV-PERF-EXCL-BALANCE           PIC S9(09)
                                                    BINARY.
                 10  MG-PERF-EXCL-BALANCE           PIC S9(09)
                                                    BINARY.
                 10  LN-PERF-EXCL-BALANCE           PIC S9(09)
                                                    BINARY.
                 10  EXTERNAL-DATA OCCURS 2 TIMES.
                     20  EXTERNAL-TRIGGER-CODE      PIC  X(05).
                     20  EXTERNAL-TRIGGER-DATE      PIC  9(08)
                                                    BINARY.
                     20  EXTERNAL-TRIGGER-AMT       PIC S9(09)
                                                    BINARY.
                 10  EXT-SCORE-DATA OCCURS 20 TIMES
                    INDEXED BY CU-EXT-IDX.
                     25  EXTERNAL-RISK-FACTOR   PIC S9(01)V9(07)
                                                BINARY.
                     25  EXTERNAL-EXCLUSION-CAT PIC  X(01).
                     25  EXTERNAL-EXCLUSION-IND
                                                PIC  9(02).
                     25  EXTERNAL-MAX-DELQ      PIC  9(01).
                     25  EXTERNAL-GOOD-BAD-IND  PIC  9(01).
                 10  CB-SCORE-TYPE                  PIC  9(01).
                 10  NEXTGEN-CB-SCORE               PIC S9(04)
                                                    BINARY.
                 10  BAR-FACTOR                 PIC S9(01)V9(07)
                                                    BINARY.
                 10  RECOVERY-FACTOR            PIC S9(01)V9(07)
                                                    BINARY.
                 10  PP20-PROC-CODE                 PIC X(04).
                 10  PP20-PROC-DATE-CYMD            PIC 9(08).
                 10  PP20-EVENT-CODE                PIC 9(03).
                 10  PP20-RR-MODE                   PIC X(01).
                 10  LI-SPID                            PIC 9(03).
