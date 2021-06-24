           02  RCT.
              10  LENGTH              PIC S9(08) BINARY.
              10  SUBJECT-CODE        PIC X(01).
              10  TENANT-ID           PIC S9(08) BINARY.
              10  SPID                PIC S9(03) BINARY.
              10  PROC-MODE           PIC X(10).
              10  COMP-EST-EXPT-ID    PIC X(05).
              10  PROC-DATE-CYMD      PIC 9(08) BINARY.
              10  CUSTOMER-ID         PIC X(20).
              10  PREV-CUSTOMER-ID    PIC X(20).
              10  ACCOUNT-ID          PIC X(20).
              10  PREV-ACCOUNT-ID     PIC X(20).
              10  CALL-TYPE           PIC X(01).
              10  SAMPL-FTR           PIC S9(06) BINARY.
              10  CO-PROD-TYPE        PIC 9(01).
              10  VERIFICATION-ID     PIC X(33).
              10  CT-LENGTH                 PIC S9(08) BINARY.
              10  CT-SUBJECT-CODE           PIC X(01).
              10  CT-SUBSEGMENT-CODE        PIC X(01).
              10  CT-ACTIONS OCCURS 4 TIMES.
                  20  CT-STGY-TYPE          PIC X(03).
                  20  CT-DA-EXCLUSION-IND   PIC 9(02).
                  20  CT-TENANT-CNTL-IND    PIC X(01).
                  20  CT-RANDOM-NUMBER      PIC 9(01).
                  20  CT-DGT-LWRLMT         PIC S9(04) BINARY.
                  20  CT-DGT-UPRLMT         PIC S9(04) BINARY.
                  20  CT-SCEN-ID            PIC 9(04).
                  20  CT-STGY-ID            PIC 9(03).
                  20  CT-LIMIT-MODEL-ID     PIC 9(03).
                  20  CT-STGY-ROW           PIC S9(08) BINARY.
                  20  CT-LIMIT-DATA         OCCURS 15 TIMES.
                      25  CT-NEW-LIMIT      PIC S9(07) BINARY.
                      25  CT-REVIEW-TYPE    PIC 9(02).
                      25  CT-LIMIT-ADJUSTMENT OCCURS 6 TIMES PIC 9(01).
                          88  CT-LMT-ADJUSTED VALUE 1.
                          88  CT-LMT-NOT-ADJUSTED VALUE ZEROS.
                      25  CT-LIMIT-OFFER    PIC 9(01).
                      25  CT-LIMIT-AUTH     PIC 9(01).
                      25  CT-LIMIT-PRODUCT  PIC 9(01).
                      25  CT-LIMIT-RPT-TYPE PIC X(02).
                      25  CT-COMPONENTS     OCCURS 10 TIMES.
                          30  CT-LIMIT-COMPONENT PIC S9(09) BINARY.
                          30  CT-LIMIT-COMP-IND PIC 9(02).
                          30  CT-LIMIT-COMP-KEY-NO PIC 9(03).
                      25  CT-MAX-REF-POINT   PIC S9(09) BINARY.
                      25  CT-MAX-REF-POINT-IND PIC 9(02).
                      25  CT-MIN-REF-POINT     PIC S9(09) BINARY.
                      25  CT-MIN-REF-POINT-IND PIC 9(02).
                      25  CT-AMI-REF-POINT     PIC S9(09) BINARY.
                      25  CT-LIMIT-DATE-ASSIGNED PIC 9(08).
                      25  CT-AMT-ACCT OCCURS 16 TIMES PIC S9(07) BINARY.
                  20  CURR-EXP PIC S9(07) BINARY.
                  20  CT-ACTION   OCCURS 20 TIMES PIC X(05).
                  20  CT-INT-TRIGGER  PIC S9(04) BINARY.
              10  CT-SCEN-PARMS OCCURS 4 TIMES.
                  20  CT-LIMIT-PARMS   OCCURS 15.
                      25  CT-LIMIT-FACT  OCCURS 10.
                          30  CT-LIMIT-FACTORS PIC S9(05)V99 BINARY.
                      25  CT-MAXIMUM-VALUE  PIC S9(07) BINARY.
                      25  CT-MINIMUM-VALUE  PIC S9(07) BINARY.
                      25  CT-ROUNDING-VALUE PIC S9(05) BINARY.
                      25  CT-ALLOW-MOVE-IND PIC X(01).
              10  CT-STGY-PARMS OCCURS 4 TIMES.
                  20  CT-MAX-EXPOSURE-VALUE PIC S9(07) BINARY.
                  20  CT-STGY-LIMIT-PARM    OCCURS 15 TIMES.
                      25 CT-LIMIT-ROUND-METHOD PIC X(01).
                      25 CT-MAX-EXP-FACTOR PIC 9(03)V9(02).
                      25 CT-MAX-EXP-IND    PIC 9(01).
                      25 CT-MAX-EXP-PRIORITY PIC 9(02).
                      25 CT-LIMIT-TYPE       PIC X(02).
                      25 CT-COMPONENT-SIGNS  OCCURS 10 TIMES.
                          30 CT-COMPONENT-SIGN PIC X(01).
                      25 CT-MAX-INC   PIC S9(07) BINARY.
                      25 CT-MIN-INC   PIC S9(07) BINARY.
              10  CT-CA-EXP-FACTOR    PIC 9(03)V9(02).
              10  CT-RV-EXP-FACTOR    PIC 9(03)V9(02).
              10  CT-MG-EXP-FACTOR    PIC 9(03)V9(02).
              10  CT-LN-EXP-FACTOR    PIC 9(03)V9(02).
              10  CT-FC-MAX-EXP-SW    PIC X(01).
              10  INTRA-LOOP-PGM  PIC X(08).
              10  ALTER-LOOP-PGM  PIC X(08).