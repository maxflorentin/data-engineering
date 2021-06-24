           02  RCR.
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
               10  CR-LENGTH           PIC S9(08) BINARY.
               10  CR-SUBJECT-CODE     PIC X(01).
               10  CR-SUBSEGMENT-CODE  PIC X(01).
               10  CR-HD-TENANT-ID     PIC S9(08) BINARY.
               10  CR-HD-SPID          PIC S9(03) BINARY.
               10  CR-HD-PROC-DATE-CYM PIC 9(06) BINARY.
               10  CR-DGTS-LOWLMT      PIC S9(04) BINARY.
               10  CR-DGTS-UPLMT       PIC S9(04) BINARY.
               10  CR-STGY-TYPE        PIC X(03).
               10  CR-LIMIT-MODEL-ID   PIC 9(03).
               10  CR-SCEN-ID          PIC 9(04).
               10  CR-SCEN-SEG-ID      PIC X(05).
               10  CR-STGY-ROW         PIC S9(08) BINARY.
               10 CR-MAPPED-ACTIONS    OCCURS 20 TIMES
                                    INDEXED BY CR-IDX PIC X(05).
               10  CR-FILTER-KEYS      OCCURS 3 TIMES
                                    INDEXED BY CR-K-IDX.
                   15 CR-FILTER-VAL    PIC S9(08) BINARY.
               10  CR-CU-RAW-SCORE     PIC S9(07) BINARY.
               10  CR-CU-ALIGNED-SCORE PIC S9(07) BINARY.
               10  CR-RISK-FACTOR      PIC S9(01)V9(07) BINARY.
               10  CR-HD-SAMPL-FTR     PIC S9(06) BINARY.
               10  CR-HD-PROC-MODE     PIC X(10).
               10 CR-NUM-LMT           PIC S9(02) BINARY.
               10 CR-LIMIT-ENTRY OCCURS 1 TO 3 TIMES
                                    DEPENDING ON CR-NUM-LMT
                                    INDEXED BY CR-LMT-IDX.
                  20  CR-LIMIT-ID      PIC 9(02) BINARY.
                  20  CR-NEW-LIMIT     PIC S9(09) BINARY.
                  20  CR-REVIEW-TYPE   PIC 9(02).
                  20  CR-OLD-LIMIT     PIC S9(09) BINARY.
                  20  CR-AGREED-LIMIT  PIC S9(09) BINARY.
                  20  CR-BALANCE       PIC S9(09) BINARY.
                  20  CR-BALANCE-COUNT PIC S9(08) BINARY.
                  20  CR-LIMIT-ADJ     OCCURS 6 TIMES
                                         INDEXED BY CR-IDXADJ.
                      25 CR-LIMIT-ADJUSTMENT PIC 9(01).

