           02  RCL.
               10  LENGTH                 PIC S9(08) BINARY.
               10  SUBJECT-CODE           PIC X(01).
               10  TENANT-ID              PIC S9(08) BINARY.
               10  SPID                   PIC S9(03) BINARY.
               10  PROC-MODE              PIC X(10).
               10  COMP-EST-EXPT-ID       PIC X(05).
               10  PROC-DATE-CYMD         PIC 9(08) BINARY.
               10  CUSTOMER-ID            PIC X(20).
               10  PREV-CUSTOMER-ID       PIC X(20).
               10  ACCOUNT-ID             PIC X(20).
               10  PREV-ACCOUNT-ID        PIC X(20).
               10  CALL-TYPE              PIC X(01).
               10  SAMPL-FTR              PIC S9(06) BINARY.
               10  CO-PROD-TYPE           PIC 9(01).
               10  VERIFICATION-ID        PIC X(33).
           05  LENGTH-1                   PIC S9(08) BINARY.
           05  CRFA-LIM-LVL.
                10  LIMIT-DATA.
                    15  SUBJECT-CODE-1    PIC X(01).
                    15  SUBSEGMENT-CODE   PIC X(01).
                    15  DGTS-RANGE.
                        20 DGTS-LOWLMT    PIC S9(04) BINARY.
                        20 DGTS-UPLMT     PIC S9(04) BINARY.
                    15 STGY-TYPE          PIC X(03).
                    15 LIMIT-ID           PIC 9(02) BINARY.
                    15 NEW-LIMIT          PIC S9(09) BINARY.
                    15 REVIEW-TYPE        PIC 9(02).
                    15 OLD-LIMIT          PIC S9(09) BINARY.
                    15 AGREED-LIMIT       PIC S9(09) BINARY.
                    15 BALANCE            PIC S9(09) BINARY.
                    15 BALANCE-COUNT      PIC S9(08) BINARY.
                    15 LIMIT-ADJ          OCCURS 6 TIMES
                                          INDEXED BY CL-IDXADJ.
                        20 LIMIT-ADJUSTMENT
                                          PIC 9(01).
                10  FILLER                PIC X(09).
