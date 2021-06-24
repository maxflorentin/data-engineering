           02  RXT.
               10  LENGTH                  PIC S9(08) BINARY.
               10  SUBJECT-CODE            PIC X(01).
               10  TENANT-ID               PIC S9(08) BINARY.
               10  SPID                    PIC S9(03) BINARY.
               10  PROC-MODE               PIC X(10).
               10  COMP-EST-EXPT-ID        PIC X(05).
               10  PROC-DATE-CYMD          PIC 9(08) BINARY.
               10  CUSTOMER-ID             PIC X(20).
               10  PREV-CUSTOMER-ID        PIC X(20).
               10  ACCOUNT-ID              PIC X(20).
               10  PREV-ACCOUNT-ID         PIC X(20).
               10  CALL-TYPE               PIC X(01).
               10  SAMPL-FTR               PIC S9(06) BINARY.
               10  CO-PROD-TYPE            PIC 9(01).
               10  VERIFICATION-ID         PIC X(33).
               10  CHARS-LENGTH            PIC S9(08) BINARY.
               10  XT-SUBJECT-CODE         PIC X(01).
               10  XT-SUBSEGMENT-CODE      PIC X(01).
               10  XT-MAX-ENTRIES          PIC S9(04) BINARY.
               10  XT-NUM-ENTRIES          PIC S9(04) BINARY.
               10  XT-ENTRIES              OCCURS 653 TIMES.
                   20  NUMBER              PIC S9(04) BINARY.
                   20  STND-CUST-IND       PIC X.
                   20  TYPE                PIC X(01).
                   20  ATTR-VALUE          PIC S9(09) BINARY.