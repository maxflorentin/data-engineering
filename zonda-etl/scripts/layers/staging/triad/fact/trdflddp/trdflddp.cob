           05  HEADER.
               10  LENGTH             PIC S9(08) BINARY.
               10  TENANT-ID          PIC S9(08) BINARY.
               10  CUSTOMER-ID        PIC  X(20).
               10  REC-ID             PIC  X(02).
               10  REC-ID-NUM         PIC  9(02).
               10  REC-SEQ-NUM        PIC  9(02).
               10  ACCOUNT-ID                 PIC  X(20).
               10  PREV-ACCT-ID               PIC  X(20).
               10  DATE-OPEN                  PIC  9(08)  BINARY.
               10  DATE-CLOSED                PIC  9(08)  BINARY.
               10  REASON-CLOSED              PIC  9(01).
               10  NUM-PARTIES                PIC  9(01).
               10  DIVISOR OCCURS 2 TIMES     PIC  9(02)V9(02)
                                                          BINARY.
               10  PRIMARY-ACCT-IND           PIC  9(01).
               10  ACCOUNT-TYPE               PIC S9(03)  BINARY.
               10  FREE-FUNDS-IND             PIC S9(03)  BINARY.
                  88  DP-FREE-FUNDS              VALUE +001.
                  88  DP-FIXED-FUNDS             VALUE +002.
               10  DATE-LAST-CREDIT           PIC  9(08)  BINARY.
               10  DATE-LAST-DEBIT            PIC  9(08)  BINARY.
               10  CURR-BALANCE               PIC S9(09)  BINARY.
               10  PROFIT                     PIC S9(09)  BINARY.
               10  MONTHLY-SEGMENT OCCURS 12 TIMES
                  INDEXED BY DP-M-SEG-IDX.
                   15  DATE                   PIC  9(08)  BINARY.
                   15  BALANCE                PIC S9(09)  BINARY.
                   15  VAL-INTEREST           PIC S9(09)  BINARY.
                   15  VAL-CREDITS            PIC S9(09)  BINARY.
                   15  VAL-DEBITS             PIC S9(09)  BINARY.
                   15  VAL-INTER-ACCT-CREDITS PIC S9(09)  BINARY.
                   15  VAL-INTER-ACCT-DEBITS  PIC S9(09)  BINARY.
                   15  VAL-AUTO-DEBITS        PIC S9(09)  BINARY.
                   15  VAL-SALARY             PIC S9(09)  BINARY.
                   15  MIN-BALANCE            PIC S9(09)  BINARY.
                   15  NO-NSF                 PIC S9(03)  BINARY.
