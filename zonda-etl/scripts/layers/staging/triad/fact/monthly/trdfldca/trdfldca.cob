           05  DCA.
               10  LENGTH                        PIC S9(08) BINARY.
               10  TENANT-ID                     PIC S9(08) BINARY.
               10  CUSTOMER-ID                   PIC  X(20).
               10  REC-ID                        PIC  X(02).
               10  REC-ID-NUM                    PIC  9(02).
               10  REC-SEQ-NUM                   PIC  9(02).
               10  ACCOUNT-ID                    PIC  X(20).
               10  PREV-ACCT-ID                  PIC  X(20).
               10  DATE-OPEN                     PIC  9(08)  BINARY.
               10  DATE-CLOSED                   PIC  9(08)  BINARY.
               10  REASON-CLOSED                 PIC  9(01).
               10  NUM-PARTIES                   PIC  9(01).
               10  DIVISOR OCCURS 2 TIMES        PIC  9(02)V9(02)
                                                             BINARY.
               10  LIABILITY-IND                 PIC  9(01).
               10  ACCOUNT-TYPE                  PIC S9(03)  BINARY.
               10  ACCOUNT-SPR-TYPE              PIC  9(01).
               10  AMT-LAST-CREDIT               PIC S9(09)  BINARY.
               10  DATE-LAST-CREDIT              PIC  9(08)  BINARY.
               10  DATE-LAST-DEBIT               PIC  9(08)  BINARY.
               10  CURR-BALANCE                  PIC S9(09)  BINARY.
               10  CURR-AGREED-LIMIT             PIC S9(09)  BINARY.
               10  DATE-START-DEBIT-BAL          PIC  9(08)  BINARY.
               10  DATE-START-OVERLIMIT          PIC  9(08)  BINARY.
               10  PRIMARY-ACCT-IND              PIC  9(01).
               10  FACILITIES-IND                PIC  9(03)  BINARY.
               10  FACILITY-1                    PIC  9(01).
               10  FACILITY-2                    PIC  9(01).
               10  FACILITY-3                    PIC  9(01).
               10  FACILITY-4                    PIC  9(01).
               10  FACILITY-RENEWAL-IND          PIC  9(01).
               10  TRIAD-CAT                     PIC  9(02).
               10  SHADOW-LIMIT OCCURS 2 TIMES
                                                 PIC S9(09)  BINARY.
               10  DEBIT-INTEREST-1              PIC S9(09)  BINARY.
               10  DEBIT-INTEREST-2              PIC S9(09)  BINARY.
               10  FEES-GROUP.
                   15  FEES OCCURS 5 TIMES
                                                 PIC S9(09)  BINARY.
               10  CREDITS-2                     PIC S9(09)  BINARY.
               10  CREDITS-3                     PIC S9(09)  BINARY.
               10  TXN1-NUM                      PIC S9(03)  BINARY.
               10  TXN1-VAL                      PIC S9(09)  BINARY.
               10  TXN2-NUM                      PIC S9(03)  BINARY.
               10  TXN2-VAL                      PIC S9(09)  BINARY.
               10  TXN-SHAD-NUM                  PIC S9(03)  BINARY.
               10  TXN-SHAD-VAL                  PIC S9(09)  BINARY.
               10  ATM-TXNS                      PIC S9(09)  BINARY.
               10  DEBIT-CARD-TXNS               PIC S9(09)  BINARY.
               10  CHQ-TXNS                      PIC S9(09)  BINARY.
               10  PROFIT                        PIC S9(09)  BINARY.
               10  PROVISION                     PIC S9(09)  BINARY.
               10  APPORTIONMNT-EXCL-IND         PIC  9(01).
               10  MONTHLY-SEGMENT OCCURS 12 TIMES
                  INDEXED BY M-SEG-IDX.
                   15 DATE                   PIC  9(08)  BINARY.
                   15 BLOCK-CODE             PIC  X(04).
                   15 MAX-BALANCE            PIC S9(09)  BINARY.
                   15 MIN-BALANCE            PIC S9(09)  BINARY.
                   15 AVG-BALANCE            PIC S9(09)  BINARY.
                   15 EOM-BALANCE            PIC S9(09)  BINARY.
                   15 EOM-DAYS-IN-EXCESS     PIC S9(03)  BINARY.
                   15 NUM-CUST-DEBITS        PIC S9(03)  BINARY.
                   15 VAL-CUST-DEBITS        PIC S9(09)  BINARY.
                   15 VAL-INTER-ACCT-DEBITS  PIC S9(09)  BINARY.
                   15 VAL-DEBIT-INTEREST     PIC S9(09)  BINARY.
                   15 VAL-TOTAL-FEES         PIC S9(09)  BINARY.
                   15 VAL-OTHER-DEBITS       PIC S9(09)  BINARY.
                   15 NUM-CUST-CREDITS       PIC S9(03)  BINARY.
                   15 VAL-CUST-CREDITS       PIC S9(09)  BINARY.
                   15 VAL-INTER-ACCT-CREDITS PIC S9(09)  BINARY.
                   15 VAL-CREDIT-INTEREST    PIC S9(09)  BINARY.
                   15 VAL-OTHER-CREDITS      PIC S9(09)  BINARY.
                   15 NUM-DAYS-IN-CREDIT     PIC S9(03)  BINARY.
                   15 NUM-DAYS-IN-DEBIT      PIC S9(03)  BINARY.
                   15 NUM-DAYS-IN-EXCESS     PIC S9(03)  BINARY.
                   15 MAX-CNSCT-DAYS-IN-EXCS PIC S9(03)  BINARY.
                   15 MAX-VAL-EXCESS         PIC S9(09)  BINARY.
                   15 AGREED-LIMIT           PIC S9(09)  BINARY.
                   15 SALARY-IND             PIC  9(01).
                   15 VAL-SALARY             PIC S9(09)  BINARY.
                   15 NUM-DIR-DEPOSITS       PIC S9(03)  BINARY.
                   15 VAL-DIR-DEPOSITS       PIC S9(09)  BINARY.
                   15 NUM-AUTO-DEBITS        PIC S9(03)  BINARY.
                   15 VAL-AUTO-DEBITS        PIC S9(09)  BINARY.
                   15 NUM-RET-PYMT           PIC S9(03)  BINARY.
                   15 VAL-RET-PYMT           PIC S9(09)  BINARY.
                   15 VAL-UTILITY-PYMT       PIC S9(09)  BINARY.
