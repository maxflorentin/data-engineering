           05  HEADER.
               10  LENGTH             PIC S9(08) BINARY.
               10  TENANT-ID          PIC S9(08) BINARY.
               10  CUSTOMER-ID        PIC  X(20).
               10  REC-ID             PIC  X(02).
               10  REC-ID-NUM         PIC  9(02).
               10  REC-SEQ-NUM        PIC  9(02).
               10  ACCOUNT-ID             PIC  X(20).
               10  PREV-ACCT-ID           PIC  X(20).
               10  DATE-OPEN              PIC  9(08)  BINARY.
               10  DATE-CLOSED            PIC  9(08)  BINARY.
               10  REASON-CLOSED          PIC  9(01).
               10  NUM-PARTIES            PIC  9(01).
               10  DIVISOR OCCURS 2 TIMES PIC  9(02)V9(02)
                                                      BINARY.
               10  LIABILITY-IND          PIC  9(01).
               10  PRIMARY-ACCT-IND       PIC  9(01).
               10  ACCOUNT-TYPE           PIC S9(03)  BINARY.
               10  ACCOUNT-SPR-TYPE       PIC  9(01).
               10  LOAN-PURPOSE           PIC  9(01).
               10  ORIGINAL-TERM          PIC S9(03)  BINARY.
               10  REMAINING-TERM         PIC S9(03)  BINARY.
               10  ORIGINAL-LOAN-AMOUNT   PIC S9(09)  BINARY.
               10  DATE-START-ARREARS     PIC  9(08)  BINARY.
               10  DATE-FIRST-INSTALLMENT PIC  9(08)  BINARY.
               10  DATE-LAST-PAY          PIC  9(08)  BINARY.
               10  PAYMENT-FREQUENCY      PIC  9(02).
               10  STNDRD-INSTALLMENT-AMT PIC S9(09)  BINARY.
               10  PAYMENT-METHOD         PIC  9(01).
               10  INSURANCE              PIC  9(01).
               10  TRIAD-CAT              PIC  9(02).
               10  VAL-FEES-GROUP.
                   15  VAL-FEES OCCURS 5 TIMES PIC S9(09)  BINARY.
               10  PROFIT                      PIC S9(09)  BINARY.
               10  PROVISION                   PIC S9(09)  BINARY.
               10  APPORTIONMNT-EXCL-IND       PIC  9(01).
               10  MONTHLY-SEGMENT OCCURS 12 TIMES
                   INDEXED BY LN-M-SEG-IDX.
                   15  DATE-DUE        PIC  9(08)  BINARY.
                   15  BLOCK-CODE      PIC  X(04).
                   15  BALANCE         PIC S9(09)  BINARY.
                   15  INSTALLMENT-DUE PIC S9(09)  BINARY.
                   15  VAL-PAYMENTS    PIC S9(09)  BINARY.
                   15  VAL-ARREARS     PIC S9(09)  BINARY.
                   15  VAL-INTEREST    PIC S9(09)  BINARY.
                   15  VAL-TOTAL-FEES  PIC S9(09)  BINARY.
               10  MONTHLY-SEGMENT2 OCCURS 24 TIMES
                   INDEXED BY LN-M-SEG2-IDX.
                   15  NUM-MTHS-IN-ARREARS PIC S9(03)  BINARY.
