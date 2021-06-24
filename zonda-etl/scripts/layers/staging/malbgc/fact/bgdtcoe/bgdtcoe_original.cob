01  BGTCCOE.                                                   
    05  COE-CLAVE.                                             
        10  COE-CCC.                                           
            15  COE-ENTIDAD         PIC X(04)   VALUE SPACES.  
            15  COE-CENTRO-ALTA     PIC X(04)   VALUE SPACES.  
            15  COE-CUENTA          PIC X(12)   VALUE SPACES.  
        10  COE-DIVISA              PIC X(03)   VALUE SPACES.  
        10  COE-CONCEPTO            PIC X(04)   VALUE SPACES.  
        10  COE-FECHA-DESDE         PIC X(10)   VALUE SPACES.  
                                                               
    05  COE-DATOS.                                             
        10  COE-PLAN                PIC X(04)   VALUE SPACES.  
        10  COE-ESTADO              PIC X(01)   VALUE SPACES.  
        10  COE-FECHA-HASTA         PIC X(10)   VALUE SPACES.  
        10  COE-FECHA-CAMBIO-EST    PIC X(10)   VALUE SPACES.  
        10  COE-IND-COMESPECIF      PIC X(01)   VALUE SPACES.  
        10  COE-FECHA-ULTLIQ        PIC X(10)   VALUE SPACES.  
        10  COE-FECHA-PROLIQ        PIC X(10)   VALUE SPACES.  
        10  COE-POR-REDUC           PIC S9(3)V9(5) COMP-3      
                            VALUE ZEROS.                       
        10  COE-POR-SECUND          PIC S9(3)V9(5) COMP-3
                            VALUE ZEROS.
        10  COE-CCC-SECUND.
            15  COE-ENTIDAD-SECUND  PIC X(04)   VALUE SPACES.
            15  COE-CENTRO-SECUND   PIC X(04)   VALUE SPACES.
            15  COE-CUENTA-SECUND   PIC X(12)   VALUE SPACES.
        10  COE-DIVISA-SECUND       PIC X(03)   VALUE SPACES.

        10  COE-IND-BOF             PIC X(01)   VALUE SPACES.
        10  COE-PERIOD-COM          PIC X(01).
            88  COE-PERIOD-COM-J                VALUE 'J'.
            88  COE-PERIOD-COM-P                VALUE 'P'.

        10  COE-PERIOD-COBR         PIC X(01).
            88  COE-PERIOD-COBR-D               VALUE 'D'.
            88  COE-PERIOD-COBR-W               VALUE 'W'.
            88  COE-PERIOD-COBR-Q               VALUE 'Q'.
            88  COE-PERIOD-COBR-M               VALUE 'M'.
            88  COE-PERIOD-COBR-B               VALUE 'B'.
            88  COE-PERIOD-COBR-T               VALUE 'T'.
            88  COE-PERIOD-COBR-S               VALUE 'S'.
            88  COE-PERIOD-COBR-A               VALUE 'A'.
            88  COE-PERIOD-COBR-V               VALUE 'V'.
            88  COE-PERIOD-COBR-N               VALUE ' '.

        10  COE-CPO-LIBRE           PIC S9(3)   COMP-3
                                                VALUE ZEROS.
        10  COE-COMI-IM             PIC S9(13)V9(4) COMP-3
                            VALUE ZEROS.
        10  COE-COMI-PO             PIC S9(03)V9(5) COMP-3
                            VALUE ZEROS.
        10  COE-COMI-MIN            PIC S9(13)V9(4) COMP-3
                            VALUE ZEROS.                       
        10  COE-COMI-MAX            PIC S9(13)V9(4) COMP-3     
                            VALUE ZEROS.                       
        10  COE-DIV-COM             PIC X(03)   VALUE SPACES.  
        10  COE-DIA-NAT-COBR        PIC S9(3)   COMP-3         
                                                VALUE ZEROS.   
        10  COE-DIAS-CALC-PROP      PIC S9(3)   COMP-3         
                                                VALUE ZEROS.   
                                                               
    05  COE-STAMP.                                             
        10  COE-ENTIDAD-UMO         PIC X(04)   VALUE SPACES.  
        10  COE-CENTRO-UMO          PIC X(04)   VALUE SPACES.  
        10  COE-USERID-UMO          PIC X(08)   VALUE SPACES.  
        10  COE-NETNAME-UMO         PIC X(08)   VALUE SPACES.  
        10  COE-TIMEST-UMO          PIC X(26)   VALUE SPACES.