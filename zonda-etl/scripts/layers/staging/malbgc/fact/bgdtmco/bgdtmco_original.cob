05 BGTCMCO.                                             
   10 MCO-NUM-CONVENIO     PIC S9(07)      USAGE COMP-3.
   10 MCO-CONVENIO         PIC X(04).                   
   10 MCO-PRODUCTO         PIC X(02).                   
   10 MCO-SUBPRODU         PIC X(04).                   
   10 MCO-SUSCRIPTOR       PIC X(11).                   
   10 MCO-CCC-SUSCRIPTOR.                               
      15 MCO-ENTIDAD       PIC X(04).                   
      15 MCO-CENTRO-ALTA   PIC X(04).                   
      15 MCO-CUENTA        PIC X(12).                   
   10 MCO-DIVISA           PIC X(03).                   
   10 MCO-INDESTA          PIC X(01).                   
      88 MCO-INDESTA-A    VALUES 'A'.                   
      88 MCO-INDESTA-D    VALUES 'D'.                   
   10 MCO-FECHA-EST        PIC X(10).                   
   10 MCO-NUM-MES          PIC S9(03)      USAGE COMP-3.
   10 MCO-SALDO-MEDIO      PIC S9(13)V9(2) USAGE COMP-3.
   10 MCO-SALDO-MED-CV     PIC S9(13)V9(4) USAGE COMP-3.
   10 MCO-TIPO-CONVENIO    PIC X(01).                   
   10 MCO-NUM-ASOCIADOS    PIC S9(05)      USAGE COMP-3.
   10 MCO-DIAS-VIGENCIA    PIC S9(03)      USAGE COMP-3.
   10 MCO-ENTIDAD-UMO      PIC X(04).                   
   10 MCO-CENTRO-UMO       PIC X(04).                   
   10 MCO-USERID-UMO       PIC X(08).                   
   10 MCO-NETNAME-UMO      PIC X(08).                   
   10 MCO-TIMEST-UMO       PIC X(26).