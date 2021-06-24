      ******************************************************************        
      *                       B S C H - ALTAIR                         *        
      * -------------------------------------------------------------- *        
      * ›REA       - ACTIVO                                            *        
      * APLICACI‰N - PRàSTAMOS Y AVALES                                *        
      * -------------------------------------------------------------- *        
      * REGISTRO          - UGTCRFR                                    *        
      * FECHA DE CREACI‰N - 24-NOV-1999                                *        
      *                                                                *        
      * COPY DE LA TABLA DE CONVERSION CUENTA ANTIGUA A CUENTA NUEVA   *        
      * (CCC)                                                          *        
      ******************************************************************        
      *                     LOG DE MODIFICACIONES                      *        
      * -------------------------------------------------------------- *        
      * FECHA    | AUTOR     | DESCRIPCI‰N                             *        
      * -------------------------------------------------------------- *        
      *                                                                *        
      ******************************************************************        
      *                                                                *        
      * CAMPO                      DESCRIPCIÏN                         *        
      * -------------------------- ----------------------------------- *        
      *                                                                *        
      *CLAVE.                                                          *        
      *  CCC-NUEVA --------------- CUENTA NUEVA                        *        
      *    CUENTA-NUEVA ---------- NUMERO DE PRESTAMO DE LA CUENTA NUEV*        
      *    ENTIOFI                                                     *        
      *      OFICINA-NUEVA ------- OFICINA DE LA CUENTA NUEVA          *        
      *      ENTIDAD-NUEVA ------- ENTIDAD DE LA CUENTA NUEVA          *        
      *    PROD-NUEVO ------------ PRODUCTO DE LA CUENTA NUEVA         *        
      *    SUBPRO-NUEVO ---------- SUBPRODUCTO NUEVO                   *        
      *DATOS.                                                          *        
      *  UNCTAVEL                                                      *        
      *    UNVELL                                                      *        
      *       UNANY -------------- NUMERO  A#O DEL EXPEDIENTE          *        
      *    UCLAVEAN -------------- CLAVE ANTERIOR                      *        
      *  UFTRASP ----------------- FECHA DE TRASPASO DE LA MAE         *        
      *STAMP.                                                          *        
      *  ENTIDAD-UMO  ------------  ENTIDAD ULTIMA MODIFICACION        *        
      *  CENTRO-UMO   ------------  CENTRO ULTIMA MODIFICACION         *        
      *  USERID-UMO   ------------  USUARIO ULTIMA MODIFICACION        *        
      *  NETNAME-UMO  ------------  TERMINAL ULTIMA MODIFICACION       *        
      *  TIMEST-UMO   ------------  FECHA Y HORA ULTIMA MODIFICACION   *        
      ******************************************************************        
       02 UGTCRFR.                                                              
         05 RFR-CLAVE.                                                          
           10 RFR-CCC-NUEVA.                                                    
             15 RFR-CUENTA-NUEVA     PIC X(12).                                 
             15 RFR-ENTIOFI.                                                    
               20 RFR-OFICINA-NUEVA  PIC X(4).                                  
               20 RFR-ENTIDAD-NUEVA  PIC X(4).                                  
             15 RFR-PROD-NUEVO       PIC X(2).                                  
             15 RFR-SUBPRO-NUEVO     PIC X(4).                                  
         05 RFR-DATOS.                                                          
           10 RFR-UNCTAVEL.                                                     
             15 RFR-RFR-UNVELL.                                                 
               20 RFR-UNANY          PIC X(4).                                  
             15 RFR-UCLAVEAN         PIC X(20).                                 
           10 RFR-UFTRASP            PIC X(10).                                 
         05 RFR-STAMP.                                                          
           10 RFR-ENTIDAD-UMO      PIC X(4).                                    
           10 RFR-CENTRO-UMO       PIC X(4).                                    
           10 RFR-USERID-UMO       PIC X(8).                                    
           10 RFR-NETNAME-UMO      PIC X(8).                                    
           10 RFR-TIMEST-UMO       PIC X(26).                                   
