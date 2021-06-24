      ******************************************************************        
      *                                                                *        
      * NOMBRE DEL OBJETO:  ZBECTRA0                                   *        
      *                                                                *        
      * DESCRIPCION: COMMAREA DE INTERFASE CON ATRA Y PARA CONTABILIDAD*        
      * -------------------------------------------------------------- *        
      *                                                                *        
      *  * CAMPOS DE ENTRADA                                           *        
      *  ===================                                           *        
      *                                                                *        
      *      CCC-CARGO                                                 *        
      *      DIVISA-CARGO                                              *        
      *      IMPORTE-CARGO                                             *        
      *      CONCEPTO CARGO                                            *        
      *      CODIGO CARGO                                              *        
      *      CCC-ABONO                                                 *        
      *      DIVISA-ABONO                                              *        
      *      IMPORTE-ABONO                                             *        
      *      CODIGO ABONO                                              *        
      *      FECHA-HOY                                                 *        
      *      HORA-HOY                                                  *        
      *      NIO                                                       *        
      *      CANAL                                                     *        
      *      ESTADO                                                    *        
      *      TRANSAC                                                   *        
      *      USUARIO                                                   *        
      *      COMPROBANTE                                               *        
      *                                                                *        
      ******************************************************************        
       02  ZBTCTRA.                                                             
          03 TTRA-NIO                       PIC X(24).                          
          03 TTRA-OPERACION  REDEFINES TTRA-NIO.                                
              05 TTRA-NUM-CESION            PIC X(08).                          
              05 FILLER                     PIC X(16).                          
          03 TTRA-CCC-CARGO                 PIC X(20).                          
          03  FILLER REDEFINES TTRA-CCC-CARGO.                                  
              05 TTRA-ENTIDAD-CARGO         PIC 9(04).                          
              05 TTRA-SUC-CARGO             PIC 9(04).                          
              05 TTRA-CUENTA-CARGO.                                             
                  07  FILLER                    PIC X(01).                      
                  07 TTRA-COD-PROD-CARGO        PIC 9(02).                      
                  07 TTRA-NRO-CUENTA-CARGO      PIC 9(09).                      
          03 TTRA-DIVISA-CARGO              PIC X(03).                          
          03 TTRA-IMPORTE-CARGO             PIC S9(13)V99 COMP-3.               
          03 TTRA-CONCEPTO-CARGO            PIC X(04).                          
          03 TTRA-CODIGO-CARGO              PIC X(04).                          
          03 TTRA-CCC-ABONO                 PIC X(20).                          
          03  FILLER REDEFINES TTRA-CCC-ABONO.                                  
              05 TTRA-ENTIDAD-ABONO         PIC 9(04).                          
              05 TTRA-SUC-ABONO             PIC 9(04).                          
              05 TTRA-CUENTA-ABONO.                                             
                  07  FILLER                    PIC X(01).                      
                  07 TTRA-COD-PROD-ABONO        PIC 9(02).                      
                  07 TTRA-NRO-CUENTA-ABONO      PIC 9(09).                      
          03 TTRA-DIVISA-ABONO              PIC X(03).                          
          03 TTRA-IMPORTE-ABONO             PIC S9(13)V99 COMP-3.               
          03 TTRA-CODIGO-ABONO              PIC X(04).                          
          03 TTRA-COMPROBANTE               PIC 9(08) COMP-3.                   
          03 TTRA-FECHA-HOY                 PIC X(10).                          
          03 TTRA-HORA-HOY                  PIC X(08).                          
          03 TTRA-CANAL                     PIC X(02).                          
          03 TTRA-ESTADO                    PIC X.                              
          03 TTRA-TRANSAC                   PIC X(04).                          
          03 TTRA-USUARIO                   PIC X(08).                          
          03 TTRA-VAL-LIM                   PIC X(01).                          
          03 TTRA-NUP                       PIC 9(08).                          
          03 TTRA-FECHA-VALOR               PIC X(10).                          
          03 TTRA-TERMINAL                  PIC X(4).                           
          03 TTRA-CENTRO-CONTAB             PIC X(4).                           
          03 TTRA-IDENT-OPER-CONTAB         PIC X(04).                          
             88 TTRA-88-ATRA                VALUE 'ATRA'.                       
             88 TTRA-88-CES-ALTA            VALUE 'CESA'.                       
             88 TTRA-88-CES-BAJA            VALUE 'CESB'.                       
          03 TTRA-ID-UNICO-TRANSF           PIC X(25).                          
          03 TTRA-ESTADO-TRANSF             PIC X(1).                           
          03 TTRA-FILLER                    PIC X(30).                          
          03 TTRA-TIMESTAMP                 PIC X(26).                          
      ******************************************************************        
      *                                                                *        
      *         LONGITUD TOTAL DEL REGISTRO EN BYTES : 200             *        
      *                                                                *        
      ******************************************************************        
