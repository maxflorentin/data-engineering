      ******************************************************************        
      * DCLGEN TABLE(BGDTUMO)                                          *        
      *        LIBRARY(MD1.CTOCTAS.DCA(BGGTUMO))                       *        
      *        ACTION(REPLACE)                                         *        
      *        LANGUAGE(COBOL)                                         *        
      *        NAMES(UMOR-)                                            *        
      *        QUOTE                                                   *        
      *        DBCSDELIM(NO)                                           *        
      *        COLSUFFIX(YES)                                          *        
      * ... IS THE DCLGEN COMMAND THAT MADE THE FOLLOWING STATEMENTS   *        
      ******************************************************************        

      ******************************************************************        
      * COBOL DECLARATION FOR TABLE BGDTUMO                            *        
      ******************************************************************        
       01  UMO.
      *                       ENTIDAD                                           
           10 ENTIDAD         PIC X(4).                                    
      *                       CENTRO_ALTA                                       
           10 CENTRO-ALTA     PIC X(4).                                    
      *                       CUENTA                                            
           10 CUENTA          PIC X(12).                                   
      *                       DIVISA                                            
           10 DIVISA          PIC X(3).                                    
      *                       NUMER_MOV                                         
           10 NUMER-MOV       PIC S9(9)V USAGE COMP-3.                     
      *                       CODIGO                                            
           10 CODIGO          PIC X(4).                                    
      *                       IMPORTE                                           
           10 IMPORTE         PIC S9(13)V9(2) USAGE COMP-3.                
      *                       CONCEPTO                                          
           10 CONCEPTO        PIC X(4).                                    
      *                       BASE_IMPUESTO                                     
           10 BASE-IMPUESTO   PIC S9(13)V9(2) USAGE COMP-3.                
      *                       COD_IMPUESTO                                      
           10 COD-IMPUESTO    PIC X(4).                                    
      *                       IMPOR_IMPUESTO                                    
           10 IMPOR-IMPUESTO  PIC S9(13)V9(2) USAGE COMP-3.                
      *                       CPTO_IMPUESTO                                     
           10 CPTO-IMPUESTO   PIC X(4).                                    
      *                       FECHA_CONTA                                       
           10 FECHA-CONTA     PIC X(10).                                   
      *                       FECHA_OPER                                        
           10 FECHA-OPER      PIC X(10).                                   
      *                       HORA_OPER                                         
           10 HORA-OPER       PIC X(4).                                    
      *                       FECHA_VALOR                                       
           10 FECHA-VALOR     PIC X(10).                                   
      *                       CHEQUE                                            
           10 CHEQUE          PIC S9(9)V USAGE COMP-3.                     
      *                       NUM_SERIE                                         
           10 NUM-SERIE       PIC S9(8)V USAGE COMP-3.                     
      *                       MINIBANCO                                         
           10 MINIBANCO       PIC X(6).                                    
      *                       REF_INTERNA                                       
           10 REF-INTERNA     PIC X(15).                                   
      *                       IND_DESDOBLA                                      
           10 IND-DESDOBLA    PIC X(1).                                    
      *                       IND_OBSERVA                                       
           10 IND-OBSERVA     PIC X(1).                                    
      *                       SALDO_AUT                                         
           10 SALDO-AUT       PIC S9(13)V9(2) USAGE COMP-3.                
      *                       ENTIDAD_CONT                                      
           10 ENTIDAD-CONT    PIC X(4).                                    
      *                       CENTRO_CONT                                       
           10 CENTRO-CONT     PIC X(4).                                    
      *                       PRODUCTO                                          
           10 PRODUCTO        PIC X(2).                                    
      *                       SUBPRODU                                          
           10 SUBPRODU        PIC X(4).                                    
      *                       IND_FICTICIO                                      
           10 IND-FICTICIO    PIC X(1).                                    
      *                       IND_AOM                                           
           10 IND-AOM         PIC X(1).                                    
      *                       IND_ACT                                           
           10 IND-ACT         PIC X(1).                                    
      *                       IND_ANULABLE                                      
           10 IND-ANULABLE    PIC X(1).                                    
      *                       IND_ANUL                                          
           10 IND-ANUL        PIC X(1).                                    
      *                       IND_CAR_ABO                                       
           10 IND-CAR-ABO     PIC X(1).                                    
      *                       IND_GIRO_DEP                                      
           10 IND-GIRO-DEP    PIC X(1).                                    
      *                       CODIGO_UR                                         
           10 CODIGO-UR       PIC X(3).                                    
      *                       TIPO_CAMBIO                                       
           10 TIPO-CAMBIO     PIC S9(3)V9(6) USAGE COMP-3.                 
      *                       CANAL                                             
           10 CANAL           PIC X(2).                                    
      *                       IND_CONTAB                                        
           10 IND-CONTAB      PIC X(1).                                    
      *                       IND_IMPTO_MIR                                     
           10 IND-IMPTO-MIR   PIC X(1).                                    
      *                       IND_PPAL                                          
           10 IND-PPAL        PIC X(1).                                    
      *                       TRANSACCION                                       
           10 TRANSACCION     PIC X(4).                                    
      *                       COD_APLICACION                                    
           10 COD-APLICACION  PIC X(2).                                    
      *                       TERM_NIO                                          
           10 TERM-NIO        PIC X(4).                                    
      *                       FECHA_NIO                                         
           10 FECHA-NIO       PIC X(10).                                   
      *                       HORA_NIO                                          
           10 HORA-NIO        PIC X(8).                                    
      *                       COD_OPER_PPAL                                     
           10 COD-OPER-PPAL   PIC X(4).                                    
      *                       COD_DESTINO_MOV                                   
           10 COD-DESTINO-MOV  PIC X(1).                                   
      *                       COD_TRIBUTA                                       
           10 COD-TRIBUTA     PIC X(1).                                    
      *                       IND_ACRED_SUELDO                                  
           10 IND-ACRED-SUELDO  PIC X(1).                                  
      *                       NUMER_RET                                         
           10 NUMER-RET       PIC S9(5) USAGE COMP-3.
      *                       COD_APLIC_CONTAB                                  
           10 COD-APLIC-CONTAB  PIC X(2).                                  
      *                       CENTRO_ORIGEN                                     
           10 CENTRO-ORIGEN   PIC X(4).                                    
      *                       USERID_AUT                                        
           10 USERID-AUT      PIC X(8).                                    
      *                       IND_MOV_GENUINO                                   
           10 IND-MOV-GENUINO  PIC X(1).                                   
      *                       ENTIDAD_UMO                                       
           10 ENTIDAD-UMO     PIC X(4).                                    
      *                       CENTRO_UMO                                        
           10 CENTRO-UMO      PIC X(4).                                    
      *                       USERID_UMO                                        
           10 USERID-UMO      PIC X(8).                                    
      *                       CAJERO_UMO                                        
           10 CAJERO-UMO      PIC X(1).                                    
      *                       NETNAME_UMO                                       
           10 NETNAME-UMO     PIC X(8).                                    
      *                       TIMEST_UMO                                        
           10 TIMEST-UMO      PIC X(26).                                   
      *                       DISCRIMINANTE                                     
           10 DISCRIMINANTE   PIC X(10).                                   
      *                       DIV_CONTRARIA                                     
           10 DIV-CONTRARIA   PIC X(3).                                    
      ******************************************************************        
      * THE NUMBER OF COLUMNS DESCRIBED BY THIS DECLARATION IS 62      *        
      ******************************************************************        
