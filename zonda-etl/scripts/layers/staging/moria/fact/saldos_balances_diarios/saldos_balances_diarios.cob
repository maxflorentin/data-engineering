      * -----------------------------------------------------*                  
      *  DESCRIPCION DEL ARCHIVO DE INTERFASE DE SALDOS      *                  
      *  PARA EL SISTEMA DE GESTION CORPORATIVO MIS-AMERICA  *                  
      *                                                      *                  
      *  LONGITUD DE REG. = 183 - FIJO     BLOQUEADO         *                  
      * -----------------------------------------------------*                  
      *                                                                         
      * COD-ENTIDAD              :codigo de entidad                             
      *                                                                         
      * COD-CENTRO               :codigo de centro                              
      *                                                                         
      * NUM-CTA                  :numero de cuenta                              
      *                                                                         
      * COD-PRODUCTO             :codigo de producto                            
      *                                                                         
      * COD-SUBPRODU             :codigo de subproducto                         
      *                                                                         
      * NUM-SECUENCIA-CTO        :numero secuencia contrato                     
      *                                                                         
      * CLAVINT                  :clave de interface enviado a                  
      *                           contabilidad para mvto.informado              
      *                                                                         
      * CLACONV                  :clave de concepto enviado a                   
      *                           contabilidad para mvto.informado              
      *                                                                         
      * IMPOR-SALDO-MO           :importe saldo puntual moneda                  
      *                           origen formato packed                         
      *                                                                         
      * IMPOR-PROME-MO           :importe saldo promedio moneda                 
      *                           origen formato packed                         
      *                                                                         
      * IMPOR-SALDO-ML           :importe saldo puntual moneda                  
      *                           local formato packed                          
      *                                                                         
      * IMPOR-PROME-ML           :importe saldo promedio moneda                 
      *                           local formato packed                          
      *                                                                         
      * COD-MONEDA               :divisa del saldo informado                    
      *                           formato ISO                                   
      *                                                                         
      * RUBRO-ALTAIR             :rubro en formato ALTAIR                       
      *                                                                         
      * CENTRO-CONTABLE          :centro contable                               
      * -------------------------------------------------------------           
      *                                                                         
       01  REGISTRO-ODSSAL.                                                     
           03  ODSSAL-LLAVE-PRIMARIA.                                           
               05  ODSSAL-COD-ENTIDAD          PIC X(4).                        
               05  ODSSAL-COD-CENTRO           PIC X(4).                        
               05  ODSSAL-NUM-CUENTA           PIC X(12).                       
               05  ODSSAL-COD-PRODUCTO         PIC X(2).                        
               05  ODSSAL-COD-SUBPRODU         PIC X(4).                        
               05  ODSSAL-NUM-SECUENCIA-CTO    PIC 9(5).                        
               05  ODSSAL-CLVINT               PIC X(3).                        
               05  ODSSAL-CLVCONV              PIC X(81).                       
           03  ODSSAL-DATOS.                                                    
               05  ODSSAL-IMPOR-SALDO-MO       PIC S9(15)V99 COMP-3.            
               05  ODSSAL-IMPOR-PROME-MO       PIC S9(15)V99 COMP-3.            
               05  ODSSAL-IMPOR-SALDO-ML       PIC S9(15)V99 COMP-3.            
               05  ODSSAL-IMPOR-PROME-ML       PIC S9(15)V99 COMP-3.            
               05  ODSSAL-COD-MONEDA           PIC X(3).                        
               05  ODSSAL-RUBRO-ALTAIR         PIC X(15).                       
               05  ODSSAL-COD-CENTRO-CONT      PIC X(04).                       
