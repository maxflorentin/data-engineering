******************************************************************00010000
      * PROYECTO: OBTENCION DE CLIENTES MIPYME                         *00020000
      * NOMBRE DEL OBJETO:  WAAPE685                                   *00030000
      *                                                                *00040000
      * ARCHIVO MIPYME CON DEUDA INFORMADA                             *00050000
      *                                                                *00070000
      * LONGITUD TOTAL DEL REGISTRO EN BYTES :     243                 *00080002
      *                                                                *00090000
      ******************************************************************00100000
      * MODIFICACION     : SE INCORPORA DEUDA PRETAMOS Y DEUDA MIPYME  *00110000
      * PETICION         : POR AUMENTO LIMITES MIPYME ID 4530          *00120000
      * AUTOR            : RUBEN SANCHEZ                               *00130000
      * FECHA            : 10-06-2013                                  *00140000
      ******************************************************************00330000
       03 WAAPE683-REG-CPA.                                                     
          05 WAAPEPYM-PENUMPER             PIC X(08).                           
          05 WAAPEPYM-TIPDOC               PIC X(02).                           
          05 WAAPEPYM-DOCUM                PIC X(11).                           
          05 WAAPEPYM-APEYNOMB             PIC X(60).                           
          05 WAAPEPYM-PETIPPER             PIC X(01).                           
             88 W683-ES-PJ                              VALUE 'J'.              
             88 W683-ES-PF                              VALUE 'F'.              
          05 WAAPEPYM-PESEGCAL             PIC X(03).                           
             88 W683-SEG-INDIVIDUO         VALUE 'A  ' 'B  '                    
                                                 'C  ' 'S  '.                   
             88 W683-SEG-P1-C1             VALUE 'P1 ' 'C1 '.                   
      *   ------------------------------------------------ HASTA AQUI:85        
          05 WAAPEPYM-BAL-MONT-FEC.                                             
             10 WAAPEPYM-MONT-BAL1         PIC S9(13)V9(02).                    
             10 WAAPEPYM-MONT-BAL1-X REDEFINES                                  
                WAAPEPYM-MONT-BAL1         PIC X(15).                           
             10 WAAPEPYM-FEC-BAL1          PIC 9(08).                           
             10 WAAPEPYM-MONT-BAL2         PIC S9(13)V9(02).                    
             10 WAAPEPYM-MONT-BAL2-X REDEFINES                                  
                WAAPEPYM-MONT-BAL2         PIC X(15).                           
             10 WAAPEPYM-FEC-BAL2          PIC 9(08).                           
             10 WAAPEPYM-MONT-BAL3         PIC S9(13)V9(02).                    
             10 WAAPEPYM-MONT-BAL3-X REDEFINES                                  
                WAAPEPYM-MONT-BAL3         PIC X(15).                           
             10 WAAPEPYM-FEC-BAL3          PIC 9(08).                           
      *   ----------------------------------------------- HASTA AQUI:154        
          05 WAAPEPYM-CLANAE10             PIC X(03).                           
          05 WAAPEPYM-PERESIVA             PIC X(02).                           
             88 W683-RESP-INSCRIPTO                    VALUE '05' '06'.         
             88 W683-MONOTRIBUTISTA                    VALUE '02'.              
          05 WAAPEPYM-MARCA-SEPYME         PIC X(01).                           
             88 W683-TIENE-SEPYME                      VALUE 'S'.               
          05 WAAPEPYM-MARCA-5319           PIC X(03).                           
             88 W683-TIENE-NMP                         VALUE 'NMP'.             
             88 W683-TIENE-MP                          VALUE 'MP '.             
          05 WAAPEPYM-COD-MIPYME           PIC X(02).                           
          05 WAAPEPYM-FECHA-PROC           PIC X(10).                           
      *   ----------------------------------------------- HASTA AQUI:175        
          05 WAAPEPYM-ORIGEN-REG           PIC X(03).                           
          05 WAAPEPYM-MARCA-BYP            PIC X(01).                           
             88 W683-TIENE-BIANDPI                     VALUE 'S'.               
          05 WAAPEPYM-MOTIVO-OK            PIC X(01).                           
          05 WAAPEPYM-TIP-DOC-ADSF         PIC X(02).                           
          05 WAAPEPYM-FEC-INSC             PIC X(10).                           
          05 WAAPEPYM-PENUMGRU             PIC 9(08).                           
          05 WAAPEPYM-SECTOR               PIC X(02).                           
          05 WAAPEPYM-TAM-EMPRE            PIC X(02).                           
          05 WAAPEPYM-COD-MIPYME-REAL      PIC X(02).                           
          05 WAAPEPYM-USO-FUTURO           PIC X(13).                           
      *   ---------------------------------------------- HASTA AQUI:219 00345602
       03 WAAPE685-REG-CPA.                                             00340000
          05 WAAPEPYM-ISEC4110-IMP-DEUDA-T   PIC   9(15)V9(02).         00009400
      *   -- DEUDA TOTAL -------------------------------->HASTA AQUI:23600351418
          05 WAAPEPYM-ISEC4110-IMP-DEUDA-P   PIC   9(15)V9(02).         00009400
      *   -- DEUDA PRESTAMOS (SUBCONJUNTO DEUDA TOTAL) -->HASTA AQUI:25300351418
          05 WAAPEPYM-ISEC4110-IMP-DEUDA-M   PIC   9(15)V9(02).         00009400
      *   -- DEUDA MIPYME (SUBCONJ. DEUDA DE PRESTAMOS) ->HASTA AQUI:27000351418
      ******************************************************************00520000
      *                        F  I  N                                 *00530000
      ******************************************************************00540000
