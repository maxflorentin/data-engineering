
      ***************************************************************** 00010000
      *   COPY: PEEC867C - Archivo de Traspaso de Contratos en          00020000
      *         formato display                                         00020000
      ***************************************************************** 00030000
      *   PETIPREG --> Tipo de registro                                 00060000
      *   PECDGENT --> Entidad del traspaso                             00070000
      *   PENUMTRA --> Numero del traspaso                              00080100
      *   DATOS COMUNES --> Grupo de datos comunes                      00090000
      *   PETIPTRA  --> Tipo de traspaso                                00100000
      *   PEFASTRA --> Fase del Trasapaso                               00110000
      *   PEENTORI --> Entidad origen del traspaso                      00120000
      *   PEOFIORI --> Oficina de origen del traspaso                   00130000
      *   PEOFIDES --> Oficina destino del traspaso                     00140000
      *   CONTRATO ORIGEN -->                                           00150000
      *   PECDGENT --> Codigo de la ENTIDAD                             00160000
      *   PENUMCON --> Numero de Contrato de CCC                        00170000
      *   PECODOFI --> Codigo de Oficina/Sucursal de CCC                00180000
      *   PECODENT --> Codigo de Entidad CCC                            00190000
      *   PECODPRO --> Codigo de Producto                               00200000
      *   PECODSUB --> Codigo de Subproducto                            00210000
      *   PECODMON --> Codigo de Moneda                                 00220000
      *   PESUCOPE --> Sucursal de Operacion                            00230000
      *   PEOFIOPE --> Oficial de Operacion                             00240000
      *   PECANVEN --> Canal de Venta                                   00250000
      *   PEOFIVEN --> Oficial de Venta                                 00260000
      *   PEOFICOM --> Oficial Comercial                                00270000
      *   PESIGNS1 --> Signo del saldo anterior                                 
      *   PESDOANT --> Saldo al dia anterior                            00280000
      *   PESIGNS2 --> Signo del saldo promedio del mes anterior                
      *   PESDOPRO --> Saldo promedio del mes anterior                  00290000
      *   PEFECINI --> Fecha de Inicio de la Operacion                  00300000
      *   PEFECTER --> Fecha de Termino de la Operacion                 00310000
      *   PEESTOPE --> Estado de la Operacion                           00320000
      *   PEFECEST --> Fecha de Estado                                  00330000
      *   PEMOTEST --> Motivo del Estado                                00340000
      *   PESUBEST --> Subestado                                        00350000
      *   PERELBAN --> Numero de Relacion Bancaria                      00360000
      *   PEPAQPER --> CCC del paquete al que pertenece                 00370000
      *   CONTRATO DESTINO -->                                          00380000
      *   PECDGENTD --> Codigo de la ENTIDAD                            00390000
      *   PENUMCOND --> Numero de Contrato de CCC                       00400000
      *   PECODOFID --> Codigo de Oficina/Sucursal de CCC               00410000
      *   PECODENTD --> Codigo de Entidad CCC                           00420000
      *   PECODPROD --> Codigo de Producto                              00430000
      *   PECODSUBD --> Codigo de Subproducto                           00440000
      *   PECODMOND --> Codigo de Moneda                                00450000
      *   PESUCOPED --> Sucursal de Operacion                           00460000
      *   PEOFIOPED --> Oficial de Operacion                            00470000
      *   PECANVEND --> Canal de Venta                                  00480000
      *   PEOFIVEND --> Oficial de Venta                                00490000
      *   PEOFICOMD --> Oficial Comercial                               00500000
      *   PESIGNS1D --> Signo del saldo ant destino                    *        
      *   PESDOANTD --> Saldo al dia anterior                           00510000
      *   PESIGNS2D --> Signo del saldo pro destino                    *        
      *   PESDOPROD --> Saldo promedio del mes anterior                 00520000
      *   PEFECINID --> Fecha de Inicio de la Operacion                 00530000
      *   PEFECTERD --> Fecha de Termino de la Operacion                00540000
      *   PEESTOPED --> Estado de la Operacion                          00550000
      *   PEFECESTD --> Fecha de Estado                                 00560000
      *   PEMOTESTD --> Motivo del Estado                               00570000
      *   PESUBESTD --> Subestado                                       00580000
      *   PERELBAND --> Numero de Relacion Bancaria                     00590000
      *   PEPAQPERD --> CCC del paquete al que pertenece                00600000
      *   PEINDTRA --> Indicador de traspaso de contratos               00610000
      *     BAJA         VALUE 'BA' - Cierra y No traspasa              00610100
      *     TRASPASO     VALUE 'SR' - Cierra Origen y Abre destino      00610200
      *     ALTA         VALUE 'AL' - Alta nueva de contrato destino    00610300
      *     SUC-OPERA    VALUE 'SO' - Traspaso Sucursal de Operacion    00610400
      *     CTA-INTERNA  VALUE 'IN' - Cuenta Interna                    00610500
      *   PEUSUALT-ORIG --> Usuario de Alta del Contrato Origen         00620000
      *   PEFECALT-ORIG --> Fecha de Alta del Contrato Origen           00630000
      *   DATOS DE PERSONA -->                                          00630010
      *   PENUMPER --> Numero unico de Persona                          00630100
      *   PEFORTRA --> Forma de traspaso 1 = 1 a 1 / 2 = 1 a N          00630200
      *   PENUMLOT --> Numero del Lote (Agrupa Sucursal Origen)         00080000
      ******************************************************************00640000

      ******************************************************************        
      * COBOL DECLARATION FOR TABLE PEEC867C                            *        
      ****************************************************************** 
           02  PEEC867C.                                                00650000
               05  PEEC867C-DATOS-ENTRADA.                              00660000
                   10  PEEC867C-PETIPREG        PIC X(1).               00670000
                       88 PETIPREG-CABECERA     VALUE '1'.              00670100
                       88 PETIPREG-DETALLE      VALUE '2'.              00670200
                   10  PEEC867C-PECDGENT        PIC X(4).               00680000
                   10  PEEC867C-PENUMTRA        PIC X(8).               00690100
               05  PEEC867C-DATOS-COMUNES.                              00700000
                   10  PEEC867C-PETIPTRA        PIC X(3).               00710000
                       88 PETIPTRA-TOTAL        VALUE 'TOT'.            00710100
                       88 PETIPTRA-CONTRATO     VALUE 'CON'.            00710200
                       88 PETIPTRA-PERSONA      VALUE 'PER'.            00710300
                   10  PEEC867C-PEFASTRA        PIC X(3).               00720000
                       88 PEFASTRA-INF-PERSONA  VALUE 'IIP'.            00720001
                       88 PEFASTRA-INF-CONTRATO VALUE 'IIC'.            00720002
                       88 PEFASTRA-EJE-PERSONA  VALUE 'EEP'.            00720010
                       88 PEFASTRA-EJE-CONTRATO VALUE 'EEC'.            00720100
                   10  PEEC867C-PEENTORI        PIC X(4).               00730000
                   10  PEEC867C-PEOFIORI        PIC X(4).               00740000
                   10  PEEC867C-PEOFIDES        PIC X(4).               00750000
               05  PEEC867C-CONTRATO-ORIGEN.                            00760000
                   10  PEEC867C-PECDGENT        PIC X(4).               00770000
                   10  PEEC867C-CCC.                                    00780000
                       15 PEEC867C-PENUMCON     PIC X(12).              00790000
                       15 PEEC867C-PECODOFI     PIC X(4).               00800000
                       15 PEEC867C-PECODENT     PIC X(4).               00810000
                   10  PEEC867C-PECODPRO        PIC X(2).               00820000
                   10  PEEC867C-PECODSUB        PIC X(4).               00830000
                   10  PEEC867C-PECODMON        PIC X(3).               00840000
                   10  PEEC867C-PESUCOPE        PIC X(4).               00850000
                   10  PEEC867C-PEOFIOPE        PIC X(4).               00860000
                   10  PEEC867C-PECANVEN        PIC X(3).               00870000
                   10  PEEC867C-PEOFIVEN        PIC X(4).               00880000
                   10  PEEC867C-PEOFICOM        PIC X(4).               00890000
                   10  PEEC867C-PESIGNS1        PIC X(1).                       
                   10  PEEC867C-PESDOANT        PIC 9(16)V9(2).         00900000
                   10  PEEC867C-PESIGNS2        PIC X(1).                       
                   10  PEEC867C-PESDOPRO        PIC 9(16)V9(2).         00910000
                   10  PEEC867C-PEFECINI        PIC X(10).              00920000
                   10  PEEC867C-PEFECTER        PIC X(10).              00930000
                   10  PEEC867C-PEESTOPE        PIC X(1).               00940000
                   10  PEEC867C-PEFECEST        PIC X(10).              00950000
                   10  PEEC867C-PEMOTEST        PIC X(2).               00960000
                   10  PEEC867C-PESUBEST        PIC X(2).               00970000
                   10  PEEC867C-PERELBAN        PIC X(8).               00980000
                   10  PEEC867C-PEPAQPER        PIC X(20).              00990000
               05  PEEC867C-CONTRATO-DESTINO.                           01000000
                   10  PEEC867C-PECDGENTD       PIC X(4).               01010000
                   10  PEEC867C-PENUMCOND       PIC X(12).              01020000
                   10  PEEC867C-PECODOFID       PIC X(4).               01030000
                   10  PEEC867C-PECODENTD       PIC X(4).               01040000
                   10  PEEC867C-PECODPROD       PIC X(2).               01050000
                   10  PEEC867C-PECODSUBD       PIC X(4).               01060000
                   10  PEEC867C-PECODMOND       PIC X(3).               01070000
                   10  PEEC867C-PESUCOPED       PIC X(4).               01080000
                   10  PEEC867C-PEOFIOPED       PIC X(4).               01090000
                   10  PEEC867C-PECANVEND       PIC X(3).               01100000
                   10  PEEC867C-PEOFIVEND       PIC X(4).               01110000
                   10  PEEC867C-PEOFICOMD       PIC X(4).               01120000
                   10  PEEC867C-PESIGNS1D       PIC X(1).                       
                   10  PEEC867C-PESDOANTD       PIC 9(16)V9(2).         01130000
                   10  PEEC867C-PESIGNS2D       PIC X(1).                       
                   10  PEEC867C-PESDOPROD       PIC 9(16)V9(2).         01140000
                   10  PEEC867C-PEFECINID       PIC X(10).              01150000
                   10  PEEC867C-PEFECTERD       PIC X(10).              01160000
                   10  PEEC867C-PEESTOPED       PIC X(1).               01170000
                   10  PEEC867C-PEFECESTD       PIC X(10).              01180000
                   10  PEEC867C-PEMOTESTD       PIC X(2).               01190000
                   10  PEEC867C-PESUBESTD       PIC X(2).               01200000
                   10  PEEC867C-PERELBAND       PIC X(8).               01210000
                   10  PEEC867C-PEPAQPERD       PIC X(20).              01220000
               05  PEEC867C-PEINDTRA            PIC X(2).               01230000
                       88 PEINDTRA-BAJA         VALUE 'BA'.             01230100
                       88 PEINDTRA-TRASPASO     VALUE 'SR'.             01230200
                       88 PEINDTRA-ALTA         VALUE 'AL'.             01230300
                       88 PEINDTRA-SUC-OPERA    VALUE 'SO'.             01230400
                       88 PEINDTRA-CTA-INTERNA  VALUE 'IN'.             01230500
               05  PEEC867C-PEUSUALT-ORIG       PIC X(08).              01240000
               05  PEEC867C-PEFECALT-ORIG       PIC X(10).              01250000
               05  PEEC867C-PENUMPER            PIC X(08).              01280000
               05  PEEC867C-PEFORTRA            PIC X(01).              01290001
                   88 TRASPASO-1A1              VALUE '1'.              01290002
                   88 TRASPASO-1AN              VALUE '2'.              01290003
                   88 TRASPASO-1AN-CSEG         VALUE '3'.              01290003
               05  PEEC867C-PENUMLOT            PIC X(8).               00690000
               05  PEEC867C-FILLER              PIC X(14).              01300000