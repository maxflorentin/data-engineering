      ******************************************************************
      *                          B S C H                               *
      *                                                                *      *
      * NOMBRE DE LA COPY : ZOECTAR (ex WATBE01)                       *
      * PREFIJO           : ZOECTAR                                    *
      * LONGITUD          : 300 BYTES                                  *
      * CLAVE             : 19 BYTES, POS 1 (NUMERO-TARJETA)           *
      * DESCRIPCION       : MAESTRO DE TARJETAS BANELCO                *
      *                                                                *
      *----------------------------------------------------------------*
      *                   MODIFICACIONES                               *
      *                   --------------                               *
      * USUARIO    FECHA                   DESCRIPCION                 *
      * -------  ----------  ----------------------------------------- *
      * HOV      03/11/2004  Agregar NUP y SUB-PROD de la cta ppal     *
      *----------------------------------------------------------------*
      * B082081  29/11/2007  Se agrega Marca de Vinculacion a Tarjeta  *      *
      *                      de Coordenadas                            *      *
      *----------------------------------------------------------------*
      *          10/06/2015  Se agrega campo CHIP                      *      *
      * ISBAN-0001                                                     *      *
      *----------------------------------------------------------------*
      * MARCA: ISBAN-0002    FECHA: ENERO 2016.                        *
      * AUTOR: TOMAS KRYSZYCHA, AGUSTIN CHAVEZ.                        *
      * MODIF: SE GENERA EL CAMPO :ZOECTAR:-SECUENCIAL Y SE DISMINUYE  *
      *        FILLER                                                  *
      ******************************************************************
      ******************************************************************
      * COMPILAR CON COBOL II
      * EL PREFIJO DEBE SER REEMPLAZADO POR UN COPY REPLACING
      * EJ.: COPY ZOECTAR REPLACING ==:ZOECTAR:== BY ==ZOECTAR==
      *---------------------------------------------------*
            02 :ZOECTAR:-REGISTRO.
      *        Numero de tarjeta completo (CLAVE).               001-019
                     05 :ZOECTAR:-NUMERO-TARJETA      PIC X(19).
      *        Sucursal de tarjeta                               020-023
                     05 :ZOECTAR:-SUCURSAL-ADM        PIC 9(04).
      *        Entidad de la cuenta principal                    024-027
                     05 :ZOECTAR:-ENTIDAD-PPAL        PIC 9(04).
      *        Centro de la cuenta principal                     028-031
                     05 :ZOECTAR:-CENTRO-PPAL         PIC 9(04).
      *        Cuenta Principal                                  032-043
                     05 :ZOECTAR:-CUENTA-PPAL.
                        15 :ZOECTAR:-CUENTA-PPAL-PROD PIC 9(03).
                        15 :ZOECTAR:-CUENTA-PPAL-NRO  PIC 9(09).
      *        Divisa de la cuenta principal                     044-046
                     05 :ZOECTAR:-DIVISA-PPAL         PIC X(03).
      *        Firmante de la cuenta                             047-049
                     05 :ZOECTAR:-FIRMANTE-PPAL       PIC 9(03).
      *        TIPO para Banelco                                 050-051
                     05 :ZOECTAR:-TIPO-CUENTA-BANELCO PIC 9(02).
                         88 :ZOECTAR:-CC-PESOS          VALUE 01.
                         88 :ZOECTAR:-CC-DOLAR          VALUE 02.
                         88 :ZOECTAR:-CA-PESOS          VALUE 11.
                         88 :ZOECTAR:-CA-DOLAR          VALUE 12.
      *        TIPO de tarjeta                                   052-052
                     05 :ZOECTAR:-TIPO-TARJETA        PIC X(01).
                         88 :ZOECTAR:-ADM               VALUE 'A'.
                         88 :ZOECTAR:-DEPOSITO          VALUE 'B'.
                         88 :ZOECTAR:-DEBITO            VALUE 'P'.
      *        Estado  tarjeta                                   053-053
                     05 :ZOECTAR:-ESTADO-TARJETA      PIC 9(01).
                         88 :ZOECTAR:-NO-HABILITADA     VALUE 0.
                         88 :ZOECTAR:-DEBITO-ACTIVA     VALUE 1.
                         88 :ZOECTAR:-ACTIVA            VALUE 1 4.
                         88 :ZOECTAR:-DEPOSITO-ACTIVA   VALUE 4.
                         88 :ZOECTAR:-CERRADA           VALUE 9.
      *        COdigo de LImite - Clase de Tarjeta               054-055
                     05 :ZOECTAR:-COD-LIM             PIC 9(02).
      *        Fecha de Vencimiento (AAAAAMM)                    056-061
                     05 :ZOECTAR:-FEC-VTO             PIC 9(06).
                     05 :ZOECTAR:-FEC-VTO-CARC REDEFINES
                                                :ZOECTAR:-FEC-VTO.
                       07 :ZOECTAR:-ANIO-VTO          PIC 9(04).
                       07 :ZOECTAR:-MES-VTO           PIC 9(02).
      *        Fecha de Alta (AAAAMMDD)                          062-069
                     05 :ZOECTAR:-FEC-ALTA            PIC 9(08).
                     05 :ZOECTAR:-USUARIO-ALTA        PIC X(08).
      *        Fecha de Habilitacion - Es igual a FEC-ALTA       078-085
                     05 :ZOECTAR:-FEC-HABILITACION    PIC 9(08).
      *        Fecha de Ultima modificacion                      086-093
                     05 :ZOECTAR:-FEC-ULT-ACT         PIC 9(08).
                     05 :ZOECTAR:-USUARIO-ACT         PIC X(08).
      *        Fecha envio banelco. Define generacion de novedad 102-109 en CAF
                     05 :ZOECTAR:-FEC-ENVIO-BANELCO   PIC 9(08).
      *        Fecha en que fue procesada la novedad en banelco
                     05 :ZOECTAR:-FEC-ACT-BANELCO     PIC 9(08).

      *****          05 :ZOECTAR:-FILLER-1            PIC X(01).
      *        Estado de la renovacion por cambio de marca
                05 :ZOECTAR:-ESTADO-RENOV            PIC X(01).
      *        ESTADO ESPACIO O CERO: SIN ACCION DE RENOVACION
                      88 :ZOECTAR:-ESTADO-RENOV-SAC VALUE ' ' '0'.
      *        ESTADO S: SELECCIONADA PARA RENOVAR
                      88 :ZOECTAR:-ESTADO-RENOV-SEL VALUE 'S'.
      *        ESTADO P: PEDIDO DE RENOVACION
                      88 :ZOECTAR:-ESTADO-RENOV-PED VALUE 'P'.
      *        ESTADO E: RENOVADA SIN USO
                      88 :ZOECTAR:-ESTADO-RENOV-SIN VALUE 'E'.
      *        ESTADO R: REINTENTO POR ROBO
                      88 :ZOECTAR:-ESTADO-RENOV-ROB VALUE 'R'.
      *        ESTADO X: RENOVACION RECHAZADA
                      88 :ZOECTAR:-ESTADO-RENOV-RCH VALUE 'X'.
      *        ESTADO U: RENOVADA EN USO
                      88 :ZOECTAR:-ESTADO-RENOV-USO VALUE 'U'.
      *        ESTADO D: RENOVACION DESTRUIDA
                      88 :ZOECTAR:-ESTADO-RENOV-DES VALUE 'D'.
      *        ESTADOS QUE PIDEN RENOVACION
                      88 :ZOECTAR:-ESTADO-RENOV-PID VALUE
                                            'P' 'R' 'X' .
                     05 :ZOECTAR:-MARCA-PIN           PIC X(01).
                      88 :ZOECTAR:-GENERA-PIN           VALUE 'S'.
      *        Fecha de ultimo embozado
                     05 :ZOECTAR:-FEC-EMBZ            PIC 9(08).
      *        Fecha de Blanqueo de PIN
                     05 :ZOECTAR:-FEC-PIN             PIC 9(08).
                     05 :ZOECTAR:-USUARIO-PIN         PIC X(08).
      *        Fecha para cambio de sucursal
                     05 :ZOECTAR:-FEC-CBIO-SUC        PIC 9(08).
      *        Cantidad de cuentas relacionadas
                     05 :ZOECTAR:-CAN-CUENTAS         PIC 9(02).
                     05 :ZOECTAR:-MARCA-TAR           PIC X(01).
                      88 :ZOECTAR:-ELECTRON             VALUE '*'.
                      88 :ZOECTAR:-BOR-VIEJA            VALUE 'V'.
                      88 :ZOECTAR:-BOR-NUEVA            VALUE 'N'.
      *        Old-PAN
                     05 :ZOECTAR:-NRO-TAR-ANT         PIC X(19).
                     05 :ZOECTAR:-COD-REEM            PIC X(01).
                        88 :ZOECTAR:-NO-REEMITIR        VALUE '0'.
                        88 :ZOECTAR:-REEMITIR-IGUAL-VTO VALUE '1'.
                        88 :ZOECTAR:-REEMITIR-NUEVO-VTO VALUE '2'.
      *        Codigo de distribucion.
                     05 :ZOECTAR:-COD-DIST            PIC X(02).
      *        Modelo de plastico (CAFTIPO)
                     05 :ZOECTAR:-TIPO-PLAS           PIC X(03).
                     05 :ZOECTAR:-TIPO-PLAS-ORIGI     PIC X(03).
                     05 :ZOECTAR:-COD-COMERCIAL       PIC X(01).
                        88 :ZOECTAR:-ES-INDIVIDUO       VALUE 'I'.
                        88 :ZOECTAR:-ES-UNIVERSIDAD     VALUE 'U'.
                        88 :ZOECTAR:-ES-COMERCIO        VALUE 'C'.
                        88 :ZOECTAR:-ES-EMPRESA         VALUE 'E'.
                        88 :ZOECTAR:-ES-UNIPERSONAL     VALUE 'P'.
      *              05 :ZOECTAR:-IDE-COMERCIAL       PIC X(23).
                     05 :ZOECTAR:-COD-EMISION         PIC X(03).
                     05 :ZOECTAR:-NOMBRE-FOTO         PIC X(10).
                     05 :ZOECTAR:-TRACKII             PIC X(10).
      *        TEXTO DE CUARTA LINEA
                     05 :ZOECTAR:-CUARTA-LINEA        PIC X(24).
                     05 :ZOECTAR:-MOT-EMISION         PIC X(01).
                        88 :ZOECTAR:-EMISION-ALTA       VALUE '1'.
                        88 :ZOECTAR:-EMISION-RENOVACION VALUE '2'.
                        88 :ZOECTAR:-EMISION-REEMBOZADO VALUE '3'.
                        88 :ZOECTAR:-EMISION-RETENCION  VALUE '4'.
      *        Error en la actualizacion en Banelco
                     05 :ZOECTAR:-RETORNO-BANELCO     PIC X(05).
      *        NUP del firmante de la cuenta principal
                     05 :ZOECTAR:-NUP                 PIC X(08).
      *        Subproducto de la cuenta ppal
                     05 :ZOECTAR:-PPAL-SUBPROD        PIC X(04).
                     05 :ZOECTAR:-TRACKI.
                       07 :ZOECTAR:-TRACKI-DISC1        PIC X(08).
                       07 :ZOECTAR:-TRACKI-DISC2        PIC X(02).
                       07 :ZOECTAR:-TRACKI-DISC3        PIC X(06).
                     05 :ZOECTAR:-FEC-UTILES          PIC X(08).
                     05 :ZOECTAR:-COD-UTILES          PIC X(01).
      *        Fecha de cambio de estado renovacion por cambio de marca
      *        (estado-renov)
                     05 :ZOECTAR:-FEC-ESTADO-RENOV    PIC X(08).
      * B082081 - 29/11/2007 - INICIO
      *        Marca de Vinculacion de Tarjeta de Coordenadas
                     05 :ZOECTAR:-TARJETA-COORD       PIC 9(01).
      * B011198 - 06/03/2008
      *        Marca de Inhabilitados operar en cambios
                     05 :ZOECTAR:-INHABIL-CBIO        PIC X(01).
MOD001               05 :ZOECTAR:-COD-DESTINO         PIC X(08).
      * B024681 - 22/07/2011
      *        ESTADO DE CLAVE OTP
                     05 :ZOECTAR:-ADHESION-OTP        PIC X(01).
      *                   'A'                   ACTIVO
      *                   'B'                   BLOQUEDO
id4454* CANAL / PERMISIONARIA
id4454               05 :ZOECTAR:-CANAL              PIC X(02).
id4454               05 :ZOECTAR:-PERMISIONARIA      PIC X(02).
      *ISBAN-0001-I
                     05 :ZOECTAR:-CHIP               PIC X(01).
      *ISBAN-0001-F

      *ISBAN-0002-I
      *        CODIGO DE SECUENCIAL
                     05 :ZOECTAR:-SECUENCIAL          PIC X(03).
      *ISBAN-0002-F
      *        PARA USO FUTURO
      *              05 :ZOECTAR:-FILLER              PIC X(27).
      *              05 :ZOECTAR:-FILLER              PIC X(19).
080306*              05 :ZOECTAR:-FILLER              PIC X(18).
110722*              05 :ZOECTAR:-FILLER              PIC X(17).
110722*              05 :ZOECTAR:-FILLER              PIC X(08).
      *ISBAN-0001-I
id4454*              05 :ZOECTAR:-FILLER              PIC X(04).
      *ISBAN-0002-I
id4454*              05 :ZOECTAR:-FILLER              PIC X(03).
      *ISBAN-0002-F
      *
      *ISBAN-0001-F
      * B082081 - 29/11/2007 - FIN
