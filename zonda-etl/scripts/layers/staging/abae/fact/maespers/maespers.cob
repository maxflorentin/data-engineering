      **********************************************************
      *******                                            *******
      ******   tarjetas de debito banelco
      *******  indice por nup                            *******
      **********************************************************
      *     longitud de registro = 048
      ****************************************


       01  :zoecper:-registro.
      * clave principal
        02 :zoecper:-clave.
           03 :zoecper:-nup                 pic x(008).                    00150
           03 :zoecper:-numero-tarjeta      pic x(019).
        02 :zoecper:-datos.
           03 :zoecper:-origen              pic x(001).
              88 :zoecper:-origen-tarjeta       value 't'.
              88 :zoecper:-origen-credencial    value 'c'.
           03 :zoecper:-libre               pic x(020).
