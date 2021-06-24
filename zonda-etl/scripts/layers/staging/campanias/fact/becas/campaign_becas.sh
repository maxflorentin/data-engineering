#!/bin/bash
#Conectamos al servidor remoto

sshpass -p 36v36H.8 sftp -P 2222 -o 'ProxyCommand /usr/bin/nc --proxy-type socks5 --proxy proxy.ar.bsch:1080 %h %p' svc_crm_argentina@sftp.universia.net << END_OF_SCRIPT


#Obtenemos los datos

get /Report-By-Country-Becas-V2-AR--$1*.csv /aplicaciones/bi/zonda/inbound/campanias/fact/becas
exit
END_OF_SCRIPT


#Cambiamos el nombre
cd /aplicaciones/bi/zonda/inbound/campanias/fact/becas
for f in *csv;
do
mv -- "/aplicaciones/bi/zonda/inbound/campanias/fact/becas/$f" "/aplicaciones/bi/zonda/inbound/campanias/fact/becas/${f%--$1*.csv}_$2.csv"
done