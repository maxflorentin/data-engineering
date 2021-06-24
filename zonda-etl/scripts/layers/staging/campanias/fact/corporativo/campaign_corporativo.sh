#!/bin/bash

#Conectamos con el servidor remoto

sshpass -p  28Kfufiee3# sftp innovacion28@CSSLCSRHATI0022.UNIX.AACC.CORP<< END_OF_SCRIPT

#Obtenemos los datos

get /data/Campaign_Leads/campaign_$1/*.csv /aplicaciones/bi/zonda/inbound/campanias/fact/corporativo

exit
END_OF_SCRIPT


#Cambiamos el nombre
cd /aplicaciones/bi/zonda/inbound/campanias/fact/corporativo
for f in *csv;
do
mv -- "/aplicaciones/bi/zonda/inbound/campanias/fact/corporativo/$f" "/aplicaciones/bi/zonda/inbound/campanias/fact/corporativo/${f%_$1_pct1_pct100_v01.csv}_$2.csv"
done