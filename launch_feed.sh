#!/bin/bash
#
# lo script è all'interno del container e lancia in sequenza lo script R ogni 10 minuti
# lo script fa sempre recuperi corti ogni 20'
#source ~/.bash_profile;
#  s/S = short, ovvero recupero 1h
#  l/L = long, ovvero recupero 24h
#controllo se il container è già stato lanciato: se sì, ritardo l'esecuzione 
# lo inserisco in un ciclo di attesa di 10 minuti, trascorsi i quali non eseguo il comando
nomescript=${0##*/}
while [ 1 ]
do
   data_corrente=$[ 10#$(date +"%M") % 10 ]
   if [ $data_corrente == 3  ]
   then
      echo 'lancio script'
      python3 feed_iris.py
   else
     sleep 10
     echo $data_corrente
   fi
done
