#!/bin/bash
# script di lancio dell'alimentazione di iris tramite pyhton
#
# il primo argomento è il minuto in cui eseguire il comando ogni 10 minuti
# il secondo argomento è in caso di recupero: mettere comunque una lettera diversa da 'R'
# in caso di recupero esegue lo script e si ferma per un'ora
nomescript=${0##*/}
while [ 1 ]
do
   data_corrente=$[ 10#$(date +"%M") % 10 ]
   if [ $data_corrente == $1 ] && [ $2 != "R" ]
   then
      python3 feed_iris.py
   else
     sleep 30
   fi
   if [ $2 == "R"]
   then
     python3 feed_iris_recupero.py
     sleep 3600
   fi
done
