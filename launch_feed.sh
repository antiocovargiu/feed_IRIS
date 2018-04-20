#!/bin/bash
# script di lancio dell'alimentazione di iris tramite pyhton
#
# il primo argomento è il minuto in cui eseguire il comando ogni 10 minuti
nomescript=${0##*/}
while [ 1 ]
do
   data_corrente=$[ 10#$(date +"%M") % 10 ]
   if [ $data_corrente == $1  ]
   then
      python3 feed_iris.py
   else
     sleep 10
   fi
done
