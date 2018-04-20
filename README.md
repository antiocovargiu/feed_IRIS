# feed IRIS
Alimentazione di IRIS tramite script python in sostituzione di recupero_RT

# prerequisiti
1. rwmwsgwyd funzionante
2. tabelle di IRIS per il temporeale su postgres
3. pacchetti di python indicati nel codice

# funzionamento (as a service)
```
docker run -it -d --rm -e "PGSQL_USER=<>" -e "PGSQL_PASSWORD=<>" -e "PGSQL_IP=<>" -e "PGSQL_DBNAME=<>" -e "FTP_USER=<>" -e "FTP_PASSWORD=<>" -e "FTP_SERVER=<>" -v "$PWD":/usr/src/myapp -w /usr/src/myapp arpasmr/python_base ./launch_feed.sh _arg1_ _arg2_
```
I due argomenti che vengono passati al launcher sono:
_arg1_ minuto al quale eseguire il recupero (es. 3)
_arg2_ flag per il recupero globale di 24h (R) oppure nessun recupero (_una lettera qualunque_)
