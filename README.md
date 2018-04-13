# feed IRIS
Alimentazione di IRIS tramite script python in sostituzione di recupero_RT

# prerequisiti
1. rwmwsgwyd funzionante
2. tabelle di IRIS per il temporeale su postgres
3. pacchetti di python indicati nel codice

# funzionamento
```
docker run -it --rm -e "PGSQL_USER=<>" -e "PGSQL_PASSWORD=<>" -e "PGSQL_IP=<>" -e "PGSQL_DBNAME=<>" -e "FTP_USER=<>" -e "FTP_PASSWORD=<>" -e "FTP_SERVER=<>" -v "$PWD":/usr/src/myapp -w /usr/src/myapp arpasmr/python_base python StatoSensori.py
```
