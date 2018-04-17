import datetime as dt
s=dt.datetime.now()
QueryInsert="INSERT INTO bla bla"+\
'\','+s.strftime("%Y")
print (QueryInsert)
