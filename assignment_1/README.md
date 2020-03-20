TASK: move the database of your company from an RDBMS to MongoDB

1. Firstly I retrieved restore.sql from postgree directory file to my new created database `dvdrental`:
```console
$ psql -U postgres
postgres=# CREATE DATABASE dvdrental;
postgres=# \q
$ psql -U postgres -d dvdrental -f restore.sql
```
2. I wrote the script.py(info.py must contain the variable password with your password from postgres user) that connects to both of databases(Postgres, MongoDB) and table by table copies all entries from postgres to mongodb. There were some issues with compatibility of datatypes:
    * date in postgres was changed to date in mongodb(using datetime.datetime f-n)
    * numeric in postgres was changed to double in mongodb(using conversion float())
    * bytea in postgres was changed to binary data in mongodb(using the method tobytes())
  
\
P.S. for running mongodb server:
```console
$ sudo mongod
```
In another terminal window run ```$ mongo``` for openning mongo console\
```show dbs``` for showing all dbs in mongo\
P.P.S for running psql server:
```console
$ pg_ctl -D /usr/local/var/postgres start
```
Running the psql console
```console
$ psql -U postgres
```
Shut down the server
```console
$ pg_ctl -D /usr/local/var/postgres stop
```
