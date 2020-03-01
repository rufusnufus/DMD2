1. Firstly I retrieved restore.sql file to my new created database `dvdrental`:
```console
$ psql -U postgres
postgres=# CREATE DATABASE dvdrental;
postgres=# \q
$ psql -U postgres -d dvdrental -f restore.sql
```
2. I wrote the script that connects to both of databases(Postgres, MongoDB) and table by table copies all entries from postgres to mongodb. There were some issues with compatibility of datatypes:
    * date in postgres was changed to date in mongodb(using datetime.datetime f-n)
    * numeric in postgres was changed to double in mongodb(using conversion float())
    * bytea in postgres was changed to binary data in mongodb(using the method tobytes())

