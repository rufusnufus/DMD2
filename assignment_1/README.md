TASK: move the database of your company from an RDBMS to MongoDB

1. Firstly I retrieved restore.sql from postgree directory file to my new created database `dvdrental`:
```console
$ psql -U postgres
postgres=# CREATE DATABASE dvdrental;
postgres=# \q
$ psql -U postgres -d dvdrental -f restore.sql
```
2. ####Starting servers####
#####Mongo#####
For running mongodb server:
```console
$ sudo mongod
```
In another terminal window run ```$ mongo``` for openning mongo console\
```show dbs``` for showing all dbs in mongo\
#####PostgreSQL#####
For running psql server:
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
3. I wrote the script.py that connects to both of databases(Postgres, MongoDB) and table by table copies all entries from postgres to mongodb. There were some issues with compatibility of datatypes:
    * date in postgres was changed to date in mongodb(using datetime.datetime f-n)
    * numeric in postgres was changed to double in mongodb(using conversion float())
    * bytea in postgres was changed to binary data in mongodb(using the method tobytes())
4. Adjustments:
   * In SQL DB there were fields in the tables that could be empty(None), so I didn't add those fields if they were empty according to flexibility of documents in mongo.
   * I don't see any profit in merging the tables, because they constructed logically with the aim of no memory consumption and data anomolies at all. One thing that we could do in moving the tables, we could optimize the tables with composite primary keys. We have 2 such tables: film_category and film_actor. I did optimization for film_category table(e.g. One film can have multiple categories, in SQL we could not store several categories for 1 film in the cell, but in mongo document we can afford it.) As a consequence, we do not need category table, so we can merge it with film_category table via category_id. I did not optimize film_actor table, because for 2nd query I need to look for films in which particular actor has played as well as for actors acted in particular film.
   * One more adjustment was done for speeding up the 4th query: in inventory table for each film_id was added category field.
