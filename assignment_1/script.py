from info import password
from pymongo import MongoClient
import psycopg2
import datetime

#connecting to mongodb
con_mongo = MongoClient("mongodb://localhost")

#connecting to posgresql database 
con_psql = psycopg2.connect(database="dvdrental", user="postgres",
                       		password=password, host="127.0.0.1", port="5432")

#creating database named dvdrental in mongo
db = con_mongo['dvdrental']

#cursor object is used to execute the commands in psql
cur = con_psql.cursor()

# -- TRANSFER ACTOR TABLE -- #

#get all entries from the table actor
cur.execute("SELECT * FROM actor;")

#load whole output of our query into rows
rows = cur.fetchall()

#creating collection actor in mongo dvdrental db
col = db['actor']

#inserting entries into mongo's actor collection
for row in rows:
	col.insert_one({"actor_id": row[0], "first_name": row[1], "last_name": row[2], 
					"last_update": row[3]})

# --          END         -- #

#the same procedure is applied to all tables

# -- TRANSFER ADDRESS TABLE -- #

cur.execute("SELECT * FROM address;")
rows = cur.fetchall()
col = db['address']
for row in rows:
	d = {"address_id": row[0], "address": row[1],  
		"district": row[3], "city_id": row[4], 
		"phone": row[6], "last_update": row[7]}
	if row[2] is not None:
		d["address2"] = row[2]
	if row[5] is not None:
		d["postal_code"] = row[5]
	col.insert_one(d)

# --           END          -- #

# -- TRANSFER CATEGORY TABLE -- #

cur.execute("SELECT * FROM category;")
rows = cur.fetchall()
col = db['category']
for row in rows:
	col.insert_one({"category_id": row[0], "name": row[1], "last_update": row[2]})

# --           END          -- #

# -- TRANSFER CITY TABLE -- #

cur.execute("SELECT * FROM city;")
rows = cur.fetchall()
col = db['city']
for row in rows:
	col.insert_one({"city_id": row[0], "city": row[1], "country_id": row[2], 
					"last_update": row[3]})

# --           END          -- #

# -- TRANSFER COUNTRY TABLE -- #

cur.execute("SELECT * FROM country;")
rows = cur.fetchall()
col = db['country']
for row in rows:
	col.insert_one({"country_id": row[0], "country": row[1], "last_update": row[2]})

# --           END          -- #

# -- TRANSFER CUSTOMER TABLE -- #

cur.execute("SELECT * FROM customer;")
rows = cur.fetchall()
col = db['customer']
for row in rows:
	date = [int(i) for i in (str(row[7])).split("-")]
	d = {"customer_id": row[0], "store_id": row[1], "first_name": row[2],
		"last_name": row[3], "address_id": row[5], "activebool": row[6],
		"create_date": datetime.datetime(date[0], date[1], date[2])}
	if row[4] is not None:
		d["email"] = row[4]
	if row[8] is not None:
		d["last_update"] = row[8]
	if row[9] is not None:
		d["active"] = row[9]
	col.insert_one(d)

# --           END          -- #

# -- TRANSFER FILM TABLE -- #

cur.execute("SELECT * FROM film;")
rows = cur.fetchall()
col = db['film']
for row in rows:
	d = {"film_id": row[0], "title": row[1], "language_id": row[4],
		"rental_duration": row[5], "rental_rate": float(row[6]), 
		"replacement_cost": float(row[8]), "last_update": row[10], "fulltext": row[12]}
	if row[2] is not None:
		d["description"] = row[2]
	if row[3] is not None:
		d["release_year"] = row[3]
	if row[7] is not None:
		d["length"] = row[7]
	if row[9] is not None:
		d["rating"] = row[9]
	if row[11] is not None:
		d["special_features"] = row[11]
	col.insert_one(d)

# --           END          -- #

# -- TRANSFER FILM_ACTOR TABLE -- #

cur.execute("SELECT * FROM film_actor;")
rows = cur.fetchall()
col = db['film_actor']
for row in rows:
	col.insert_one({"actor_id": row[0], "film_id": row[1], "last_update": row[2]})

# --           END          -- #

# -- TRANSFER FILM_CATEGORY TABLE -- #

cur.execute("SELECT * FROM film_category;")
rows = cur.fetchall()
col = db['film_category']
for row in rows:
	col.insert_one({"film_id": row[0], "category_id": row[1], "last_update": row[2]})

# --           END          -- #

# -- TRANSFER INVENTORY TABLE -- #

cur.execute("SELECT * FROM inventory;")
rows = cur.fetchall()
col = db['inventory']
for row in rows:
	col.insert_one({"inventory_id": row[0], "film_id": row[1], "store_id": row[2], 
					"last_update": row[3]})

# --           END          -- #

# -- TRANSFER LANGUAGE TABLE -- #

cur.execute("SELECT * FROM language;")
rows = cur.fetchall()
col = db['language']
for row in rows:
	col.insert_one({"language_id": row[0], "name": row[1], "last_update": row[2]})

# --           END          -- #

# -- TRANSFER PAYMENT TABLE -- #

cur.execute("SELECT * FROM payment;")
rows = cur.fetchall()
col = db['payment']
for row in rows:
	col.insert_one({"payment_id": row[0], "customer_id": row[1], "staff_id": row[2], 
					"rental_id": row[3], "amount": float(row[4]), "payment_date": row[5]})

# --           END          -- #

# -- TRANSFER RENTAL TABLE -- #

cur.execute("SELECT * FROM rental;")
rows = cur.fetchall()
col = db['rental']
for row in rows:
	d = {"rental_id": row[0], "rental_date": row[1], "inventory_id": row[2], 
		"customer_id": row[3], "staff_id": row[5], "last_update": row[6]}
	if row[4] is not None:
		d["return_date"] = row[4]
	col.insert_one(d)

# --           END          -- #

# -- TRANSFER STAFF TABLE -- #

cur.execute("SELECT * FROM staff;")
rows = cur.fetchall()
col = db['staff']
for row in rows:
	d = {"staff_id": row[0], "first_name": row[1], "last_name": row[2], 
		"address_id": row[3], "store_id": row[5], "active": row[6], "username": row[7],
		"last_update": row[9]}
	if row[4] is not None:
		d["email"] = row[4]
	if row[8] is not None:
		d["password"] = row[8]
	if row[10] is not None:
		d["picture"] = row[10].tobytes()
	col.insert_one(d)

# --           END          -- #

# -- TRANSFER STORE TABLE -- #

cur.execute("SELECT * FROM store;")
rows = cur.fetchall()
col = db['store']
for row in rows:
	col.insert_one({"store_id": row[0], "manager_staff_id": row[1], "address_id": row[2], 
					"last_update": row[3]})

# --           END          -- #

#close connections to dbs
con_mongo.close()
con_psql.close()