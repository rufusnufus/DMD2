from pymongo import MongoClient
import csv

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']


d = {}
for film_category in db['film_category'].find():
	category_of_film = db['category'].find_one({'category_id':film_category['category_id']})
	film = db['film'].find_one({'film_id': film_category['film_id']})
	inventory_records = db['inventory'].find({'film_id': film_category['film_id']})
	amount_of_rentals = 0
	for inventory_record in inventory_records:
		rentals_of_inventory = db['rental'].count_documents({'inventory_id':inventory_record['inventory_id']})
		amount_of_rentals += rentals_of_inventory
	d[film_category['film_id']] = [film['title'],category_of_film['name'], amount_of_rentals]


with open('query3.csv', 'w', newline='') as f:
	fieldnames = ['id', 'title', 'category', 'amount of rentals']

	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()

	for film_id in d.keys():
		dic = {}
		dic['id'] = film_id
		dic['title'] = d[film_id][0]
		dic['category'] = d[film_id][1]
		dic['amount of rentals'] = d[film_id][2]
		writer.writerow(dic)

con.close()