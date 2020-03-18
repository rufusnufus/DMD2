from pymongo import MongoClient
import csv

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']
rent = db['rental']

def find_recent_year(collection):
	year_frequency = {entry['rental_date'].year for entry in collection.find()}
	return(max(year_frequency))

recent_year = find_recent_year(rent)
population = {}
for entry in rent.find():
	if entry['rental_date'].year == recent_year:
		if entry['customer_id'] not in population:
			population[entry['customer_id']] = set()
		film_id = [i['film_id'] for i in db['inventory'].find({'inventory_id': entry['inventory_id']})]
		for film in db['film_category'].find({'film_id': film_id[0]}):
			population[entry['customer_id']].add(film['category_id'])
file = []
for customer in population.keys():
	if len(population[customer]) >= 2:
		file.append(db['customer'].find_one({'customer_id': customer}))

with open('query1.csv', 'w', newline='') as f:
	fieldnames = ['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id',
				 'activebool','create_date', 'last_update', 'active']
	writer = csv.DictWriter(f, fieldnames=fieldnames)

	writer.writeheader()
	for i in file:
		writer.writerow({'customer_id': i['customer_id'], 'store_id': i['store_id'], 'first_name': i['store_id'],
		 'last_name': i['last_name'], 'email': i['email'], 'address_id':i['address_id'],'activebool':i['activebool'],
		 'create_date': i['create_date'], 'last_update': i['last_update'], 'active': i['active']})

con.close()