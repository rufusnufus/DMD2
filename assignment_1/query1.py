from pymongo import MongoClient
import csv
import time

start_time = time.time()

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']

def find_recent_year(collection):
	year_frequency = set([entry['rental_date'].year for entry in collection.find()])
	return(max(year_frequency))

recent_year = find_recent_year(db['rental'])

#dict that maps each customer id to set of categories of films that he has seen during current year
population = {}

#for each document in rental collection that of the current year, adds customer's id to population as a key
for entry in db['rental'].find({}, {'_id':0, 'customer_id':1, 'rental_date':1, 'inventory_id':1}):
	if entry['rental_date'].year == recent_year:
		if entry['customer_id'] not in population:
			population[entry['customer_id']] = set()
		#retrieves film_id from inventory collection using inventory_id from entry(current document from rental collection)
		film_id = db['inventory'].find_one({'inventory_id': entry['inventory_id']}, {'_id':0, 'film_id':1})['film_id']
		#finds all categories for film_id 
		for film in db['film_category'].find_one({'film_id': film_id},{'_id':0, 'categories':1})['categories']:
			#adds categories to set of categories of customer id 
			population[entry['customer_id']].add(film['category'])

#list of customers that rented movies of at least two different categories during the current year
file = []
for customer in population.keys():
	#checking whether customer rented 2 or more categories or not
	if len(population[customer]) >= 2:
		#adds to the list
		file.append(db['customer'].find_one({'customer_id': customer}))

#writes everything about customer to the output file.
with open('query1.csv', 'w', newline='') as f:
	fieldnames = ['customer_id', 'store_id', 'first_name', 'last_name', 'email', 'address_id',
				 'activebool','create_date', 'last_update', 'active']
	writer = csv.DictWriter(f, fieldnames=fieldnames)

	writer.writeheader()
	for i in file:
		writer.writerow({'customer_id': i['customer_id'], 'store_id': i['store_id'], 'first_name': i['first_name'],
		 'last_name': i['last_name'], 'email': i['email'], 'address_id':i['address_id'],'activebool':i['activebool'],
		 'create_date': i['create_date'], 'last_update': i['last_update'], 'active': i['active']})

#closes connection to mongodb
con.close()

print(f'Now you can see the results in query1.csv file.')
print(f'--- {(time.time() - start_time)} seconds ---')
