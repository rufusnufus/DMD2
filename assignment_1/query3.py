from pymongo import MongoClient
import csv
import time

start_time = time.time()

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']

#dictionary that will have film_id as key and [film's title, it's categories, amount of rentals] as value
d = {}

#for each film document in film_category table retrieves it's id and list of categories
for film_category in db['film_category'].find({}, {'_id':0, 'film_id':1, 'categories':1}):
	
	#finds film's title from film collection
	film = db['film'].find_one({'film_id': film_category['film_id']}, {'_id':0, 'title':1})

	#retrieves all documents from inventory collection associated with current film
	inventory_records = db['inventory'].find({'film_id': film_category['film_id']}, {'_id':0, 'inventory_id':1})
	amount_of_rentals = 0

	#for each such document from inventory
	for inventory_record in inventory_records:
		#count amount of documents in rental collection associated with this inventory_record
		rentals_of_inventory = db['rental'].count_documents({'inventory_id':inventory_record['inventory_id']})
		
		#amount of such documents shows how many times customer rented this inventory of current film
		#so we add this amount to amount_of_rentals of current film
		amount_of_rentals += rentals_of_inventory

	#add to dictionary film_id: [film's title, set of categories og the film, amount of rentals of the film]
	d[film_category['film_id']] = [film['title'],frozenset([category['category'] for category in film_category['categories']]), amount_of_rentals]

#write dictionary d to query3.csv
with open('query3.csv', 'w', newline='') as f:
	fieldnames = ['id', 'title', 'category', 'amount of rentals']
	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()

	for film_id in d.keys():
		dic = {}
		dic['id'] = film_id
		dic['title'] = d[film_id][0]
		dic['category'] = ', '.join(d[film_id][1])
		dic['amount of rentals'] = d[film_id][-1]
		writer.writerow(dic)

#close the connection to mongodb
con.close()

print(f'Now you can see the results in query3.csv file.')
print(f'--- {(time.time() - start_time)} seconds ---')
