from pymongo import MongoClient
import csv
import time

start_time = time.time()

#connecting to mongodb
con = MongoClient('mongodb://localhost')

#connecting to database named dvdrental in mongo
db = con['dvdrental']


print('Input the name and surname of the customer(e.q. Ivan Ivanov):', end = ' ')
person = input()
print('How many films you want to see?', end = ' ')
need = int(input())
customer_first_name, customer_last_name = person.split()
print('Wait a little bit:) Our system analyzes customer\'s preferences to give you the best result!')
print('You have time to drink a tea with some coockie)')
customer_id = db['customer'].find_one({'first_name': customer_first_name, 
							'last_name': customer_last_name})['customer_id']

customers_id = db['customer'].find({},{'_id':0, 'customer_id':1})

customers_films = {}
for cid in customers_id:
	curr_id = cid['customer_id']
	customers_films[curr_id] = []
	inventory_ids = db['rental'].find({'customer_id': curr_id}, {'_id':0,'inventory_id':1})
	for iid in inventory_ids:
		film_ids = db['inventory'].find({'inventory_id': iid['inventory_id']}, {'_id':0, 'film_id':1, 'category':1})
		for fid in film_ids:
			customers_films[curr_id].append([fid['film_id'], fid['category']])

customer_categories = set()
for film in customers_films[customer_id]:
	customer_categories.add(film[-1])

cust_ids = set(customers_films.keys())

film_matches = dict(zip(cust_ids, [0].copy()*len(cust_ids)))
film_recommendations = {}
max_matches = 0
for f in cust_ids:
	if f == customer_id:
		continue
	for film in customers_films[f]:
		if film in customers_films[customer_id]:
			film_matches[f] += 1
			film_recommendations[f] = []
		elif f in film_recommendations and film[-1] in customer_categories:
			film_recommendations[f].append(film)
		elif film[-1] in customer_categories:
			film_recommendations[f]=[film]
		if film_matches[f] > max_matches:
			max_matches = film_matches[f]

appropriate_match = max_matches
recommendations = []

while(appropriate_match > 0 and len(recommendations) < need):
	for f in cust_ids:
		if film_matches[f] >= appropriate_match and f != customer_id:
			for film in film_recommendations[f]:
				if len(recommendations) >= need:
					break
				if film in recommendations:
					break
				film[-1] = int(appropriate_match/max_matches*100)
				recommendations.append(film)
				
	appropriate_match //= 1.5


with open(f'{person}.csv', 'w', newline='') as f:
	fieldnames = ['film', 'metric']

	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()

	for film in recommendations:
		dic = {}
		dic['film'] = db['film'].find_one({'film_id': film[0]})['title']
		dic['metric'] = f'{film[-1]}%'
		writer.writerow(dic)
print(f'Now you can see the list of film recommendations of the customer in {person}.csv file.')
print('Have a good time!')
con.close()

print(f'--- {(time.time() - start_time)} seconds ---')
