from pymongo import MongoClient
import csv
import time
import heapq

print('Input name and surname of the customer(e.g. Ruth Martinez):', end = ' ')
person = input()
print('How many films you want to see?(e.g. 10)', end = ' ')
need = int(input())

start_time = time.time()

#connecting to mongodb
con = MongoClient('mongodb://localhost')

#connecting to database named dvdrental in mongo
db = con['dvdrental']

customer_first_name, customer_last_name = person.split()
print('Wait a little bit:) Our system analyzes customer\'s preferences to give you the best result!')
print('You have time to drink a tea with some coockies)')

#retrieves an id of given customer
customer_id = db['customer'].find_one({'first_name': customer_first_name, 
							'last_name': customer_last_name},{'_id':0, 'customer_id':1})['customer_id']

#retrieves all customers' ids
customers_id = db['customer'].find({},{'_id':0, 'customer_id':1})

#maps customer_id to films that he/she has watched and saves into customers_films
customers_films = {}
for cid in customers_id:
	curr_id = cid['customer_id']
	customers_films[curr_id] = []
	inventory_ids = db['rental'].find({'customer_id': curr_id}, {'_id':0,'inventory_id':1})
	for iid in inventory_ids:
		film_ids = db['inventory'].find({'inventory_id': iid['inventory_id']}, {'_id':0, 'film_id':1, 'category':1})
		for fid in film_ids:
			customers_films[curr_id].append([fid['film_id'], fid['category']])

#finds all categories of films watched by our target customer
customer_categories = set()
for film in customers_films[customer_id]:
	customer_categories.add(film[-1])

#set of all customers's ids
cust_ids = set(customers_films.keys())

#maps customer's id to the amount of same films watched with our target customer  
film_matches = dict(zip(cust_ids, [0].copy()*len(cust_ids)))

#maps customer's id and films that he/she watched, but our target customer did not
#also all these films must have one of the categories that our target customer watched.
film_recommendations = {}

for f in cust_ids:
	for film in customers_films[f]:
		if film in customers_films[customer_id]:
			film_matches[f] += 1
			film_recommendations[f] = []
		elif f in film_recommendations and film[-1] in customer_categories:
			film_recommendations[f].append(film)
		elif film[-1] in customer_categories:
			film_recommendations[f]=[film]

#priority queue with film recomendations to our target customer
recommendations = []

#pushes each film from film_recommendations with metric of (film_matches of current customer with our customer)
#divided by amount of films seen by our customer multiplied by 100 to get percentage
#here we negate the metric with aim of highest metric will be popped first, because heapq is realised via min heap
for f in cust_ids:
	if f != customer_id:
		for film in film_recommendations[f]:
			heapq.heappush(recommendations, (-int(film_matches[f]/film_matches[customer_id]*100), film[0]))

#write film recommendations to csv file named by our customer
with open(f'{person}.csv', 'w', newline='') as f:
	fieldnames = ['film', 'metric']

	writer = csv.DictWriter(f, fieldnames=fieldnames)
	writer.writeheader()

	for i in range(0, need):
		dic = {}
		film = heapq.heappop(recommendations)
		dic['film'] = db['film'].find_one({'film_id': film[1]},{'_id':0, 'title':1})['title']
		dic['metric'] = f'{-film[0]}%'
		writer.writerow(dic)

con.close()

print(f'Now you can see the list of film recommendations of the customer in {person}.csv file.')
print('Have a good time!')
print(f'--- {(time.time() - start_time)} seconds ---')
