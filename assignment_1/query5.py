from dijkstar import Graph, find_path
from dijkstar.algorithm import NoPathError
from query2 import calculate_matches, set_actors_and_film_actors
from pymongo import MongoClient
import csv
import time

start_time = time.time()

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']

actors, film_actors = set_actors_and_film_actors(db)
actors_to_actors = calculate_matches(actors, film_actors)
graph = Graph()
for i in actors:
	for j in actors:
		if actors_to_actors[i][j] > 0:
			graph.add_edge(i, j, 1)
			graph.add_edge(j, i, 1)


print("Input the name and surname of the actor(e.q. Penelope Guiness):", end = ' ')
actor = input()
actor_name, actor_surname = actor.split()
try:
	actor_id = db['actor'].find_one({'first_name': actor_name, 'last_name': actor_surname})['actor_id']

	with open('query5.csv', 'w', newline='') as f:
		fieldnames = ['actor id', 'first name', 'last name', 'Bacon number']

		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()

		for alt_actor_id in actors:
			dic = {}
			dic['actor id'] = alt_actor_id
			dic['first name'] = db['actor'].find_one({'actor_id': alt_actor_id})['first_name']
			dic['last name'] = db['actor'].find_one({'actor_id': alt_actor_id})['last_name']
			try:
				bacon_num = find_path(graph, actor_id, alt_actor_id).total_cost
			except NoPathError:
				bacon_num = None
			dic['Bacon number'] = bacon_num
			writer.writerow(dic)
except:
	print('There is no such actor in the database:( We are sorry.')
	print('We hope that this actor will become famous soon and will be added into DB.')

con.close()
print(f'Now you can see results for {actor} in query5.csv file.')

print(f'--- {(time.time() - start_time)} seconds ---')
