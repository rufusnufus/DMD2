from dijkstar import Graph, find_path
from dijkstar.algorithm import NoPathError
from query2 import calculate_matches, set_actors_and_film_actors
from pymongo import MongoClient
import csv
import time

print("Input the name and surname of the actor(e.g. Penelope Guiness):", end = ' ')
actor = input()

start_time = time.time()

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']

#transfer the set of actors and film_actors from 2nd query
actors, film_actors = set_actors_and_film_actors(db)

#calculate via above parameters actors to actors costarring times
actors_to_actors = calculate_matches(actors, film_actors)

#construct the graph from actors_to_actors dict
#if actors played at least one time they know each other, so 
#edge between them equals to 1
graph = Graph()
for i in actors:
	for j in actors:
		if actors_to_actors[i][j] > 0:
			graph.add_edge(i, j, 1)
			graph.add_edge(j, i, 1)

actor_name, actor_surname = actor.split()
try:
	actor_id = db['actor'].find_one({'first_name': actor_name, 'last_name': actor_surname}, {'_id':0, 'actor_id':1})['actor_id']

	with open('query5.csv', 'w', newline='') as f:
		fieldnames = ['first name', 'last name', 'Bacon number']

		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()

		for alt_actor in actors:
			dic = {}
			dic['first name'] = alt_actor['first_name']
			dic['last name'] = alt_actor['last_name']
			#calculate the shortest path to another actor which is exactly Bacon number
			try:
				bacon_num = find_path(graph, actor, alt_actor).total_cost
			#if there is no such path, then Bacon number is None
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
