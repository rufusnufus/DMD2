from dijkstar import Graph, find_path
from dijkstar.algorithm import NoPathError
from query2 import actors_to_actors, actors
from pymongo import MongoClient
import csv

#connecting to mongodb
con = MongoClient("mongodb://localhost")

#connecting to database named dvdrental in mongo
db = con['dvdrental']

graph = Graph()
for i in actors:
	for j in actors:
		if actors_to_actors[i][j] > 0:
			graph.add_edge(i, j, 1)
			graph.add_edge(j, i, 1)


actor = "Penelope Guiness"
actor_name, actor_surname = actor.split()
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

con.close()