from pymongo import MongoClient
import csv
import time

#finds all actors(id) and assigns them to set actors 
#maps each film_id and actors played in it, returns as film_actors
def set_actors_and_film_actors(db):
	film_actors = {}
	actors = set()
	for film in db['film_actor'].find():
		actors.add(film['actor_id'])
		if film['film_id'] not in film_actors:
			film_actors[film['film_id']] = [film['actor_id']]
		else:
			film_actors[film['film_id']].append(film['actor_id'])

	return actors, film_actors

#calculates amount of costarring of each actor with all other actors using film_actors dictionary
#actors_to_actors - dict, where key is actor's id and value is another dict with all actors'id as keys 
#and amount of co-starred movies 
def calculate_matches(actors, film_actors):
	actors_to_actors = {}
	for i in actors:
		actors_to_actors[i] = dict(zip(actors, [0].copy()*len(actors)))


	for f in film_actors.keys():
		for i in range(0, len(film_actors[f])):
			for j in range(i+1, len(film_actors[f])):
				actors_to_actors[film_actors[f][i]][film_actors[f][j]] += 1
				actors_to_actors[film_actors[f][j]][film_actors[f][i]] += 1

	return actors_to_actors

if __name__ == "__main__":

	start_time = time.time()

	#connecting to mongodb
	con = MongoClient("mongodb://localhost")

	#connecting to database named dvdrental in mongo
	db = con['dvdrental']

	actors, film_actors = set_actors_and_film_actors(db)
	actors_to_actors = calculate_matches(actors, film_actors)

	#writes actors_to_actors table to query2.csv file
	with open('query2.csv', 'w', newline='') as f:
		fieldnames = ['name']
		for actor_id in actors:
			actor = db['actor'].find_one({'actor_id': actor_id})
			fieldnames.append(actor['first_name']+' '+actor['last_name'])

		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()

		for i in actors_to_actors.keys():
			actor = db['actor'].find_one({'actor_id': i})
			d = {'name': actor['first_name']+' '+actor['last_name']}
			for actor_id in actors:
				coactor = db['actor'].find_one({'actor_id': actor_id})
				d[coactor['first_name']+' '+coactor['last_name']] = actors_to_actors[i][actor_id]
			writer.writerow(d)

	#closes connection to mongodb
	con.close()

	print(f'Now you can see the results in query2.csv file.')
	print(f'--- {(time.time() - start_time)} seconds ---')
