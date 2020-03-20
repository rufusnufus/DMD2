from pymongo import MongoClient
import csv
import time

start_time = time.time()

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

	#connecting to mongodb
	con = MongoClient("mongodb://localhost")

	#connecting to database named dvdrental in mongo
	db = con['dvdrental']

	actors, film_actors = set_actors_and_film_actors(db)
	actors_to_actors = calculate_matches(actors, film_actors)

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

	con.close()

	print(f'Now you can see the results in query2.csv file.')
	print(f'--- {(time.time() - start_time)} seconds ---')
