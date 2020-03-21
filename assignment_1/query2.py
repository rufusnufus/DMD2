from pymongo import MongoClient
import csv
import time

def set_actors_and_film_actors(db):
	film_actors = {}
	actors = set()
	for film in db['film_actor'].find({},{'_id':0, 'actor_id':1, 'film_id':1}):
		actor = db['actor'].find_one({'actor_id': film['actor_id']}, {'_id':0, 'first_name':1, 'last_name':1})
		actor = actor['first_name']+ ' ' + actor['last_name']
		actors.add(actor)
		if film['film_id'] not in film_actors:
			film_actors[film['film_id']] = [actor]
		else:
			film_actors[film['film_id']].append(actor)

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

	start_time = time.time()

	#connecting to mongodb
	con = MongoClient("mongodb://localhost")

	#connecting to database named dvdrental in mongo
	db = con['dvdrental']

	actors, film_actors = set_actors_and_film_actors(db)
	actors_to_actors = calculate_matches(actors, film_actors)

	with open('query2.csv', 'w', newline='') as f:
		fieldnames = ['name']
		for actor in actors:
			fieldnames.append(actor)

		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()

		for actor in actors_to_actors.keys():
			d = {'name': actor}
			for coactor in actors:
				d[coactor] = actors_to_actors[actor][coactor]
			writer.writerow(d)

	con.close()

	print(f'Now you can see the results in query2.csv file.')
	print(f'--- {(time.time() - start_time)} seconds ---')
