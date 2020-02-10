import googlebooks
import numpy as np
import time
import pandas as pd
import os



def get_genre(csv_file_path, google_api_client):
	''' Use a csv file with 'titles' column to look the book genre using google Api
		csv = csv file containin at least title and author column
		google_api_client = googlebooks api client
		emergencyDt = mechanism used to prevent data loss from API errors during executions
	'''	 

	data = pd.read_csv(csv_file_path,header=0, names=['row_id', 'branch_id', 'branch_name', 'holds', 'title', 
	                                               'author', 'date', 'url', 'genre'])
	data = data[data.title.notnull()]
	data = data[data.branch_name.notnull()]
	data.genre = data.genre.fillna('NA')
	#Taking off the name of the authors from the title
	data['title'] = data['title'].apply(lambda x: x.split(' /')[0])


	### Checking the data that should be look for ###
	if os.path.exists('EmergencyDt.csv'):
		em = pd.read_csv('EmergencyDt.csv') #getting the already looked books
		em_dict = em.to_dict(orient='list')
		suport_dict = {row['title']:row['author'] for index, row in data.iterrows() if row['genre']=='NA' and row['title'] not in em_dict.keys()}
		print(len(suport_dict.keys()))
		### Building a dict with the genre and title ###
		genre_dict = em_dict
	else:
		suport_dict = {row['title']:row['author'] for index, row in data.iterrows() if row['genre']=='NA' and row['title']}
		print(len(suport_dict.keys()))
		genre_dict = {}

	if len(suport_dict.keys()) <= 0:
		return 'you now have all book genre possible, you should be good by now!'
	else:

		emergency_counter = 0
		for key, value in suport_dict.items():
			print('going through title : {}'.format(key))
			time.sleep(2)
			try:
				genre_dict[key] = [google_api.list(key,inauthor=value )['items'][0]['volumeInfo']['categories'][0]]
				print('first try done')
				emergency_counter += 1
			except KeyError:
				print('title : {} , KeyError - did not find categories'.format(key))
				time.sleep(1)
				solved = False

				try:
					for element in google_api.list(key,inauthor=value)['items']: #this approach will try to catch the first result with categorie, 
						time.sleep(3)											 #might get wrong though
						try:
							genre_dict[key] = [element['volumeInfo']['categories'][0]]
							print('got it on second try, genre: {}'.format(element['volumeInfo']['categories'][0]))
							solved = True
							break
						except KeyError:
							continue
						except TypeError:
							continue
				except TypeError:
					pass
				if not solved:
					print('not able to retrieve categories')
					genre_dict[key] = [np.nan]
				emergency_counter += 1
			except TypeError:
				print('TypeError, response: {}'.format('403 forbiden'))
				time.sleep(7)
				genre_dict[key] = [np.nan]
				emergency_counter += 1
			print('done!')
			if emergency_counter >= 15: # in case of api stop working so don't loose all the data
				emergencyDt = pd.DataFrame(genre_dict)
				emergencyDt.to_csv('EmergencyDt.csv')
				emergency_counter = 0

		print('Created DataFrame with all genres')
		emergencyDt = pd.DataFrame(genre_dict)
		emergencyDt.to_csv('suportGenreDt.csv')

		return 'worked a bit more, but now your are done'


google_api = googlebooks.Api()

get_genre('finalGenreDataset.csv', google_api)
