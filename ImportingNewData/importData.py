import googlebooks
import numpy as np
import time
import pandas as pd
import os
from collections import defaultdict, Counter
import pdb

def col_check(interest_cols, tocheck_list):
	col_checker = defaultdict()
	for col in interest_cols:
		if col in tocheck_list:
			col_checker[col] = True
		else:
			col_checker[col] = False 
	return col_checker

def prepare_data(csv_file_path):
	data = pd.read_csv(csv_file_path, header=0, names=['row_id', 'branch_id', 'branch_name', 'holds', 'title', 'author',
	   'date', 'url', 'genre', 'google_rating', 'publishing_Date'])

	data = data[data.title.notnull()]
	data = data[data.branch_name.notnull()]
	data.genre = data.genre.fillna('NA')
	data.google_rating = data.google_rating.fillna('NA')
	data.publishing_Date = data.publishing_Date.fillna('NA')

	#Taking off the name of the authors from the title
	data['title'] = data['title'].apply(lambda x: x.split(' /')[0])

	if os.path.exists('EmergencyDt.csv'):
		em = pd.read_csv('EmergencyDt.csv').iloc[:,1:] #getting the already looked books
		em_dict = em.to_dict(orient='list')
		titles_dict = {row['title']:row['author'] for index, row in data.iterrows() if row['genre']=='NA' and row['title'] not in em_dict['title']}
		print(len(titles_dict.keys()))
		### Building a dict with the genre and title ###
		final_data = em_dict
	else:
		titles_dict = {row['title']:row['author'] for index, row in data.iterrows() if row['genre']=='NA'}
		print(len(titles_dict.keys()))
		final_data = defaultdict(list)

	return data, titles_dict, final_data


def get_pub(csv_file_path, google_api_client):
	''' Use a csv file with 'titles' column to look the book genre using google Api
		csv_file_path = csv file containin at least title and author column
		google_api_client = googlebooks api client
		emergencyDt = mechanism used to prevent data loss from API errors during executions
	'''
	data, titles_dict, final_data = prepare_data(csv_file_path)
	
	### Checking the data that should be look for ###
	
	interest_cols = ['categories', 'averageRating', 'publishedDate']
	emergency_counter = 0

	for title, author in titles_dict.items():
		#pdb.set_trace()
		print('going through title : {}'.format(title))
		final_data['title'].append(title)
		time.sleep(3)

		query = google_api_client.list(title, inauthor=author)

		#Collect 5 first queries for each title
		params = defaultdict(list)
		query_explore_limit = 0
		try:
			for item in query['items']:
				if query_explore_limit <=5:
					query_explore_limit += 1
					col_checker = col_check(interest_cols, list(item['volumeInfo'].keys()))   
					for key in col_checker.keys():
						if col_checker[key] == True:
							if key == 'categories':
								params[key].append(item['volumeInfo'][key][0])
							elif key == 'publishedDate':
								pub_year = item['volumeInfo'][key].split('-')[0]
								params[key].append(pub_year)
							else:
								params[key].append(item['volumeInfo'][key])
				else:
					query_explore_limit = 0
					break
		except TypeError:
			print('Request 403 forbidden')
			time.sleep(5)
			col_checker = col_check(interest_cols, list(item['volumeInfo'].keys()))
			for key in col_checker.keys():
				params[key].append(np.nan)

		#Decides who win the votes and finishes the title proccess
		tries = ['categories', 'averageRating', 'publishedDate']
		for t in tries:
			#pdb.set_trace()
			try:
				final_data[t].append(Counter(params[t]).most_common(1)[0][0])
			except IndexError:
				print('IndexError in {}'.format(t))
				print(params)
				final_data[t].append(np.nan)
				time.sleep(5)
		
		emergency_counter += 1
		print('done - next book.')

		if emergency_counter >15:
			#pdb.set_trace()
			emergencyDt = pd.DataFrame(final_data)
			emergencyDt.to_csv('EmergencyDt.csv')
			emergency_counter = 0

	print('Created DataFrame with all genres')
	finalDt = pd.DataFrame(final_data)
	finalDt.to_csv('suportGenreDt.csv')		

	return 'Done!'		


google_api = googlebooks.Api()

get_pub('Final_Dataset.csv', google_api)
