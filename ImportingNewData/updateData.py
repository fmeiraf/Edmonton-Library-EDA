import pandas as pd
import os
from collections import defaultdict

def prepare_data(recepient_df):
	recepient_df['genre'] = recepient_df['genre'].fillna('NA')
	recepient_df['google_rating'] = recepient_df['google_rating'].fillna('NA')
	recepient_df['publishing_Date'] = recepient_df['publishing_Date'].fillna('NA')

	recepient_df = recepient_df[recepient_df.title.notnull()]
	recepient_df = recepient_df[recepient_df.branch_name.notnull()]
	recepient_df['title'] = recepient_df['title'].apply(lambda x: x.split(' /')[0])

	return recepient_df

def update_dataset(recepient_df, updater_df):
	''' take a recepient data frame and update it with genre information from
		another data frame '''

	recipient_dt = prepare_data(recepient_df)

	#creating the updater dict from the updater dataframe
	data_dict = defaultdict()
	for index, row in updater_df.iterrows():
	    content = {
	        'genre' : row['categories'],
	        'google_rating' : row['averageRating'],
	        'publishing_Date' : row['publishedDate']
	    }
	    data_dict[row['title']] = content

	#inputing info in the recipient dataframe
	for col in content.keys():
		try:
			recipient_dt.loc[recipient_dt[col] == 'NA', col] = recipient_dt[recipient_dt[col] == 'NA']['title'].apply(lambda title: data_dict[title][col])
		except KeyError:
			print('error in col {}'.format(col))
			continue

	recipient_dt.to_csv('Final_Dataset.csv')
	if os.path.exists('EmergencyDt.csv'):
		os.remove('EmergencyDt.csv')
	
	print('Merged!')

	return 'Done!'


recepient_df = pd.read_csv('Final_Dataset.csv')
updateDf = pd.read_csv('suportGenreDt.csv')
update_dataset(recepient_df, updateDf)
