import pandas as pd
import os

genreDf = pd.read_csv('finalGenreDataset.csv')
updateDf = pd.read_csv('suportGenreDt.csv')

def update_dataset(recepient_df, updater_df):
	''' take a recepient data frame and update it with genre information from
		another data frame '''

	recepient_df['genre'] = recepient_df['genre'].fillna('NA')
	genre_dict = updater_df.to_dict(orient='list')

	recepient_df.loc[recepient_df.genre == 'NA', 'genre'] = recepient_df[recepient_df['genre'] == 'NA']['title'].apply(lambda x: genre_dict[x][0])

	recepient_df.to_csv('finalGenreDataset.csv')

	return 'Done!'

update_dataset(genreDf, updateDf)
if os.path.exists('EmergencyDt.csv'):
	os.remove('EmergencyDt.csv')

