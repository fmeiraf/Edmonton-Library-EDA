import pandas as pd
import os

genreDf = pd.read_csv('finalPubDataset.csv')
updateDf = pd.read_csv('suportGenreDt.csv')

def update_dataset(recepient_df, updater_df):
	''' take a recepient data frame and update it with genre information from
		another data frame '''

	recepient_df['pubdate'] = recepient_df['pubdate'].fillna('NA')
	pub_dict = updater_df.to_dict(orient='list')

	recepient_df.loc[recepient_df.pubdate == 'NA', 'pubdate'] = recepient_df[recepient_df['pubdate'] == 'NA']['title'].apply(lambda x: pub_dict[x][0])

	recepient_df.to_csv('finalPubDataset.csv')

	return 'Done!'

update_dataset(genreDf, updateDf)
if os.path.exists('EmergencyDt.csv'):
	os.remove('EmergencyDt.csv')
