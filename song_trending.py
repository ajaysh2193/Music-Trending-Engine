
# coding: utf-8

from datetime import datetime
globalstart = datetime.now()

print("\nScript started execution:", globalstart, "\n")

import numpy as np
import pandas as pd


# Class for Popularity based Recommender System model
class popularity_recommender():
    def __init__(self):
        self.train_data = None
        self.score= None
        self.item_id = None
        self.popularity_recommendations = None
        
    # Create the popularity based recommender system model
    def create(self, train_data, score, item_id):
        self.train_data = train_data
        self.score = 'score'
        self.item_id = 'audio_track_id'

        # Get a count of score for each unique song as recommendation score
        train_data_grouped = train_data.groupby([self.item_id]).agg({self.score: 'count'}).reset_index()
       
    
        # Sort the songs based upon recommendation score
        train_data_sort = train_data_grouped.sort_values(['score', self.item_id], ascending = [0,1])
    
        # Generate a recommendation rank based upon score
        
        self.popularity_recommendations = train_data_sort

    # Use the popularity based recommender system model to
    # make recommendations
    def recommend(self,score):    
        user_recommendations = self.popularity_recommendations
        
        user_recommendations['score'] = df['score']
   
        cols = user_recommendations.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        user_recommendations = user_recommendations[cols]
        
        return user_recommendations
    
    

# Connection to get data from database
import mysql.connector

cnx = mysql.connector.connect(
    user='analytics', password='analytics@123',
    host='localhost', database='indieon'
)

query = '''SELECT
    `a`.`id` AS `audio_track_id`, `a`.`title` AS `audio_track_title`, `g`.`name` AS `genre_name`, 
    (SELECT IFNULL(SUM(`f`.`favourite`), 0) FROM `favourites` AS `f` WHERE `f`.`entity` = 'audio' AND `f`.`entity_id` = `a`.`id`) AS `favourite_count`,
    (SELECT IFNULL(SUM(`l`.`like`), 0) FROM `likes` AS `l` WHERE `l`.`entity` = 'audio' AND `l`.`entity_id` = `a`.`id`) AS `like_count`,
    (SELECT IFNULL(SUM(`r`.`value` / `r`.`out_of`), 0) FROM `ratings` AS `r` WHERE `r`.`entity` = 'audio' AND `r`.`entity_id` = `a`.`id`) AS `rating_count`
FROM
    `audio_tracks` AS `a` LEFT JOIN `genres` AS `g` ON `g`.`id` = `a`.`genre_id`
WHERE
	`a`.`album_id` IS NULL AND `a`.`status` = 1 AND `a`.`is_converted` != 0 AND `a`.`deleted_at` IS NULL
GROUP BY
	`a`.`id`;
'''

df = pd.read_sql_query(query, cnx)


# Merge song title and artist_name columns to make a merged column
df['song'] = df['audio_track_title'].map(str) + " - " + df['genre_name']

df['score'] = df['like_count'] + df['favourite_count'] + df['rating_count']


song_grouped = df.groupby(['audio_track_id']).agg({'score':'sum'}).reset_index()
grouped_sum = song_grouped['score'].sum()
song_grouped['percentage']  = song_grouped['score'].div(grouped_sum)*100
df = song_grouped.sort_values(['score', 'audio_track_id'], ascending = [0,1])


pm = popularity_recommender()
pm.create(df, 'score', 'audio_track_id')
pm = pm.recommend('score')

# Get the top 10 recommendations
trend = pm.sort_values(by=['score'],ascending=[0]).head(10)
trend = trend.assign(entity="audio")
trend = trend[['entity', 'audio_track_id', 'score']]

foo = [tuple(x) for x in trend.values]
values = ', '.join(map(str, foo))



# Storing results into database
cursor = cnx.cursor()

query = "DELETE from trending_entities"
cursor.execute(query)
cnx.commit()

print("\n", cursor.rowcount, "record deleted at:", datetime.now())

sql = "INSERT INTO trending_entities (entity, entity_id, entity_score) VALUES {}".format(values)
cursor.execute(sql)
cnx.commit()

print("\n", cursor.rowcount, "record inserted at:", datetime.now())

cursor.close()
cnx.close()

print("\nScript finished execution:", datetime.now())

print("\nTotal time taken to execute is:", datetime.now()-globalstart, "\n")

