import pandas as pd
import mysql.connector
from datetime import datetime

# Read the movies dataset into a pandas dataframe
movies = pd.read_csv('results/movies_metadata.csv', low_memory=False)

# Extract the genres from the genres column
genres_list = []
for row in movies.itertuples():
    genres = row.genres
    for genre in eval(genres):
        genre_name = genre['name']
        genres_list.append(genre_name)

# Remove duplicate genres
unique_genres = set(genres_list)
# Connect to the MySQL database
cnx = mysql.connector.connect(user='root', password='',
                               host='127.0.0.1', database='movie-diploma')
cursor = cnx.cursor()

# Insert the genres into the "genres" table
for genre_name in unique_genres:
    query = "INSERT INTO genres (genre_name, created_at) VALUES (%s, %s)"
    cursor.execute(query, (genre_name, datetime.now()))
cnx.commit()

# Close the database connection
cnx.close()
