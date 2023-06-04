import pandas as pd
import mysql.connector

# Load the dataset into a pandas DataFrame
df = pd.read_csv('results/movies_metadata.csv')

# Clean and transform the dataset
df = df[['title', 'overview', 'vote_average', 'imdb_id', 'poster_path', 'release_date']]
df = df.rename(columns={'overview': 'description', 'vote_average': 'rating',
                        'imdb_id': 'imdb_url', 'poster_path': 'image_url'})
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
default_date = pd.Timestamp('2001-06-07')
df['release_date'].fillna(default_date, inplace=True)
df.fillna(0, inplace=True)
# Connect to the MySQL database
cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1', database='movie-diploma')

# Iterate over the rows of the DataFrame and insert each row into the movies table
cursor = cnx.cursor()
for index, row in df.iterrows():
    query = "INSERT INTO movies (title, description, rating, imdb_url, image_url, release_date) " \
            "VALUES (%s, %s, %s, %s, %s, %s)"
    data = (row['title'], row['description'], row['rating'], row['imdb_url'], row['image_url'],
            row['release_date'].date().strftime('%Y-%m-%d'))
    cursor.execute(query, data)
cnx.commit()

# Close the database connection
cursor.close()
cnx.close()
