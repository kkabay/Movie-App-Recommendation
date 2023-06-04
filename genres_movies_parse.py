import mysql.connector
import pandas as pd
import ast

# Connect to the MySQL database
cnx = mysql.connector.connect(user='root', password='',
                              host='127.0.0.1', database='movie-diploma')
cursor = cnx.cursor()

# Load the movie dataset into a Pandas DataFrame
movies_df = pd.read_csv('results/movies_metadata.csv')
movies_df['genres'] = movies_df['genres'].apply(lambda x: ast.literal_eval(x))

# Load the genres from the genres table
query1 = "SELECT id, genre_name FROM genres"
cursor.execute(query1)
genres = cursor.fetchall()

# Create a dictionary of genre_name to genre_id mappings
genre_dict = {genre[1]: genre[0] for genre in genres}

# Loop through the movies and insert the genres into the movie_genres table
for index, movie in movies_df.iterrows():
    if movie['title'] is not None:
        # Get the movie_id for the current movie
        query2 = "SELECT id FROM movies WHERE title = %s LIMIT 1"
        cursor.execute(query2, (movie['title'],))
        movie_id = cursor.fetchone()

        if movie_id:
            # Map the genres to genre_ids
            movie_genres = [genre_dict[genre['name']] for genre in movie['genres'] if
                            genre['name'] in genre_dict and genre['name'] is not None]

            # Insert the movie_genres into the movie_genres table
            for genre_id in movie_genres:
                # Check if the row already exists
                query_check = "SELECT COUNT(*) FROM movie_genres WHERE movie_id = %s AND genre_id = %s"
                cursor.execute(query_check, (movie_id[0], genre_id))
                count = cursor.fetchone()[0]

                if count == 0:
                    # Insert the movie_genres into the movie_genres table
                    query_insert = "INSERT INTO movie_genres (movie_id, genre_id) VALUES (%s, %s)"
                    cursor.execute(query_insert, (movie_id[0], genre_id))
            cnx.commit()

# Commit the changes and close the cursor and connection
cursor.close()
cnx.close()
