from fastapi import FastAPI
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sqlalchemy import Column, Integer, String, Text, Float, Boolean, Date, TIMESTAMP, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from starlette.middleware.cors import CORSMiddleware

Base = declarative_base()


class MovieGenre(Base):
    __tablename__ = 'movie_genres'
    movie_id = Column(Integer, primary_key=True, autoincrement=True)
    genre_id = Column(Integer, primary_key=True, autoincrement=True)


class Movie(Base):
    def __lt__(self, other):
        return self.rating < other.rating
    __tablename__ = 'movies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(Text, nullable=False)
    description = Column(Text, nullable=False)
    rating = Column(Float, default=0, nullable=False)
    imdb_url = Column(Text)
    image_url = Column(Text, nullable=False)
    release_date = Column(Date)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)
    banner_url = Column(String(255))
    preview_url = Column(Text)
    is_premium = Column(Boolean, default=False, nullable=False)

    # Relationship with Genres
    # genres = relationship('Genre', secondary=MovieGenre, back_populates='movies')
    favorites = relationship('Favorite', back_populates='movie')


class Genre(Base):
    __tablename__ = 'genres'

    id = Column(Integer, primary_key=True, autoincrement=True)
    genre_name = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

    # Add genre_id column here
    # Add genre_id column here
    # movies = relationship('Movie', secondary=Mo, back_populates='genres')
    # genre_id = Column(Integer, ForeignKey('genres.id'))


class Favorite(Base):
    __tablename__ = 'favorites'

    user_id = Column(Integer, ForeignKey('users.id'), primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), primary_key=True)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

    # Relationship with User
    user = relationship('User', back_populates='favorites')

    # Relationship with Movie
    movie = relationship('Movie', back_populates='favorites')


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    password = Column(String(255), nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)

    # Relationship with Favorites
    favorites = relationship('Favorite', back_populates='user')


app = FastAPI()


origins = [
    "http://localhost:3000",
    "localhost:3000"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

engine = create_engine("mysql+mysqldb://root@localhost/movie-diploma", echo=True)
Session = sessionmaker(bind=engine)



# Function to get recommended movies
# Function to get recommended movies
def get_recommendations(movie_id: int, user_id: int) -> List[Movie]:
    # Create a session to query the database
    session = Session()

    # Get the genres of the movie
    # movie_genres = session.query(MovieGenre).filter_by(movie_id=movie_id).all()
    movie_genres = session.query(Genre)\
        .join(MovieGenre, MovieGenre.genre_id == Genre.id).filter_by(movie_id=movie_id).all()

    # Get the user's favorite movies
    user_favorites = session.query(Favorite).filter_by(user_id=user_id).all()

    # Create a list of the user's favorite movie IDs
    favorite_ids = [f.movie_id for f in user_favorites]

    # Get the genres of the user's favorite movies
    favorite_genres = session.query(Genre)\
        .join(MovieGenre, MovieGenre.genre_id == Genre.id)\
        .filter(MovieGenre.movie_id.in_(favorite_ids)).distinct().all()

    favorite_genres = list(set(movie_genres) & set(favorite_genres))


    # Get all the movies that have at least one genre in common with the user's favorite movies
    related_movies = session.query(Movie)\
        .join(MovieGenre, MovieGenre.movie_id == Movie.id)\
        .filter(MovieGenre.genre_id.in_([g.id for g in favorite_genres])).all()

    # Filter out the movies that the user has already favorited
    related_movies = [m for m in related_movies if m.id not in favorite_ids]

    # Create a TF-IDF matrix of the movie titles and descriptions
    tfidf = TfidfVectorizer(stop_words='english')
    movie_matrix = tfidf.fit_transform([m.title + ' ' + m.description for m in related_movies])

    # Calculate the cosine similarity between the input movie and the related movies
    cosine_similarities = linear_kernel(movie_matrix[0:1], movie_matrix).flatten()

    # Sort the related movies by their similarity to the input movie
    related_movies = sorted(list(zip(cosine_similarities[1:], related_movies[1:])), reverse=True)

    # Get the top 10 related movies
    top_related_movies = [m[1] for m in related_movies[:10]]

    # Close the session
    session.close()

    return top_related_movies


# Example usage
@app.get("/recommendations")
def get_movie_recommendations(movie_id: int, user_id: int):
    recommendations = get_recommendations(movie_id, user_id)
    return recommendations


import uvicorn

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000, log_level="info")
