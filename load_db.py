import pymongo
import movies

def load_movies(filename):
    """1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    
    id, title, release_date, vhs_release_date, imdb_url, """

    movie_dict = {}

    f = open(filename)
    for line in f:
        fields = clean_line(line, "|")
        if not fields:
            continue
        movie_id = int(fields[0])
        title = fields[1]
        release_date = fields[2]
        imdb_url = fields[4]
        genre_list = fields[5:]
        
        movie = {
                "_id": movie_id,
                "title": title,
                "release_date": release_date,
                "imdb_url": imdb_url,
                "genre_list": genre_list
            }

        movie_dict[movie_id] = movie
    f.close()
    # For loop is ended
    return movie_dict

def load_genres(movie_dict, filename):
    genre_dict = {}
    f = open(filename)
    for line in f:
        fields = clean_line(line, "|")
        if not fields:
            continue
        key = int(fields[1]) # the key is the second field
        genre = fields[0] # genre is the first field
        genre_dict[key] = genre
    f.close()

    for movie_id, movie in movie_dict.items():
        genre_ids = movie['genre_list']
        genre_list = []

        for i in range(len(genre_ids)):
            if genre_ids[i] == "1":
                genre = genre_dict[i]
                genre_list.append(genre)

        # genre list is populated
        movie['genres'] = genre_list

def clean_line(line, sep=None):
    stripped = line.strip().decode("latin-1").encode("utf-8")
    if stripped == "":
        return None
    
    fields = stripped.split(sep)
    #return [ field.decode("ascii").encode("utf-8") for field in stripped.split(sep) ]
    
    return fields
    
def load_users(filename):
    f = open(filename)
    user_db = {}
    genders = {"M": "Male", "F": "Female"}
    for line in f:
        fields = clean_line(line, "|")
        id, age, gender, occupation, zipcode = fields
        id = int(id)
        age = int(age)
        gender = genders[gender]
        user_db[id] = {
                "_id": id,
                "age": age,
                "gender": gender,
                "occupation": occupation
                }

    f.close()

    return user_db

def load_ratings(filename):
    ratings = []
    f = open(filename)
    for line in f:
        fields = clean_line(line)
        if not fields:
            continue
        user_id, movie_id, rating, timestamp = fields
        user_id = int(user_id)
        movie_id = int(movie_id)
        rating = int(rating)
#        movie_ratings = ratings.get(movie_id)
#        if not movie_ratings:
#            ratings[str(movie_id)] = {str(user_id): rating}
#        else:
#            movie_ratings[str(user_id)] = rating
        ratings.append({"user_id": user_id,
                "movie_id": movie_id,
                "rating": rating})

    f.close()

    return ratings

def push_to_mongo(db, collection_name, local_db):
    for _id, item in local_db.items():
        db[collection_name].insert(item)

def push_ratings(db, ratings):
    #db.ratings.update({"by_movie": {"$exists": True}}, {"by_movie":ratings}, upsert=True)
    db.ratings.insert(ratings)

def main():
    global db
    movie_db = load_movies("../ml-100k/u.item")
    load_genres(movie_db, "../ml-100k/u.genre")
    user_db = load_users("../ml-100k/u.user")
    ratings_db = load_ratings("../ml-100k/u.data")

    db = movies.connect_db("dbh36.mongolab.com", 27367, "movie_user", "password", "movies")
    #db = pymongo.connection.Connection("localhost")
    db = db['movies']

    push_to_mongo(db, "movies", movie_db)
    push_to_mongo(db, "users", user_db)
    push_ratings(db, ratings_db)

    preloaded = [
        (71, 5),
        (72, 4),
        (588, 5),
        (1014, 4),
        (541, 5),
        (543, 5),
        (42, 4),
        (83, 5),
        (82, 5),
        (88, 2),
        (739, 2)
            ]


    movies.db = db
    for movie,rating in preloaded:
        movies.rate_movie(movie, rating)

if __name__ == "__main__":
    main()
