import pymongo

mongos_ip = "54.242.181.42"
client = pymongo.MongoClient(mongos_ip, 27017)
db = client.test

# Prompt user to select a number between 1 and 15
print("1. Recommend a movie randomly\n")
print("2. Find movies released in a specific year\n")
print("3. Find movies with more than specified minimum tags\n")
print("4. Find movies with a specific rating\n")
print("5. Find movies and their tags\n")
print("6. Find movies by a specific director\n")
print("7. Add movie tags\n")
print("8. Find the total count of tags for a movie\n")
print("9. Find the average rating of a movie\n")
print("10. Get recommendations for the top 10 movies\n")
print("11. Find movies with specific cast members\n")
print("12. Find all survey answers for a specific user and tag movies\n")
print("13. Find the number of reviews a movie has\n")
print("14. Query to find movies with ratings greater than x\n")
print("15. Count all movings with rating greater than x\n")
i = int(input("\nPlease select the function: "))


# Query to recommend a movie randomly
if i == 1:
    res_id = db.metadata.find_one()
    print(res_id)

# Query to recommend movies released in a specific year
elif i == 2:
    year = int(input("\nEnter the year of release: "))
    query = {"title": {"$regex": str(year)}}
    results = db.metadata.find(query)
    for movie in results:
        print(movie)

# Query to recommend movies with more than specified minimum tags
elif i == 3:
    min_tag_count = int(input("\nEnter the minimum tag count for the movie: "))
    query = {"num": {"$gt": min_tag_count}}
    results = db.tag_count.find(query)
    for movie in results:
        movie_id = movie["item_id"]
        metadata_query = {"item_id": movie_id}
        metadata_result = db.metadata.find_one(metadata_query)
        print(metadata_result)

# Query to recommend movies with a specific rating
elif i == 4:
    rating = float(input("\nEnter the rating of the movie: "))
    query = {"avgRating": rating}
    results = db.metadata.find(query)
    for movie in results:
        print(movie)

# Query to see movies with their tags
elif i == 5:
    movie_id = int(input("\nEnter the movie id: "))
    metadata_query = {"item_id": movie_id}
    metadata_result = db.metadata.find_one(metadata_query)
    tag_query = [
        {
            "$lookup": {
                "from": "tags",
                "localField": "tag_id",
                "foreignField": "id",
                "as": "tags",
            }
        },
        {"$match": {"item_id": movie_id}},
    ]
    tag_result = db.survey_answers.aggregate(tag_query)
    print(metadata_result)
    for tag in tag_result:
        print(tag)

# Query to see movies for a particular director
elif i == 6:
    director = input("\nEnter the director's name: ")
    query = {"directedBy": director}
    results = db.metadata.find(query)
    if not results:
        print("No movies found for the director!")
    for movie in results:
        print(movie)

# Query to add movie tags
elif i == 7:
    item_id = int(input("\nEnter the movie id of the movie you want to tag : "))
    tag_name = input("\nEnter the tag name : ")
    tag_id_query = {'tag': tag_name}
    tag_id_result = db.tags.find_one(tag_id_query)
    if not tag_id_result:
        tag_id_result = db.tags.insert_one({'tag': tag_name})
        tag_id = tag_id_result.inserted_id
    else:
        tag_id = tag_id_result['id']
    tag_query = {'item_id': item_id, 'tag_id': tag_id}
    tag_result = db.tag_count.find_one(tag_query)
    if not tag_result:
        db.tag_count.insert_one({'item_id': item_id, 'tag_id': tag_id, 'num': 1})
    else:
        num = tag_result['num'] + 1
        db.tag_count.update_one(tag_query, {'$set': {'num': num}})
    print(f"Tag '{tag_name}' added to movie with id '{item_id}'.")



# Query to find the total count of tags for a movie
elif i == 8:
    movie_id = int(input("\nEnter the movie id : "))
    tag_count_query = {'item_id': movie_id}
    tag_count_result = db.tag_count.find_one(tag_count_query)
    print(f"Total count of tags for movie with id {movie_id}: {tag_count_result['num']}")

# Query to find the average rating of a movie
elif i == 9:
    movie_id = int(input("\nEnter the movie id : "))
    query = [{'$match': {'item_id': movie_id}}, {'$group': {'_id': '$item_id', 'avg_rating': {'$avg': '$avgRating'}}}]
    result = db.metadata.aggregate(query)
    for movie in result:
        print(f"Movie ID: {movie['_id']}, Average Rating: {movie['avg_rating']}")

# Query to get recommendations for the top 10 movies
elif i == 10:
    query = [
        {'$group': {'_id': '$item_id', 'avg_rating': {'$avg': '$average_rating'}}},
        {'$sort': {'avg_rating': -1}},
        {'$limit': 10}
    ]
    result = db.metadata.aggregate(query)
    for movie in result:
        print(f"Movie ID: {movie['_id']}, Average Rating: {movie['avg_rating']}")


# Query to find specific cast member:
elif i == 11:
    actor = input("\nEnter the actor's name: ")
    query = {"starring": actor}
    results = db.metadata.find(query)
    if not results:
        print("No movies found for the actor!")
    for movie in results:
        print(movie)


# Query to find survey answers for a specific user and tag movies:
elif i == 12:
    userID = int(input("\nEnter the user's ID: "))
    query = {'user_id': userID}
    survey_results = db.survey_answers.find(query).limit(10)
    
    for survey in survey_results:
        itemID = survey['item_id']
        tagID = survey['tag_id']
        score = survey['score']

        tag = db.tags.find_one({'id': tagID})['tag']
        movie = db.metadata.find_one({'item_id': itemID})['title']
        print(f"User ID: {userID}, Movie: {movie}, Tag: {tag}, Score: {score}")

# Query to find total number of reviews a movie has:
elif i == 13:
    movie_ID = int(input("\nEnter the movieID: "))
    query = {'item_id': movie_ID}
    count = db.reviews.count_documents(query)
    print(f"Movie_ID: {movie_ID} has {count} reviews")


# Query to find movies with ratings greater than x:
elif i == 14:
    rating = float(input("\nEnter the minimum Rating between 0-5: "))
    query = {"avgRating": {"$gt": rating}}
    results = db.metadata.find(query).limit(15)
    for movie in results:
        print(f"Movie: {movie['title']}, Rating: {movie['avgRating']}")


# Count all movings with rating greater than x:
elif i == 15:
    rating = float(input("\nEnter the minimum Rating between 0-5: "))
    query = {"avgRating": {"$gt": rating}}
    count = db.metadata.count_documents(query)
    print(f"Total number of movies with rating greater than {rating} is {count}")

# else condition if none of the proper options are slecetd
else:
    print("Option selected is not in the list. Please choose a correct option !")