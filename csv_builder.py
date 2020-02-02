import pandas as pd
from os import listdir
from os.path import isfile, join

categories = ['Comedy', 'Action', 'Documentary']
csv_path = 'info.csv'

def get_file_list():
    return [join('data', 'tweets', f) for f in listdir(join('data', 'tweets')) if isfile(join('data', 'tweets', f))]

def get_user_id_from_filepath(filepath):
    return filepath.split('/')[-1].split('.')[0]

def get_hashtags(filename):
    hashtags = list()
    try:
        tweets = pd.read_json(filename)
        arr = tweets['entities']
        for entity in arr:
            for hashtag in entity['hashtags']:
                hashtags.append(hashtag['text'])
    except:
        return []
    return hashtags

def get_top_hashtags(arr, max_quant):
    my_dict = {i:arr.count(i) for i in arr}
    my_dict = {k: v for k, v in sorted(my_dict.items(), key=lambda item: item[1], reverse=True)}
    return list(my_dict.keys())[:max_quant]

movies = pd.read_csv(join('data', 'movietweetings', 'movies.dat'), sep="::", engine="python", \
    names=['id', 'title', 'categories'])
users = pd.read_csv(join('data', 'movietweetings', 'users.dat'), sep="::", engine="python", \
    names=['id', 'twitter_id'])
ratings = pd.read_csv(join('data', 'movietweetings', 'ratings.dat'), sep="::", engine="python", \
    names=['user_id', 'movie_id', 'rating', 'timestamp'])

df = pd.DataFrame(columns=categories)

for filename in get_file_list():
    hashtags = get_top_hashtags(get_hashtags(filename), 100)
    if len(hashtags) == 0:
        continue
    
    twitter_id = get_user_id_from_filepath(filename)
    user_id = users[users.twitter_id == int(twitter_id)].iloc[0]['id']
    
    ratings_dic = {}
    user_ratings = ratings[ratings.user_id == int(user_id)]
    for rating_line in user_ratings.iterrows():
        movie_id = int(rating_line[1].movie_id)
        rating = int(rating_line[1].rating)
        
        movie = movies[movies.id == movie_id]
        cat_arr = movie.iloc[0]['categories']
        if cat_arr == None:
            continue
        cat_arr = cat_arr.split('|')
        for category in categories:
            if category in cat_arr:
                if category in ratings_dic:
                    ratings_dic[category].append(rating)
                else:
                    ratings_dic[category] = [rating]
    if len(ratings_dic) == 0:
        continue

    for key in ratings_dic:
        ratings_dic[key] = sum(ratings_dic[key])/len(ratings_dic[key])
    
    for hashtag in hashtags:
        ratings_dic[hashtag] = 1
    
    df = df.append(ratings_dic, ignore_index=True)
    
df.to_csv(csv_path, sep=',', encoding='utf-8')
