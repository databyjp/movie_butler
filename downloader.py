import tmdbsimple as tmdb
import glob
import os
import pandas as pd
import time

tmdb.API_KEY = os.getenv("TMDB_APIKEY")
tmdb.REQUESTS_TIMEOUT = (2, 5)  # seconds, for connect and read specifically


def get_movies():
    discover = tmdb.Discover()

    movie_list = list()
    for year in range(1950, 2024):
        for page in range(1, 5):
            print(f"Processing year {year} - page {page}")
            time.sleep(1)
            resp = discover.movie(page=page, sort_by="vote_count.desc", vote_count_gte=500,
                                  primary_release_date_gte=f"{year}-01-01",
                                  primary_release_date_lte=f"{year}-12-31",
                                  )
            for movie in resp["results"]:
                movie_list.append(movie)

    df = pd.DataFrame(movie_list)
    df.to_csv("data/movies.csv", index=False)


def get_reviews():
    df = pd.read_csv("data/movies.csv")
    df = df.sort_values(by="vote_count")
    df = df.reset_index(drop=True)

    all_reviews = list()
    review_prefix = "data/reviews_"
    res_files = glob.glob("data/reviews_*.json")
    if len(res_files) != 0:
        last_index = int(res_files[0].split(".")[0][13:])
    else:
        last_index = 0
    # last_index = 3400
    for i, row in df[last_index:].iterrows():
        if i % 25 == 0:
            time.sleep(1)
        movie_id = row['id']
        movie = tmdb.Movies(movie_id)
        reviews = movie.reviews(page=1)
        all_reviews.extend(reviews["results"])
        if i % 100 == 0 and i != 0 and i != last_index:
            print(i)
            review_df = pd.DataFrame(all_reviews)
            review_df["movie_id"] = movie_id
            review_df.to_json(review_prefix + f"{i}.json", index=False)
            all_reviews = list()
    review_df = pd.DataFrame(all_reviews)
    review_df["movie_id"] = movie_id
    review_df.to_json(review_prefix + f"{i}.json", index=False)


def main():
    # get_movies()
    get_reviews()


if __name__ == "__main__":
    main()
