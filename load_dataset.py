import pandas as pd
from pathlib import Path


movies_path = Path("data/movie_reviews_2000_2024_5_movies.json")
reviews_path = Path("data/movie_reviews_2000_2024_5_movie_reviews.json")

movies_df = pd.read_json(movies_path)
reviews_df = pd.read_json(reviews_path)


print(movies_df.info())
print(reviews_df.info())


for i, movie in movies_df[:2].iterrows():
    print(movie)
    reviews_fdf = reviews_df[reviews_df.id == movie.id]
    reviews = reviews_fdf.iloc[0]["results"]
    # print(review_refs.keys())
    for review in reviews:
        # review = reviews_df[reviews_df.id == review_ref["movie_id"]].iloc[0]
        # print(review["results"])
        print(review["content"][:20])
        print(review.keys())
        print(review["author_details"]["username"])


for i, review_row in reviews_df[:5].iterrows():
    review = review_row["results"]
    print(review)
