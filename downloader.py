from typing import List, Dict
import tmdbsimple as tmdb
import glob
import os
import pandas as pd
import time

API_KEY = os.getenv("TMDB_APIKEY")
REQUESTS_TIMEOUT = (2, 5)  # seconds, for connect and read specifically


def configure_tmdb():
    tmdb.API_KEY = os.getenv("API_KEY")
    tmdb.REQUESTS_TIMEOUT = REQUESTS_TIMEOUT


def retrieve_movies(start_year: int, end_year: int, pages: int) -> pd.DataFrame:
    discover = tmdb.Discover()
    all_movies = []
    for year in range(start_year, end_year + 1):
        for page in range(1, pages + 1):
            print(f"Processing year {year} - page {page}")
            time.sleep(1)
            movies = discover.movie(
                page=page,
                sort_by="vote_count.desc",
                vote_count_gte=500,
                primary_release_date_gte=f"{year}-01-01",
                primary_release_date_lte=f"{year}-12-31",
            )
            all_movies.extend(movies["results"])
    return pd.DataFrame(all_movies)


def save_movies_to_csv(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)


def get_existing_review_files(review_folder: str) -> List[str]:
    return glob.glob(review_folder)


def get_last_index(review_files: List[str]) -> int:
    return int(review_files[0].split(".")[0][13:]) if review_files else 0


def extract_reviews_from_movies(
    df: pd.DataFrame, last_index: int, review_prefix: str, sleep_duration: int
):
    all_reviews = list()
    for i, row in df[last_index:].iterrows():
        if i % 25 == 0:
            time.sleep(sleep_duration)
        movie_id = row["id"]
        movie = tmdb.Movies(movie_id)
        reviews = movie.reviews(page=1)
        all_reviews.extend(reviews["results"])
        if i % 100 == 0 and i != 0 and i != last_index:
            print(i)
            save_reviews(all_reviews, movie_id, review_prefix + f"{i}.json")
            all_reviews = list()
    save_reviews(all_reviews, movie_id, review_prefix + f"{i}.json")


def load_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    return df.sort_values(by="vote_count").reset_index(drop=True)


def save_reviews(reviews: List[Dict], movie_id: int, file_path: str):
    review_df = pd.DataFrame(reviews)
    review_df["movie_id"] = movie_id
    review_df.to_json(file_path, index=False)


def main():
    configure_tmdb()
    movie_df = retrieve_movies(1950, 2024, 5)
    save_movies_to_csv(movie_df, "data/movies.csv")
    review_df = load_data("data/movies.csv")
    review_files = get_existing_review_files("data/reviews_*.json")
    last_index = get_last_index(review_files)
    extract_reviews_from_movies(review_df, last_index, "data/reviews_", 1)


if __name__ == "__main__":
    main()
