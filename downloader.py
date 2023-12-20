from typing import List, Dict
import tmdbsimple as tmdb
import glob
import os
import pandas as pd
import time
from pathlib import Path
import logging
import loggerconfig
import json
from tqdm import tqdm


API_KEY = os.getenv("TMDB_APIKEY")
REQUESTS_TIMEOUT = (2, 5)  # seconds, for connect and read specifically
output_dir = Path("./data")


def configure_tmdb():
    tmdb.API_KEY = os.getenv("TMDB_APIKEY")
    tmdb.REQUESTS_TIMEOUT = REQUESTS_TIMEOUT


def retrieve_topical_movies(start_year: int, end_year: int, pages: int) -> pd.DataFrame:
    output_fname = f"discover-movies-{start_year}-{end_year}-{pages}.csv"
    output_path = output_dir / output_fname

    if not output_path.exists():
        logging.info(f"Downloading data")
        discover = tmdb.Discover()
        all_movies = []
        for year in range(start_year, end_year + 1):
            for page in range(1, pages + 1):
                logging.info(f"Processing year {year} - page {page}")
                time.sleep(1)
                movies = discover.movie(
                    page=page,
                    sort_by="vote_count.desc",
                    vote_count_gte=500,
                    primary_release_date_gte=f"{year}-01-01",
                    primary_release_date_lte=f"{year}-12-31",
                )
                all_movies.extend(movies["results"])
        df = pd.DataFrame(all_movies)
        df.to_csv(output_path, index=False)
    else:
        logging.info(f"Found {output_fname}, loading...")
        df = pd.read_csv(output_path)

    return df


def save_reviews(reviews: List[Dict], movie_id: int, file_path: str):
    review_df = pd.DataFrame(reviews)
    review_df["movie_id"] = movie_id
    review_df.to_json(file_path, index=False)


def save_or_load_data(data_object: Dict, filename: str) -> Dict:
    outpath = output_dir / "raw" / filename
    if not outpath.exists():
        logging.debug(f"Saving {outpath}...")
        outpath.write_text(json.dumps(data_object))
    else:
        logging.debug(f"Loading {outpath}...")
        data_object = json.loads(outpath.read_text())
    return data_object


def main():
    configure_tmdb()
    start_year = 1980
    end_year = 2024
    movies_per_year = 10

    movie_df = retrieve_topical_movies(start_year, end_year, 5)

    fdf_list = list()
    for year in range(start_year, end_year + 1):
        fdf = movie_df[movie_df.release_date.str[:4] == str(year)]
        fdf = fdf.sort_values(by="vote_count", ascending=False).reset_index(drop=True)
        fdf = fdf[:movies_per_year]
        fdf_list.append(fdf)
    fdf = pd.concat(fdf_list)
    fdf.reset_index(drop=True, inplace=True)

    movie_info_list = list()
    movie_credits_list = list()
    movie_review_list = list()
    person_info_list = list()
    for i, movie_row in tqdm(fdf.iterrows()):
        if i % 5 == 0:
            time.sleep(1)
        movie_id = movie_row["id"]
        movie = tmdb.Movies(id=movie_id)
        logging.info(movie.info()["title"])

        movie_info = save_or_load_data(data_object=movie.info(), filename=f"movie_info_{movie_id}.json")
        movie_credits = save_or_load_data(data_object=movie.credits(), filename=f"movie_credits_{movie_id}.json")

        movie_info_list.append(movie_info)
        movie_credits_list.append(movie_credits)

        for page_no in [1, 2, 3]:
            movie_reviews = movie.reviews(page=page_no)
            if len(movie_reviews["results"]) > 0:
                logging.info(f"Saving review page {page_no} for movie {movie_id}")
                movie_reviews = save_or_load_data(data_object=movie_reviews, filename=f"movie_reviews_{movie_id}_page_{page_no}.json")
                movie_review_list.append(movie_reviews)
                for review in movie_reviews["results"]:
                    review_id = review["id"]
                    review["movie_id"] = movie_id
                    save_or_load_data(data_object=review, filename=f"review_body_{review_id}.json")
            else:
                logging.info(f"No reviews found for page: {page_no}")

        cast = movie_credits["cast"]
        if len(cast) > 0:
            for cast_member in cast:
                person = tmdb.People(id=cast_member["id"])
                person_id = cast_member["id"]
                person_info = person.info()
                person_info = save_or_load_data(data_object=person_info, filename=f"person_info_{person_id}.json")
                person_info_list.append(person_info)

    dataset_prefix = f"movie_reviews_{len(fdf)}"
    fdf.to_json(output_dir / (dataset_prefix + "_movies.json"))
    movie_info_output = output_dir / (dataset_prefix + "_movies_info.json")
    movie_info_output.write_text(json.dumps(movie_info_list))
    movie_review_output = output_dir / (dataset_prefix + "_movie_review_references.json")
    movie_review_output.write_text(json.dumps(movie_review_list))
    reviews_output = output_dir / (dataset_prefix + "_movie_reviews.json")
    reviews_output.write_text(json.dumps(movie_review_list))

    person_info_output = output_dir / (dataset_prefix + "_person_info.json")
    person_info_output.write_text(json.dumps(person_info_list))
    movie_credits_output = output_dir / (dataset_prefix + "_movie_credits_references.json")
    movie_credits_output.write_text(json.dumps(movie_credits_list))


if __name__ == "__main__":
    main()
