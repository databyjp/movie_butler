import pandas as pd
from pathlib import Path
import weaviate
import weaviate.classes as wvc
from weaviate.util import generate_uuid5
from datetime import datetime, timezone


movies_path = Path("data/movie_reviews_2000_2024_5_movies.json")
reviews_path = Path("data/movie_reviews_2000_2024_5_movie_reviews.json")

movies_df = pd.read_json(movies_path)
reviews_df = pd.read_json(reviews_path)


client = weaviate.connect_to_local()

client.collections.delete(["Movie", "Review"])

movies = client.collections.create(
    name="Movie",
    vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
    generative_config=wvc.config.Configure.Generative.openai(),
    vector_index_config=wvc.config.Configure.VectorIndex.hnsw(),
    properties=[
        wvc.Property(
            name="title",
            data_type=wvc.DataType.TEXT,
        ),
        wvc.Property(
            name="overview",
            data_type=wvc.DataType.TEXT,
        ),
        wvc.Property(
            name="movie_id",
            data_type=wvc.DataType.INT,
            skip_vectorization=True,
        ),
        wvc.Property(
            name="release_date",
            data_type=wvc.DataType.DATE,
        ),
        wvc.Property(
            name="vote_count",
            data_type=wvc.DataType.INT,
        ),
    ]
)

reviews = client.collections.create(
    name="Review",
    vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_transformers(),
    generative_config=wvc.config.Configure.Generative.openai(),
    vector_index_config=wvc.config.Configure.VectorIndex.hnsw(),
    inverted_index_config=wvc.config.Configure.inverted_index(index_null_state=True),
    properties=[
        wvc.Property(
            name="author_username",
            data_type=wvc.DataType.TEXT,
            skip_vectorization=True,
        ),
        wvc.Property(
            name="content",
            data_type=wvc.DataType.TEXT,
        ),
        wvc.Property(
            name="rating",
            data_type=wvc.DataType.INT,
        ),
        wvc.Property(
            name="review_id",
            data_type=wvc.DataType.TEXT,
            skip_vectorization=True,
        ),
        wvc.Property(
            name="movie_id",
            data_type=wvc.DataType.INT,
        ),
    ],
    references=[
        wvc.ReferenceProperty(
            name="forMovie",
            target_collection="Movie"
        )
    ]
)

dataobj_list = list()
for i, movie_row in movies_df.iterrows():
    props = {
        k: movie_row[k] for k in ["title", "overview", "vote_count"]
    }
    props["movie_id"] = movie_row["id"]

    date_object = datetime.strptime(movie_row["release_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    props["release_date"] = date_object

    dataobj = wvc.data.DataObject(
        properties=props,
        uuid=generate_uuid5(movie_row["id"])
    )
    dataobj_list.append(dataobj)

movies.data.insert_many(dataobj_list)


for i, review_row in reviews_df.iterrows():
    for review in review_row["results"]:
        if review["author_details"]["rating"] is not None:
            rating = int(review["author_details"]["rating"])
        props = {
            "author_username": review["author_details"]["username"],
            "content": review["content"],
            "rating": rating,
            "movie_id": review["movie_id"],
            "review_id": review["id"],
        }
        movie_matches = movies.query.fetch_objects(
            filters=wvc.Filter("movie_id").equal(review["movie_id"])
        )
        assert len(movie_matches.objects) == 1
        movie_uuid = movie_matches.objects[0].uuid
        refs = {
            "forMovie": wvc.data.Reference.to(movie_uuid)
        }
        review_id = reviews.data.insert(properties=props, uuid=generate_uuid5(review["id"]), references=refs)
