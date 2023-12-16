import pandas as pd
import glob

movies_df = pd.read_csv("data/movies.csv")

review_files = glob.glob("data/reviews_*.json")
review_dfs = list()
for review_file in review_files:
    temp_df = pd.read_json(review_file)
    review_dfs.append(temp_df)
reviews_df = pd.concat(review_dfs)
reviews_df["rating"] = reviews_df["author_details"].apply(lambda x: x["rating"])
reviews_df["author"] = reviews_df["author_details"].apply(lambda x: x["username"])


authors = list()
for i, row in reviews_df.iterrows():
    author = row["author_details"]
    del author["rating"]
    authors.append(author)

authors_df = pd.DataFrame(authors)
authors_df = authors_df.drop_duplicates()


reviews_df = reviews_df.drop(columns=["author_details"], axis=1)
reviews_df = reviews_df.reset_index(drop=True)
reviews_df.to_json("data/reviews.json")
authors_df.to_json("data/authors.json")
