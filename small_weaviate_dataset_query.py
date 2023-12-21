import weaviate
import weaviate.classes as wvc
from weaviate.util import generate_uuid5
from datetime import datetime, timezone


client = weaviate.connect_to_local()


movies = client.collections.get("Movie")
reviews = client.collections.get("Review")


response = movies.query.near_text("A swashbuckling scifi adventure", limit=2)
for r in response.objects:
    print(r.properties)


response = reviews.query.near_text("Fun for the whole family", limit=2)
for r in response.objects:
    print(r.properties)


response = reviews.query.near_text(
    "Disappointed by this movie",
    limit=2,
    return_references=wvc.query.QueryReference(
        link_on="forMovie",
        return_properties=["title"]
    )
)

for r in response.objects:
    for k, v in r.references.items():
        print(v.objects[0].properties["title"])
    print(r.properties)




