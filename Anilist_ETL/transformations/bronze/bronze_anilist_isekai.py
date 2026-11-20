# Bronze: raw AniList Isekai anime snapshot, fetched from the AniList GraphQL API on every refresh.
import time

import requests
from pyspark import pipelines as dp

ANILIST_URL = "https://graphql.anilist.co"
ANILIST_QUERY = """
query ($page: Int) {
  Page(page: $page, perPage: 50) {
    pageInfo { hasNextPage }
    media(type: ANIME, tag: "Isekai", sort: POPULARITY_DESC) {
      id
      title { english romaji }
      type
      format
      tags { name }
      status
      startDate { year month day }
      endDate { year month day }
      seasonYear
      season
      averageScore
      popularity
    }
  }
}
"""


def _fetch_isekai_media():
    """Page through the AniList GraphQL API and return all Isekai media dicts."""
    rows, page = [], 1
    while True:
        response = requests.post(
            ANILIST_URL, json={"query": ANILIST_QUERY, "variables": {"page": page}}
        )
        response.raise_for_status()
        data = response.json()["data"]["Page"]
        rows.extend(data["media"])
        if not data["pageInfo"]["hasNextPage"]:
            break
        page += 1
        time.sleep(1)  # respect the AniList rate limit (90 req/min)
    return rows


@dp.materialized_view(
    name="bronze_anilist_isekai",
    comment="Raw AniList Isekai anime snapshot fetched from the GraphQL API (full re-fetch each refresh).",
)
def bronze_anilist_isekai():
    rows = _fetch_isekai_media()
    return spark.createDataFrame(rows)
