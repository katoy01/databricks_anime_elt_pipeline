# Silver: cleaned and flattened AniList Isekai anime.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="silver_anilist_isekai",
    comment="Cleaned and flattened AniList Isekai anime (one row per media id).",
)
def silver_anilist_isekai():
    bronze = spark.read.table("bronze_anilist_isekai")
    return (
        bronze.select(
            F.col("id").cast("long").alias("media_id"),
            F.col("title.english").alias("title_english"),
            F.col("title.romaji").alias("title_romaji"),
            F.col("type"),
            F.col("format"),
            F.col("status"),
            F.col("season"),
            F.col("seasonYear").cast("int").alias("season_year"),
            F.col("averageScore").cast("int").alias("average_score"),
            F.col("popularity").cast("long").alias("popularity"),
            F.transform("tags", lambda t: t["name"]).alias("tag_names"),
            F.make_date(
                F.col("startDate.year").cast("int"),
                F.col("startDate.month").cast("int"),
                F.col("startDate.day").cast("int"),
            ).alias("start_date"),
            F.make_date(
                F.col("endDate.year").cast("int"),
                F.col("endDate.month").cast("int"),
                F.col("endDate.day").cast("int"),
            ).alias("end_date"),
        )
        .dropDuplicates(["media_id"])
    )
