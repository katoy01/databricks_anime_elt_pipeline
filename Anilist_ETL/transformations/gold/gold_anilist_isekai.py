# Gold: Isekai anime (TV/ONA/OVA) with score grades.
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_anilist_isekai",
    comment="Isekai anime in TV/ONA/OVA formats with a grade derived from averageScore.",
)
def gold_anilist_isekai():
    silver = spark.read.table("silver_anilist_isekai")
    return (
        silver.select(
            "media_id",
            "title_english",
            "title_romaji",
            "format",
            "season",
            "season_year",
            "status",
            "average_score",
            F.expr(
                """
                CASE
                    WHEN average_score >= 85 THEN 'High'
                    WHEN average_score >= 80 THEN 'Good'
                    WHEN average_score >= 70 THEN 'Average'
                    WHEN average_score >= 60 THEN 'Not Good'
                    WHEN average_score >= 50 THEN 'Bad'
                    ELSE 'Very Bad'
                END
                """
            ).alias("grade"),
        )
        .where(F.col("format").isin("TV", "ONA", "OVA"))
    )
