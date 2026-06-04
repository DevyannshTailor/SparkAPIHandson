from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load datasets
logs  = spark.read.option("header", "true").option("inferSchema", "true").csv("listening_logs.csv")
songs = spark.read.option("header", "true").option("inferSchema", "true").csv("songs_metadata.csv")

enriched = logs.join(songs, on="song_id", how="inner")

# Task 1: User Favorite Genres
genre_counts = (
    enriched
    .groupBy("user_id", "genre")
    .agg(count("*").alias("listen_count"))
)

user_window = Window.partitionBy("user_id").orderBy(desc("listen_count"))

user_favorite_genres = (
    genre_counts
    .withColumn("rank", rank().over(user_window))
    .filter(col("rank") == 1)
    .drop("rank")
    .orderBy("user_id")
)

user_favorite_genres.show(10, truncate=False)
user_favorite_genres.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/user_favorite_genres")

# Task 2: Average Listen Time
avg_listen_time = (
    logs
    .groupBy("user_id")
    .agg(round(avg("duration_sec"), 2).alias("avg_listen_time_sec"))
    .orderBy("user_id")
)

avg_listen_time.show(10, truncate=False)
avg_listen_time.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/avg_listen_time")

# Task 3: Create your own Genre Loyalty Scores and rank them and list out top 10
total_listens = (
    logs
    .groupBy("user_id")
    .agg(count("*").alias("total_listens"))
)

top_genre_counts = (
    genre_counts
    .withColumn("rank", rank().over(user_window))
    .filter(col("rank") == 1)
    .select("user_id", col("listen_count").alias("top_genre_listens"), "genre")
)

genre_loyalty_top10 = (
    top_genre_counts
    .join(total_listens, on="user_id")
    .withColumn("genre_loyalty_score", round((col("top_genre_listens") / col("total_listens")) * 100, 2))
    .select("user_id", "genre", "top_genre_listens", "total_listens", "genre_loyalty_score")
    .orderBy(desc("genre_loyalty_score"))
    .limit(10)
)

genre_loyalty_top10.show(truncate=False)
genre_loyalty_top10.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/genre_loyalty_top10")

# Task 4: Identify users who listen between 12 AM and 5 AM
night_owl_users = (
    enriched
    .withColumn("hour", hour(col("timestamp")))
    .filter((col("hour") >= 0) & (col("hour") < 5))
    .select("user_id")
    .distinct()
    .orderBy("user_id")
)

night_owl_users.show(truncate=False)
night_owl_users.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/night_owl_users")

spark.stop()