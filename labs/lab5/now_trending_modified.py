from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("MusicEventConsumer") \
    .config("spark.sql.streaming.trigger.interval", "5 seconds") \
    .getOrCreate()

# Read from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "music_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value from JSON
df_parsed = df.selectExpr("CAST(value AS STRING) as json_value") \
    .selectExpr("from_json(json_value, 'song_id INT, timestamp DOUBLE, region STRING, action STRING') as data") \
    .select("data.*")

# Count total plays and skips per song
song_counts = df_parsed.groupBy("song_id").agg(
    count(when(col("action") == "play", True)).alias("total_plays"),
    count(when(col("action") == "skip", True)).alias("total_skips")
)

# Compute skip ratio (handle divide-by-zero)
song_counts = song_counts.withColumn(
    "skip_ratio", 
    expr("total_skips / NULLIF(total_plays + total_skips, 0)")
)

# Output results to console
query = song_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
