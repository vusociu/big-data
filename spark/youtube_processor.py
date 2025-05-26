from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
from pymongo import MongoClient
from datetime import datetime

# Define schema for YouTube statistics
youtube_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channel_id", StringType(), True),
    StructField("channel_title", StringType(), True),
    StructField("publish_date", StringType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

class YouTubeProcessor:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", mongo_uri="mongodb://localhost:27017/"):
        """
        Initialize Spark session and MongoDB client
        """
        self.spark = SparkSession.builder \
            .appName("YouTubeStatsProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()

        # Initialize MongoDB client
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.youtube_analytics
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        # Create indexes for better query performance
        self.db.video_stats.create_index([("timestamp", -1)])
        self.db.channel_stats.create_index([("channel_id", 1), ("window_start", -1)])
        self.db.channel_stats.create_index([("timestamp", -1)])

    def process_youtube_stats(self):
        """
        Process YouTube statistics from Kafka and compute analytics
        """
        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "youtube_stats") \
            .load()

        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), youtube_schema).alias("data")
        ).select("data.*")

        # Calculate engagement metrics (now based only on likes)
        stats_df = parsed_df \
            .withColumn("engagement_ratio", 
                       col("like_count") / col("view_count")) \
            .withWatermark("timestamp", "1 hour")

        # Save raw video stats
        stats_df.writeStream \
            .foreachBatch(self._write_video_stats_to_mongo) \
            .outputMode("append") \
            .start()

        # Compute hourly aggregations
        hourly_stats = stats_df \
            .groupBy(
                window("timestamp", "1 hour"),
                "channel_id",
                "channel_title"
            ) \
            .agg(
                avg("engagement_ratio").alias("avg_engagement"),
                avg("like_count").alias("avg_likes"),
                avg("view_count").alias("avg_views"),
                count("video_id").alias("video_count")
            )

        # Write aggregated stats to MongoDB
        query = hourly_stats \
            .writeStream \
            .foreachBatch(self._write_channel_stats_to_mongo) \
            .outputMode("update") \
            .start()

        query.awaitTermination()

    def _write_video_stats_to_mongo(self, batch_df, batch_id):
        """
        Write raw video statistics to MongoDB
        """
        if batch_df.isEmpty():
            return

        # Convert batch to pandas for easier processing
        pandas_df = batch_df.toPandas()
        
        # Prepare documents for MongoDB
        documents = []
        for _, row in pandas_df.iterrows():
            doc = row.to_dict()
            doc['timestamp'] = doc['timestamp'].isoformat()
            documents.append(doc)
        
        # Insert documents
        if documents:
            self.db.video_stats.insert_many(documents)

    def _write_channel_stats_to_mongo(self, batch_df, batch_id):
        """
        Write aggregated channel statistics to MongoDB
        """
        if batch_df.isEmpty():
            return

        # Convert batch to pandas for easier processing
        pandas_df = batch_df.toPandas()
        
        # Prepare documents for MongoDB
        for _, row in pandas_df.iterrows():
            doc = {
                "channel_id": row["channel_id"],
                "channel_title": row["channel_title"],
                "avg_engagement": float(row["avg_engagement"]),
                "avg_likes": float(row["avg_likes"]),
                "avg_views": float(row["avg_views"]),
                "video_count": int(row["video_count"]),
                "window_start": row["window"]["start"].isoformat(),
                "window_end": row["window"]["end"].isoformat(),
                "timestamp": row["window"]["end"].isoformat()
            }
            
            self.db.channel_stats.update_one(
                {
                    "channel_id": doc["channel_id"],
                    "window_start": doc["window_start"]
                },
                {"$set": doc},
                upsert=True
            )

if __name__ == "__main__":
    processor = YouTubeProcessor()
    processor.process_youtube_stats() 