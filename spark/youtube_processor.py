import os
import sys
import logging
import findspark

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    logger.info("Initializing Spark...")
    findspark.init()
    logger.info("Spark initialized successfully")
except Exception as e:
    logger.error(f"Error initializing Spark: {str(e)}")
    raise

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pymongo import MongoClient
from datetime import datetime

# Set environment variables for Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Define schema for YouTube statistics
youtube_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channel_info", StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True)
    ]), True),
    StructField("published_at", StringType(), True),
    StructField("statistics", StructType([
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True)
    ]), True),
    StructField("timestamp", TimestampType(), True)
])

class YouTubeProcessor:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", mongo_uri="mongodb://localhost:27017/"):
        """
        Initialize Spark session and MongoDB client
        """
        try:
            logger.info("Setting up environment variables...")
            # Set environment variables
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
            
            logger.info("Creating Spark session...")
            # Initialize Spark with proper configuration
            self.spark = SparkSession.builder \
                .appName("YouTubeStatsProcessor") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "localhost") \
                .config("spark.ui.enabled", "false") \
                .config("spark.driver.extraJavaOptions", "-Xss4M") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .master("local[*]") \
                .getOrCreate()
            
            logger.info("Spark session created successfully")
            
            # Set log level
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info("Spark log level set to ERROR")

            logger.info("Initializing MongoDB client...")
            # Initialize MongoDB client
            self.mongo_client = MongoClient(mongo_uri)
            self.db = self.mongo_client.youtube_analytics
            self.kafka_bootstrap_servers = kafka_bootstrap_servers

            # Create indexes for better query performance
            self.db.video_stats.create_index([("timestamp", -1)])
            self.db.channel_stats.create_index([("channel_id", 1), ("window_start", -1)])
            self.db.channel_stats.create_index([("timestamp", -1)])
            logger.info("MongoDB client initialized successfully")
        
        except Exception as e:
            logger.error(f"Error in YouTubeProcessor initialization: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            raise

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

        # Calculate engagement metrics
        stats_df = parsed_df \
            .select(
                "video_id",
                "title",
                col("channel_info.id").alias("channel_id"),
                col("channel_info.title").alias("channel_title"),
                col("published_at").alias("publish_date"),
                col("statistics.view_count").alias("view_count"),
                col("statistics.like_count").alias("like_count"),
                "timestamp"
            ) \
            .withColumn("engagement_ratio", 
                        when(col("view_count") > 0, col("like_count") / col("view_count")).otherwise(0.0)) \
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