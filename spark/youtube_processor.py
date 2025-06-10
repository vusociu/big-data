import os
import sys
import logging
import findspark
from elasticsearch import Elasticsearch

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
from datetime import datetime
from dotenv import load_dotenv

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

youtube_schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("channel_info", StructType([
        StructField("id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("subscriber_count", IntegerType(), True),
        StructField("video_count", IntegerType(), True),
        StructField("view_count", IntegerType(), True)
    ]), True),
    StructField("published_at", StringType(), True),
    StructField("statistics", StructType([
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("favorite_count", IntegerType(), True)
    ]), True),
    StructField("timestamp", TimestampType(), True)
])

class YouTubeProcessor:
    def __init__(self, kafka_bootstrap_servers="localhost:9092", es_hosts=["http://localhost:9200"]):
        """
        Initialize Spark session and Elasticsearch client
        """
        try:
            logger.info("Setting up environment variables...")
            os.environ['PYSPARK_PYTHON'] = sys.executable
            os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
            
            logger.info("Creating Spark session...")
            self.spark = SparkSession.builder \
                .appName("YouTubeStatsProcessor") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
                .config("spark.driver.host", "localhost") \
                .config("spark.driver.bindAddress", "localhost") \
                .config("spark.ui.enabled", "false") \
                .config("spark.driver.extraJavaOptions", "-Xss4M") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .master("local[*]") \
                .getOrCreate()
            
            logger.info("Spark session created successfully")
            
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info("Spark log level set to ERROR")

            logger.info("Initializing Elasticsearch client...")
            self.es = Elasticsearch(es_hosts)
            self.kafka_bootstrap_servers = kafka_bootstrap_servers

            if not self.es.indices.exists(index="youtube_analytics"):
                self.es.indices.create(
                    index="youtube_analytics",
                    body={
                        "mappings": {
                            "properties": {
                                "channel_id": {"type": "keyword"},
                                "channel_title": {"type": "text"},
                                "avg_engagement": {"type": "float"},
                                "avg_likes": {"type": "float"},
                                "avg_views": {"type": "float"},
                                "avg_comments": {"type": "float"},
                                "video_count": {"type": "integer"},
                                "window_start": {"type": "date"},
                                "window_end": {"type": "date"},
                                "timestamp": {"type": "date"},
                                "publish_date": {"type": "date"}
                            }
                        },
                        "settings": {
                            "index": {
                                "number_of_shards": 1,
                                "number_of_replicas": 0
                            }
                        }
                    }
                )
            logger.info("Elasticsearch client initialized successfully")
        
        except Exception as e:
            logger.error(f"Error in YouTubeProcessor initialization: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            raise

    def process_youtube_stats(self):
        """
        Process YouTube statistics from Kafka and compute analytics
        """
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "youtube_stats_for_processing") \
            .load()

        parsed_df = df.select(
            from_json(col("value").cast("string"), youtube_schema).alias("data")
        ).select("data.*")

        stats_df = parsed_df \
            .select(
                "video_id",
                "title",
                col("channel_info.id").alias("channel_id"),
                col("channel_info.title").alias("channel_title"),
                col("published_at").alias("publish_date"),
                col("statistics.view_count").alias("view_count"),
                col("statistics.like_count").alias("like_count"),
                col("statistics.comment_count").alias("comment_count"),
                col("timestamp").alias("event_timestamp")
            ) \
            .withColumn("engagement_ratio", 
                        when(col("view_count") > 0, 
                             (col("like_count") + col("comment_count")) / col("view_count")
                        ).otherwise(0.0)) \
            .withWatermark("event_timestamp", "1 hour")

        hourly_stats = stats_df \
            .groupBy(
                window(col("event_timestamp"), "1 hour"),
                "channel_id",
                "channel_title",
                "publish_date"
            ) \
            .agg(
                avg("engagement_ratio").alias("avg_engagement"),
                avg("like_count").alias("avg_likes"),
                avg("view_count").alias("avg_views"),
                avg("comment_count").alias("avg_comments"),
                count("video_id").alias("video_count")
            )

        query = hourly_stats \
            .writeStream \
            .foreachBatch(self._write_to_elasticsearch) \
            .outputMode("update") \
            .start()

        query.awaitTermination()

    def _write_to_elasticsearch(self, batch_df, batch_id):
        """
        Write aggregated statistics to Elasticsearch
        """
        if batch_df.isEmpty():
            return

        pandas_df = batch_df.toPandas()
        
        for _, row in pandas_df.iterrows():
            doc = {
                "channel_id": row["channel_id"],
                "channel_title": row["channel_title"],
                "avg_engagement": float(row["avg_engagement"]),
                "avg_likes": float(row["avg_likes"]),
                "avg_views": float(row["avg_views"]),
                "avg_comments": float(row["avg_comments"]),
                "video_count": int(row["video_count"]),
                "window_start": row["window"]["start"].strftime('%Y-%m-%dT%H:%M:%SZ'),
                "window_end": row["window"]["end"].strftime('%Y-%m-%dT%H:%M:%SZ'),
                "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "publish_date": row["publish_date"]
            }
            try:
                self.es.index(
                    index="youtube_analytics",
                    document=doc,
                    id=f"{doc['channel_id']}_{doc['window_start']}"
                )
            except Exception as e:
                logger.error(f"Error indexing to Elasticsearch: {str(e)}")

if __name__ == "__main__":
    load_dotenv()
    processor = YouTubeProcessor()
    processor.process_youtube_stats() 