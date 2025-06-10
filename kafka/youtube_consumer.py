import json
from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], mongo_uri=None):
        """
        Initialize Kafka consumer and MongoDB connection
        """
        if mongo_uri is None:
            mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')

        self.consumer = KafkaConsumer(
            'youtube_raw_stats',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='youtube_raw_stats_group'
        )

        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client.youtube_analytics

        self._ensure_indexes()

    def _ensure_indexes(self):
        """
        Create necessary indexes in MongoDB
        """
        self.db.videos.create_index([("video_id", 1)], unique=True)
        self.db.videos.create_index([("timestamp", -1)])
        self.db.videos.create_index([("channel_info.id", 1)])
        self.db.videos.create_index([("statistics.view_count", -1)])

        self.db.channels.create_index([("channel_id", 1)], unique=True)
        self.db.channels.create_index([("subscriber_count", -1)])

        self.db.video_history.create_index([
            ("video_id", 1),
            ("timestamp", -1)
        ])

    def process_message(self, message):
        """
        Process a message from Kafka and save to MongoDB
        """
        try:
            data = message.value
            
            self._update_video(data)
            
            self._save_video_history(data)
            
            self._update_channel(data)
            
            logger.info(f"Processed data for video: {data['video_id']}")
            
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    def _update_video(self, data):
        """
        Update or insert video document
        """
        try:
            video_doc = {
                "video_id": data["video_id"],
                "url": data["url"],
                "title": data["title"],
                "description": data["description"],
                "published_at": data["published_at"],
                "channel_info": data["channel_info"],
                "thumbnails": data["thumbnails"],
                "category_id": data["category_id"],
                "tags": data["tags"],
                "duration_seconds": data["duration_seconds"],
                "dimension": data["dimension"],
                "definition": data["definition"],
                "caption": data["caption"],
                "licensed_content": data["licensed_content"],
                "projection": data["projection"],
                "privacy_status": data["privacy_status"],
                "license": data["license"],
                "embeddable": data["embeddable"],
                "topic_categories": data["topic_categories"],
                "statistics": data["statistics"],
                "last_updated": data["timestamp"]
            }

            self.db.videos.update_one(
                {"video_id": data["video_id"]},
                {"$set": video_doc},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Error updating video: {str(e)}")

    def _save_video_history(self, data):
        """
        Save historical video statistics
        """
        try:
            history_doc = {
                "video_id": data["video_id"],
                "timestamp": data["timestamp"],
                "statistics": data["statistics"]
            }

            self.db.video_history.insert_one(history_doc)

        except Exception as e:
            logger.error(f"Error saving video history: {str(e)}")

    def _update_channel(self, data):
        """
        Update channel statistics
        """
        try:
            channel_info = data["channel_info"]
            
            channel_doc = {
                "channel_id": channel_info["id"],
                "title": channel_info["title"],
                "subscriber_count": channel_info["subscriber_count"],
                "video_count": channel_info["video_count"],
                "view_count": channel_info["view_count"],
                "last_updated": data["timestamp"]
            }

            self.db.channels.update_one(
                {"channel_id": channel_info["id"]},
                {"$set": channel_doc},
                upsert=True
            )

        except Exception as e:
            logger.error(f"Error updating channel: {str(e)}")

    def run(self):
        """
        Run the consumer
        """
        try:
            logger.info("Starting YouTube stats consumer...")
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
        finally:
            self.consumer.close()
            self.mongo_client.close()

if __name__ == "__main__":
    load_dotenv()
    consumer = YouTubeConsumer()
    consumer.run() 