from pymongo import MongoClient
from elasticsearch import Elasticsearch
import logging
from datetime import datetime
import time
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def convert_to_serializable(obj):
    """Convert NumPy types to Python native types"""
    if hasattr(obj, 'item'):
        return obj.item()
    return obj

def sync_to_elasticsearch():
    # Connect to MongoDB
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client.youtube_analytics

    # Connect to Elasticsearch
    es_client = Elasticsearch(["http://localhost:9200"])

    # Create index if it doesn't exist
    index_name = "youtube_stats"
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "timestamp": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis"
                        },
                        "video_id": {"type": "keyword"},
                        "title": {"type": "text"},
                        "views": {"type": "long"},
                        "likes": {"type": "long"},
                        "comments": {"type": "long"}
                    }
                }
            }
        )

    # Get all documents from MongoDB
    docs = db.video_stats.find()
    total_docs = db.video_stats.count_documents({})
    logger.info(f"Found {total_docs} documents in MongoDB")

    # Index each document to Elasticsearch
    for doc in docs:
        # Convert NumPy types to Python native types
        doc = {k: convert_to_serializable(v) for k, v in doc.items()}
        
        # Convert timestamp to proper format
        if isinstance(doc.get('timestamp'), str):
            try:
                # Parse the timestamp string and convert to ISO format
                timestamp = datetime.strptime(doc['timestamp'], '%Y-%m-%dT%H:%M:%S.%f')
                doc['timestamp'] = timestamp.isoformat()
            except ValueError:
                logger.warning(f"Could not parse timestamp for video {doc.get('video_id')}")
                continue

        # Remove _id field as it's not serializable
        if '_id' in doc:
            del doc['_id']

        # Create document ID
        doc_id = f"{doc['video_id']}_{doc['timestamp']}"
        
        # Index document
        try:
            es_client.index(
                index=index_name,
                id=doc_id,
                body=doc
            )
            logger.info(f"Indexed video {doc['video_id']} to Elasticsearch")
        except Exception as e:
            logger.error(f"Error indexing video {doc['video_id']}: {str(e)}")

    # Get total documents in Elasticsearch
    es_client.indices.refresh(index=index_name)
    total_es_docs = es_client.count(index=index_name)['count']
    logger.info(f"Total documents in Elasticsearch: {total_es_docs}")
    logger.info("Sync completed")

if __name__ == "__main__":
    while True:
        try:
            sync_to_elasticsearch()
            time.sleep(60)  # Wait for 60 seconds before next sync
        except Exception as e:
            logger.error(f"Error in sync process: {str(e)}")
            time.sleep(60)  # Wait before retrying 