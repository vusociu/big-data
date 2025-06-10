from pymongo import MongoClient
from elasticsearch import Elasticsearch
import logging
from datetime import datetime, timedelta
import time
import json
from elasticsearch.helpers import bulk

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

def format_timestamp(timestamp_str):
    """Format timestamp string to standard format"""
    timestamp_formats = [
        '%Y-%m-%dT%H:%M:%SZ',  #
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%S', 
        '%Y-%m-%dT%H:%M:%S.%f' 
    ]
    
    for fmt in timestamp_formats:
        try:
            timestamp = datetime.strptime(timestamp_str, fmt)
            return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            continue
    return None

def prepare_document(doc):
    """Prepare MongoDB document for Elasticsearch"""
    doc = {k: convert_to_serializable(v) for k, v in doc.items()}
    
    if isinstance(doc.get('timestamp'), str):
        formatted_timestamp = format_timestamp(doc['timestamp'])
        if not formatted_timestamp:
            return None
        doc['timestamp'] = formatted_timestamp

    if '_id' in doc:
        del doc['_id']

    return doc

def get_missing_documents(mongo_collection, es_client, index_name, batch_size=1000):
    """Find documents that exist in MongoDB but not in Elasticsearch"""
    missing_docs = []
    processed = 0
    total_docs = mongo_collection.count_documents({})

    for doc in mongo_collection.find().batch_size(batch_size):
        processed += 1
        if processed % 1000 == 0:
            logger.info(f"Processed {processed}/{total_docs} documents")

        prepared_doc = prepare_document(doc)
        if not prepared_doc:
            continue

        doc_id = f"{prepared_doc['video_id']}_{prepared_doc['timestamp']}"
        
        try:
            if not es_client.exists(index=index_name, id=doc_id):
                missing_docs.append({
                    '_index': index_name,
                    '_id': doc_id,
                    '_source': prepared_doc
                })
        except Exception as e:
            logger.error(f"Error checking document {doc_id}: {str(e)}")

    return missing_docs

def sync_to_elasticsearch():
    """Sync missing data from MongoDB to Elasticsearch"""
    try:
        mongo_client = MongoClient("mongodb://localhost:27017/")
        db = mongo_client.youtube_analytics

        es_client = Elasticsearch(["http://localhost:9200"])

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
                            "comments": {"type": "long"},
                            "channel_info": {
                                "properties": {
                                    "id": {"type": "keyword"},
                                    "title": {"type": "text"},
                                    "subscriber_count": {"type": "long"},
                                    "video_count": {"type": "long"},
                                    "view_count": {"type": "long"}
                                }
                            }
                        }
                    }
                }
            )

        mongo_count = db.video_stats.count_documents({})
        logger.info(f"Total documents in MongoDB: {mongo_count}")

        es_count = es_client.count(index=index_name)['count']
        logger.info(f"Total documents in Elasticsearch: {es_count}")

        if mongo_count > es_count:
            logger.info("Finding missing documents...")
            missing_docs = get_missing_documents(db.video_stats, es_client, index_name)
            
            if missing_docs:
                logger.info(f"Found {len(missing_docs)} missing documents. Starting bulk index...")
                success, failed = bulk(es_client, missing_docs, raise_on_error=False)
                logger.info(f"Bulk indexing completed. Success: {success}, Failed: {len(failed) if failed else 0}")
            else:
                logger.info("No missing documents found")
        else:
            logger.info("No sync needed - Elasticsearch is up to date")

        es_client.indices.refresh(index=index_name)
        
        final_es_count = es_client.count(index=index_name)['count']
        logger.info(f"Final document count in Elasticsearch: {final_es_count}")

    except Exception as e:
        logger.error(f"Error in sync process: {str(e)}")
    finally:
        mongo_client.close()

if __name__ == "__main__":
    while True:
        try:
            sync_to_elasticsearch()
            logger.info("Waiting 60 seconds before next sync...")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in main loop: {str(e)}")
            time.sleep(60) 