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
        '%Y-%m-%dT%H:%M:%SZ',
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

def prepare_video_document(doc):
    """Prepare video document for Elasticsearch"""
    try:
        doc = {k: convert_to_serializable(v) for k, v in doc.items()}
        
        # Format timestamps
        for field in ['timestamp', 'window_start', 'window_end', 'publish_date']:
            if isinstance(doc.get(field), str):
                formatted_timestamp = format_timestamp(doc[field])
                if formatted_timestamp:
                    doc[field] = formatted_timestamp

        if '_id' in doc:
            del doc['_id']

        # Ensure all required fields are present
        required_fields = {
            'video_id': str,
            'title': str,
            'channel_id': str,
            'channel_title': str,
            'publish_date': str,
            'views_growth_rate': float,
            'likes_growth_rate': float,
            'comments_growth_rate': float,
            'total_views': int,
            'total_likes': int,
            'total_comments': int,
            'engagement_rate': float,
            'views_per_hour': float,
            'likes_per_hour': float,
            'comments_per_hour': float,
            'window_start': str,
            'window_end': str,
            'timestamp': str
        }

        for field, field_type in required_fields.items():
            if field not in doc:
                return None
            try:
                doc[field] = field_type(doc[field])
            except (ValueError, TypeError):
                return None

        return doc
    except Exception as e:
        logger.error(f"Error preparing video document: {str(e)}")
        return None

def prepare_channel_document(doc):
    """Prepare channel document for Elasticsearch"""
    try:
        doc = {k: convert_to_serializable(v) for k, v in doc.items()}
        
        # Format timestamps
        for field in ['timestamp', 'window_start', 'window_end', 'publish_date']:
            if isinstance(doc.get(field), str):
                formatted_timestamp = format_timestamp(doc[field])
                if formatted_timestamp:
                    doc[field] = formatted_timestamp

        if '_id' in doc:
            del doc['_id']

        # Ensure all required fields are present
        required_fields = {
            'channel_id': str,
            'channel_title': str,
            'avg_engagement': float,
            'avg_likes': float,
            'avg_views': float,
            'avg_comments': float,
            'video_count': int,
            'window_start': str,
            'window_end': str,
            'timestamp': str,
            'publish_date': str
        }

        for field, field_type in required_fields.items():
            if field not in doc:
                return None
            try:
                doc[field] = field_type(doc[field])
            except (ValueError, TypeError):
                return None

        return doc
    except Exception as e:
        logger.error(f"Error preparing channel document: {str(e)}")
        return None

def get_missing_documents(mongo_collection, es_client, index_name, doc_preparer, batch_size=1000):
    """Find documents that exist in MongoDB but not in Elasticsearch"""
    missing_docs = []
    processed = 0
    total_docs = mongo_collection.count_documents({})

    for doc in mongo_collection.find().batch_size(batch_size):
        processed += 1
        if processed % 1000 == 0:
            logger.info(f"Processed {processed}/{total_docs} documents")

        prepared_doc = doc_preparer(doc)
        if not prepared_doc:
            continue

        doc_id = f"{prepared_doc['video_id']}_{prepared_doc['window_start']}" if 'video_id' in prepared_doc else f"{prepared_doc['channel_id']}_{prepared_doc['window_start']}"
        
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

        # Sync video analytics
        video_index = "video_analytics"
        if not es_client.indices.exists(index=video_index):
            es_client.indices.create(
                index=video_index,
                body={
                    "settings": {
                        "index": {
                            "number_of_shards": 1,
                            "number_of_replicas": 0
                        },
                        "analysis": {
                            "analyzer": {
                                "video_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "properties": {
                            "video_id": {"type": "keyword"},
                            "title": {
                                "type": "text",
                                "analyzer": "video_analyzer",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "channel_id": {"type": "keyword"},
                            "channel_title": {"type": "text"},
                            "publish_date": {"type": "date"},
                            "views_growth_rate": {"type": "float"},
                            "likes_growth_rate": {"type": "float"},
                            "comments_growth_rate": {"type": "float"},
                            "total_views": {"type": "long"},
                            "total_likes": {"type": "long"},
                            "total_comments": {"type": "long"},
                            "engagement_rate": {"type": "float"},
                            "views_per_hour": {"type": "float"},
                            "likes_per_hour": {"type": "float"},
                            "comments_per_hour": {"type": "float"},
                            "window_start": {"type": "date"},
                            "window_end": {"type": "date"},
                            "timestamp": {"type": "date"}
                        }
                    }
                }
            )

        # Sync channel analytics
        channel_index = "youtube_analytics"
        if not es_client.indices.exists(index=channel_index):
            es_client.indices.create(
                index=channel_index,
                body={
                    "settings": {
                        "index": {
                            "number_of_shards": 1,
                            "number_of_replicas": 0
                        },
                        "analysis": {
                            "analyzer": {
                                "channel_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "asciifolding"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "properties": {
                            "channel_id": {"type": "keyword"},
                            "channel_title": {
                                "type": "text",
                                "analyzer": "channel_analyzer",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
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
                    }
                }
            )

        # Sync video analytics
        video_stats_count = db.video_history.count_documents({})
        logger.info(f"Total video history documents in MongoDB: {video_stats_count}")

        video_es_count = es_client.count(index=video_index)['count']
        logger.info(f"Total documents in video_analytics index: {video_es_count}")

        if video_stats_count > 0:
            logger.info("Finding missing video documents...")
            missing_video_docs = get_missing_documents(db.video_history, es_client, video_index, prepare_video_document)
            
            if missing_video_docs:
                logger.info(f"Found {len(missing_video_docs)} missing video documents. Starting bulk index...")
                success, failed = bulk(es_client, missing_video_docs, raise_on_error=False)
                logger.info(f"Video bulk indexing completed. Success: {success}, Failed: {len(failed) if failed else 0}")
            else:
                logger.info("No missing video documents found")

        # Sync channel analytics
        channel_stats_count = db.channels.count_documents({})
        logger.info(f"Total channel documents in MongoDB: {channel_stats_count}")

        channel_es_count = es_client.count(index=channel_index)['count']
        logger.info(f"Total documents in youtube_analytics index: {channel_es_count}")

        if channel_stats_count > 0:
            logger.info("Finding missing channel documents...")
            missing_channel_docs = get_missing_documents(db.channels, es_client, channel_index, prepare_channel_document)
            
            if missing_channel_docs:
                logger.info(f"Found {len(missing_channel_docs)} missing channel documents. Starting bulk index...")
                success, failed = bulk(es_client, missing_channel_docs, raise_on_error=False)
                logger.info(f"Channel bulk indexing completed. Success: {success}, Failed: {len(failed) if failed else 0}")
            else:
                logger.info("No missing channel documents found")

        # Refresh indices
        es_client.indices.refresh(index=video_index)
        es_client.indices.refresh(index=channel_index)
        
        final_video_count = es_client.count(index=video_index)['count']
        final_channel_count = es_client.count(index=channel_index)['count']
        logger.info(f"Final document counts - Video analytics: {final_video_count}, Channel analytics: {final_channel_count}")

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