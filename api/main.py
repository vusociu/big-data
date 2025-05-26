from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from typing import List, Optional
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Shopee Analytics API")

# Database connections
mongo_client = MongoClient("mongodb://mongodb:27017/")
es_client = Elasticsearch(["http://elasticsearch:9200"])