import json
import time
from datetime import datetime, timedelta
import os
from googleapiclient.discovery import build
from kafka import KafkaProducer
import logging
import isodate

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YouTubeProducer:
    def __init__(self, api_key, bootstrap_servers=['localhost:9092']):
        """
        Initialize YouTube producer with API key and Kafka configuration
        """
        self.youtube = build('youtube', 'v3', developerKey=api_key)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def get_video_details(self, video_id):
        """
        Get detailed information for a specific video
        """
        try:
            # Get video details including content details and statistics
            video_request = self.youtube.videos().list(
                part="snippet,contentDetails,statistics,topicDetails,status",
                id=video_id
            )
            video_response = video_request.execute()

            if not video_response['items']:
                logger.warning(f"No data found for video ID: {video_id}")
                return None

            video_data = video_response['items'][0]
            snippet = video_data['snippet']
            statistics = video_data.get('statistics', {})
            content_details = video_data.get('contentDetails', {})
            topic_details = video_data.get('topicDetails', {})
            status = video_data.get('status', {})

            # Get channel details
            channel_id = snippet['channelId']
            channel_request = self.youtube.channels().list(
                part="snippet,statistics",
                id=channel_id
            )
            channel_response = channel_request.execute()
            channel_data = channel_response['items'][0] if channel_response['items'] else {}
            channel_stats = channel_data.get('statistics', {})

            # Convert duration to seconds
            duration = content_details.get('duration', 'PT0S')
            duration_seconds = int(isodate.parse_duration(duration).total_seconds())

            # Compile all video information
            video_info = {
                'video_id': video_id,
                'url': f"https://www.youtube.com/watch?v={video_id}",
                'title': snippet['title'],
                'description': snippet['description'],
                'published_at': snippet['publishedAt'],
                'channel_info': {
                    'id': channel_id,
                    'title': snippet['channelTitle'],
                    'subscriber_count': int(channel_stats.get('subscriberCount', 0)),
                    'video_count': int(channel_stats.get('videoCount', 0)),
                    'view_count': int(channel_stats.get('viewCount', 0))
                },
                'thumbnails': snippet.get('thumbnails', {}),
                'category_id': snippet.get('categoryId'),
                'tags': snippet.get('tags', []),
                'duration_seconds': duration_seconds,
                'dimension': content_details.get('dimension'),
                'definition': content_details.get('definition'),
                'caption': content_details.get('caption') == 'true',
                'licensed_content': content_details.get('licensedContent', False),
                'projection': content_details.get('projection'),
                'privacy_status': status.get('privacyStatus'),
                'license': status.get('license'),
                'embeddable': status.get('embeddable', False),
                'topic_categories': topic_details.get('topicCategories', []),
                'statistics': {
                    'view_count': int(statistics.get('viewCount', 0)),
                    'like_count': int(statistics.get('likeCount', 0)),
                    'favorite_count': int(statistics.get('favoriteCount', 0))
                },
                'timestamp': datetime.now().isoformat()
            }

            # Send to Kafka topic
            self.producer.send('youtube_stats', value=video_info)
            logger.info(f"Sent detailed statistics for video: {video_id}")
            return video_info

        except Exception as e:
            logger.error(f"Error collecting data for video {video_id}: {str(e)}")
            return None

    def search_videos(self, query, max_results=50, published_after=None):
        """
        Search for videos based on a query
        """
        try:
            search_params = {
                'part': 'id',
                'q': query,
                'type': 'video',
                'maxResults': max_results,
                'order': 'date'  # Get most recent videos
            }
            
            if published_after:
                search_params['publishedAfter'] = published_after

            request = self.youtube.search().list(**search_params)
            response = request.execute()

            video_ids = [item['id']['videoId'] for item in response.get('items', [])]
            return video_ids

        except Exception as e:
            logger.error(f"Error searching videos: {str(e)}")
            return []

    def run(self, queries=None, video_ids=None, interval=3600):
        """
        Run continuous data collection
        """
        if not queries and not video_ids:
            raise ValueError("Either queries or video_ids must be provided")

        while True:
            try:
                # Process specific video IDs
                if video_ids:
                    for video_id in video_ids:
                        self.get_video_details(video_id)
                        time.sleep(1)  # Respect API quota

                # Process search queries
                if queries:
                    # Get videos published in the last day
                    published_after = (
                        datetime.utcnow() - timedelta(days=1)
                    ).isoformat() + 'Z'
                    
                    for query in queries:
                        found_videos = self.search_videos(
                            query, 
                            published_after=published_after
                        )
                        for video_id in found_videos:
                            self.get_video_details(video_id)
                            time.sleep(1)  # Respect API quota

                logger.info(f"Sleeping for {interval} seconds before next collection cycle")
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error in collection cycle: {str(e)}")
                time.sleep(60)  # Wait before retrying

if __name__ == "__main__":
    load_dotenv()
    # Get API key from environment variable
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable is required")

    producer = YouTubeProducer(api_key=api_key)
    
    # Example: Monitor specific videos
    video_ids = [
        'dQw4w9WgXcQ',  # Example video ID
        # Add more video IDs here
    ]
    
    # Example: Search queries
    search_queries = [
        'Python programming',
        'Data Science',
        'Machine Learning',
        'C++ Programming',
        'Remix'
    ]
    
    # Run producer with either specific videos or search queries
    producer.run(video_ids=video_ids, queries=search_queries) 