import json
import time
from datetime import datetime, timedelta
import os
from googleapiclient.discovery import build
from kafka import KafkaProducer
import logging
import isodate

from dotenv import load_dotenv

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


            channel_id = snippet['channelId']
            channel_request = self.youtube.channels().list(
                part="snippet,statistics",
                id=channel_id
            )
            channel_response = channel_request.execute()
            channel_data = channel_response['items'][0] if channel_response['items'] else {}
            channel_stats = channel_data.get('statistics', {})


            duration = content_details.get('duration', 'PT0S')
            duration_seconds = int(isodate.parse_duration(duration).total_seconds())


            video_info = {
                'video_id': video_id,
                'title': snippet['title'],
                'description': snippet.get('description', ''),
                'url': f'https://www.youtube.com/watch?v={video_id}',
                'channel_info': {
                    'id': channel_id,
                    'title': snippet['channelTitle'],
                    'subscriber_count': int(channel_stats.get('subscriberCount', 0)),
                    'video_count': int(channel_stats.get('videoCount', 0)),
                    'view_count': int(channel_stats.get('viewCount', 0))
                },
                'published_at': snippet['publishedAt'],
                'thumbnails': snippet.get('thumbnails', {}),
                'category_id': snippet.get('categoryId', ''),
                'tags': snippet.get('tags', []),
                'duration_seconds': duration_seconds,
                'dimension': content_details.get('dimension', ''),
                'definition': content_details.get('definition', ''),
                'caption': content_details.get('caption', ''),
                'licensed_content': content_details.get('licensedContent', False),
                'projection': content_details.get('projection', ''),
                'privacy_status': status.get('privacyStatus', ''),
                'license': status.get('license', ''),
                'embeddable': status.get('embeddable', True),
                'topic_categories': topic_details.get('topicCategories', []),
                'statistics': {
                    'view_count': int(statistics.get('viewCount', 0)),
                    'like_count': int(statistics.get('likeCount', 0)),
                    'comment_count': int(statistics.get('commentCount', 0)),
                    'favorite_count': int(statistics.get('favoriteCount', 0))
                },
                'timestamp': datetime.now().isoformat(),
            }


            self.producer.send('youtube_raw_stats', value=video_info)
            self.producer.send('youtube_stats_for_processing', value=video_info)
            logger.info(f"Sent statistics for video {video_id} to both topics")
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
                'order': 'date'
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

    def run(self, queries=None, video_ids=None, interval=300):
        """
        Run continuous data collection
        """
        if not queries and not video_ids:
            raise ValueError("Either queries or video_ids must be provided")

        while True:
            try:

                if video_ids:
                    for video_id in video_ids:
                        self.get_video_details(video_id)
                        time.sleep(1)  

                if queries:

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
                            time.sleep(1)

                logger.info(f"Sleeping for {interval} seconds before next collection cycle")
                time.sleep(interval)

            except Exception as e:
                logger.error(f"Error in collection cycle: {str(e)}")
                time.sleep(60)

if __name__ == "__main__":
    load_dotenv()

    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY environment variable is required")

    producer = YouTubeProducer(api_key=api_key)
    

    video_ids = []
    

    search_queries = [
        "Python programming",
        "Data Science",
        "Machine Learning",
        "C++ Programming",
        "Remix",
        "JavaScript tutorials",
        "Web development 2025",
        "React.js for beginners",
        "Best Python libraries for data analysis",
        "Kubernetes explained",
        "Neural networks simplified",
        "Statistics for data science",
        "Top machine learning projects in 2025",
        "Data science interview questions",
        "Deep learning with TensorFlow",
        "Latest AI trends",
        "Cloud computing basics",
        "Quantum computing explained",
        "Blockchain technology tutorial",
        "Best free coding resources",
        "Using Docker for development",
        "Flutter app development",
        "Django vs Flask",
        "ASP.NET core tutorial",
        "Remix vs Next.js comparison",
        "Best coding channels on YouTube",
        "Funny programming memes",
        "Coding livestreams",
        "Top 10 programming documentaries",
        "Code challenges for developers",
        "How to become a software engineer",
        "Time management for developers",
        "Tech jobs with no degree required",
        "Freelancing as a programmer",
        "Tech industry insights",
    ]
    

    producer.run(video_ids=video_ids, queries=search_queries) 