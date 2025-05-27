# Hướng dẫn Thiết lập và Chạy Hệ thống

## Private Key
- Cần thêm api key của YouTube Data API v3 trong file .env
YOUTUBE_API_KEY


## Yêu cầu
- Docker và Docker Compose
- Python 3.8+
- pip (Python package manager)

## Cài đặt Dependencies
```bash
pip install kafka-python
pip install pymongo
pip install elasticsearch
pip install requests
```

## Trình tự Chạy Hệ thống

### 1. Khởi động các services bằng Docker Compose
```bash
docker-compose up -d
```

Đợi khoảng 1-2 phút để tất cả các services khởi động hoàn toàn.

### 2. Chạy YouTube Data Producer
Producer sẽ thu thập dữ liệu từ YouTube API và gửi vào Kafka:
```bash
cd kafka
python youtube_producer.py
```

### 3. Chạy Spark Processor
Spark processor sẽ xử lý dữ liệu từ Kafka và lưu vào MongoDB:
```bash
cd spark
python youtube_processor.py
```

### 4. Đồng bộ dữ liệu từ MongoDB sang Elasticsearch
Script này sẽ đồng bộ dữ liệu từ MongoDB sang Elasticsearch:
```bash
cd mongo_to_es
python sync_to_es.py
```

### 5. Tạo Kibana Dashboard
Script này sẽ tự động tạo các visualizations và dashboard trong Kibana:
```bash
cd analysis
python create_kibana_dashboard.py
```

## Truy cập các Services

### Kibana
- URL: http://localhost:5601
- Tạo index pattern "youtube_stats"
- Time field: timestamp
- Xem dashboard đã được tạo tự động

### Elasticsearch
- URL: http://localhost:9200
- Kiểm tra dữ liệu: http://localhost:9200/youtube_stats/_search

### MongoDB
- URL: mongodb://localhost:27017
- Database: youtube_analytics
- Collection: video_stats

### Kafka
- Bootstrap server: localhost:9092
- Topic: youtube_data

## Xử lý Sự cố

### Nếu không thấy dữ liệu trong Kibana
1. Kiểm tra time range trong Kibana (đặt về ngày 27/5/2025)
2. Kiểm tra dữ liệu trong Elasticsearch:
```bash
curl -X GET "http://localhost:9200/youtube_stats/_count"
```

### Nếu services không hoạt động
1. Kiểm tra logs:
```bash
docker-compose logs -f
```

2. Khởi động lại services:
```bash
docker-compose down
docker-compose up -d
```

## Dừng Hệ thống
Để dừng tất cả các services:
```bash
docker-compose down
```
