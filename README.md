# YouTube Analytics Project

Dự án phân tích dữ liệu YouTube theo thời gian thực sử dụng Big Data Stack.

## Tổng quan
Hệ thống thu thập và phân tích dữ liệu video YouTube theo thời gian thực, sử dụng các công nghệ Big Data như Apache Kafka, Apache Spark, MongoDB, Elasticsearch và Kibana.

## Kiến trúc hệ thống

```
YouTube API → Kafka → Spark → MongoDB → Elasticsearch → Kibana
```

### Components
1. **YouTube Data Producer**: Thu thập dữ liệu từ YouTube API và gửi vào Kafka
2. **Apache Kafka**: Message queue để xử lý dữ liệu theo thời gian thực
3. **Apache Spark**: Xử lý và phân tích dữ liệu từ Kafka
4. **MongoDB**: Lưu trữ dữ liệu thô
5. **Elasticsearch**: Đánh index và tìm kiếm dữ liệu
6. **Kibana**: Visualization và analytics dashboard

## Tính năng
- Thu thập dữ liệu video YouTube theo thời gian thực
- Phân tích số liệu views, likes và engagement
- Visualization với nhiều loại biểu đồ khác nhau
- Dashboard tự động cập nhật
- Tìm kiếm và lọc dữ liệu linh hoạt

## Cài đặt và Chạy

### Yêu cầu
- Docker và Docker Compose
- Python 3.8+
- YouTube Data API Key

### Quick Start
1. Clone repository:
```bash
git clone <repository-url>
cd big-data-project
```

2. Tạo file .env và thêm YouTube API Key:
```bash
YOUTUBE_API_KEY=your_api_key_here
```

3. Làm theo hướng dẫn trong [guide/Setup.md](guide/Setup.md)

## Cấu trúc thư mục
```
.
├── docker-compose.yml      # Docker services configuration
├── kafka/                  # Kafka producer code
├── spark/                  # Spark processing code
├── mongo_to_es/           # MongoDB to Elasticsearch sync
├── analysis/              # Kibana dashboard creation
└── guide/                 # Setup and configuration guides
```

## Công nghệ sử dụng
- Python 3.8+
- Apache Kafka
- Apache Spark
- MongoDB
- Elasticsearch 7.9.3
- Kibana 7.9.3
- Docker & Docker Compose

## Xử lý sự cố
Xem hướng dẫn xử lý sự cố trong [guide/Setup.md](guide/Setup.md)

## Đóng góp
Mọi đóng góp đều được chào đón. Vui lòng tạo issue hoặc pull request.

## License
MIT License
