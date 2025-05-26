// Switch to youtube_analytics database
db = db.getSiblingDB('youtube_analytics');

// Drop existing collections if they exist
db.video_stats.drop();
db.channel_stats.drop();

// Create collections with schema validation
db.createCollection("video_stats", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["video_id", "title", "channel_id", "channel_title", "publish_date", "timestamp"],
            properties: {
                video_id: {
                    bsonType: "string",
                    description: "Video ID from YouTube"
                },
                title: {
                    bsonType: "string",
                    description: "Video title"
                },
                channel_id: {
                    bsonType: "string",
                    description: "Channel ID"
                },
                channel_title: {
                    bsonType: "string",
                    description: "Channel name"
                },
                publish_date: {
                    bsonType: "string",
                    description: "Video publish date"
                },
                view_count: {
                    bsonType: "int",
                    minimum: 0,
                    description: "Number of views"
                },
                like_count: {
                    bsonType: "int",
                    minimum: 0,
                    description: "Number of likes"
                },
                timestamp: {
                    bsonType: "string",
                    description: "When the stats were collected"
                }
            }
        }
    }
});

db.createCollection("channel_stats", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["channel_id", "channel_title", "window_start", "window_end", "timestamp"],
            properties: {
                channel_id: {
                    bsonType: "string",
                    description: "Channel ID"
                },
                channel_title: {
                    bsonType: "string",
                    description: "Channel name"
                },
                avg_engagement: {
                    bsonType: "double",
                    minimum: 0,
                    description: "Average engagement ratio"
                },
                avg_likes: {
                    bsonType: "double",
                    minimum: 0,
                    description: "Average likes per video"
                },
                avg_views: {
                    bsonType: "double",
                    minimum: 0,
                    description: "Average views per video"
                },
                video_count: {
                    bsonType: "int",
                    minimum: 0,
                    description: "Number of videos analyzed"
                },
                window_start: {
                    bsonType: "string",
                    description: "Start of aggregation window"
                },
                window_end: {
                    bsonType: "string",
                    description: "End of aggregation window"
                },
                timestamp: {
                    bsonType: "string",
                    description: "When the stats were collected"
                }
            }
        }
    }
});

// Create indexes for better query performance
db.video_stats.createIndex({ "video_id": 1, "timestamp": -1 });
db.video_stats.createIndex({ "channel_id": 1, "timestamp": -1 });

db.channel_stats.createIndex({ "timestamp": -1 });
db.channel_stats.createIndex({ "channel_id": 1, "window_start": -1 });
db.channel_stats.createIndex({ "channel_title": 1 });

// Insert sample data for testing
db.video_stats.insertOne({
    video_id: "sample_video_1",
    title: "Sample Video 1",
    channel_id: "channel_1",
    channel_title: "Test Channel",
    publish_date: "2024-01-01T00:00:00Z",
    view_count: 1000,
    like_count: 100,
    timestamp: "2024-01-01T12:00:00Z"
});

db.channel_stats.insertOne({
    channel_id: "channel_1",
    channel_title: "Test Channel",
    avg_engagement: 0.15,
    avg_likes: 100,
    avg_views: 1000,
    video_count: 1,
    window_start: "2024-01-01T12:00:00Z",
    window_end: "2024-01-01T13:00:00Z",
    timestamp: "2024-01-01T13:00:00Z"
});

// Create aggregation pipelines for analytics
db.system.js.save({
    _id: "getChannelEngagement",
    value: function() {
        return db.channel_stats.aggregate([
            {
                $group: {
                    _id: {
                        channel_id: "$channel_id",
                        channel_title: "$channel_title"
                    },
                    overall_engagement: { $avg: "$avg_engagement" },
                    overall_likes: { $avg: "$avg_likes" },
                    overall_views: { $avg: "$avg_views" },
                    total_videos: { $sum: "$video_count" },
                    first_seen: { $min: "$window_start" },
                    last_seen: { $max: "$window_end" }
                }
            }
        ]);
    }
});

db.system.js.save({
    _id: "getVideoTrends",
    value: function() {
        return db.video_stats.aggregate([
            {
                $group: {
                    _id: {
                        video_id: "$video_id",
                        title: "$title",
                        channel_id: "$channel_id",
                        channel_title: "$channel_title",
                        publish_date: "$publish_date"
                    },
                    avg_views: { $avg: "$view_count" },
                    view_growth: {
                        $subtract: [
                            { $max: "$view_count" },
                            { $min: "$view_count" }
                        ]
                    },
                    avg_likes: { $avg: "$like_count" },
                    like_growth: {
                        $subtract: [
                            { $max: "$like_count" },
                            { $min: "$like_count" }
                        ]
                    },
                    data_points: { $sum: 1 },
                    first_tracked: { $min: "$timestamp" },
                    last_tracked: { $max: "$timestamp" }
                }
            }
        ]);
    }
}); 