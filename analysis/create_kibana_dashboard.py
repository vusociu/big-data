import requests
import json

# Kibana configuration
KIBANA_URL = "http://localhost:5601"
ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_PATTERN = "youtube_analytics"

# Headers for Kibana API requests
headers = {
    "kbn-xsrf": "true",
    "Content-Type": "application/json"
}

# def create_index_pattern():
#     url = f"{KIBANA_URL}/api/saved_objects/index-pattern"
#     data = {
#         "attributes": {
#             "title": INDEX_PATTERN,
#             "timeFieldName": "publish_date"
#         }
#     }
#     response = requests.post(url, headers=headers, json=data)
#     return response.json()

def create_top_videos_visualization():
    url = f"{KIBANA_URL}/api/saved_objects/visualization"
    data = {
        "attributes": {
            "title": "Top 10 video xu hướng",
            "visState": json.dumps({
                "title": "Top 10 video xu hướng",
                "type": "vertical_bar",
                "params": {
                    "type": "histogram",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1",
                        "type": "category",
                        "position": "bottom",
                        "show": True,
                        "style": {},
                        "scale": {"type": "linear"},
                        "labels": {"show": True, "truncate": 100},
                        "title": {}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1",
                        "name": "LeftAxis-1",
                        "type": "value",
                        "position": "left",
                        "show": True,
                        "style": {},
                        "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "Views"}
                    }]
                },
                "aggs": [
                    {
                        "id": "1",
                        "enabled": True,
                        "type": "max",
                        "schema": "metric",
                        "params": {"field": "view_count"}
                    },
                    {
                        "id": "2",
                        "enabled": True,
                        "type": "terms",
                        "schema": "segment",
                        "params": {
                            "field": "title.keyword",
                            "size": 10,
                            "order": "desc",
                            "orderBy": "1"
                        }
                    }
                ]
            }),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": INDEX_PATTERN,
                    "query": {"query": "", "language": "kuery"},
                    "filter": []
                })
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def create_engagement_ratio_visualization():
    url = f"{KIBANA_URL}/api/saved_objects/visualization"
    data = {
        "attributes": {
            "title": "Xu hướng tương tác video theo thời gian",
            "visState": json.dumps({
                "title": "Xu hướng tương tác video theo thời gian",
                "type": "line",
                "params": {
                    "type": "line",
                    "grid": {"categoryLines": False},
                    "categoryAxes": [{
                        "id": "CategoryAxis-1",
                        "type": "category",
                        "position": "bottom",
                        "show": True,
                        "style": {},
                        "scale": {"type": "linear"},
                        "labels": {"show": True, "truncate": 100},
                        "title": {}
                    }],
                    "valueAxes": [{
                        "id": "ValueAxis-1",
                        "name": "LeftAxis-1",
                        "type": "value",
                        "position": "left",
                        "show": True,
                        "style": {},
                        "scale": {"type": "linear", "mode": "normal"},
                        "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                        "title": {"text": "Average Engagement Ratio"}
                    }]
                },
                "aggs": [
                    {
                        "id": "1",
                        "enabled": True,
                        "type": "avg",
                        "schema": "metric",
                        "params": {"field": "engagement_ratio"}
                    },
                    {
                        "id": "2",
                        "enabled": True,
                        "type": "date_histogram",
                        "schema": "segment",
                        "params": {
                            "field": "timestamp",
                            "timeRange": {"from": "now-1y", "to": "now"},
                            "useNormalizedEsInterval": True,
                            "interval": "auto",
                            "drop_partials": False,
                            "min_doc_count": 1,
                            "extended_bounds": {}
                        }
                    }
                ]
            }),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "index": INDEX_PATTERN,
                    "query": {"query": "", "language": "kuery"},
                    "filter": []
                })
            }
        }
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def create_dashboard(visualizations):
    url = f"{KIBANA_URL}/api/saved_objects/dashboard"
    data = {
        "attributes": {
            "title": "YouTube Analytics Dashboard",
            "hits": 0,
            "description": "Analytics dashboard for YouTube data",
            "panelsJSON": json.dumps([
                {
                    "gridData": {
                        "x": 0,
                        "y": 0,
                        "w": 24,
                        "h": 15,
                        "i": viz["id"]
                    },
                    "version": "7.9.3",
                    "panelIndex": viz["id"],
                    "embeddableConfig": {},
                    "panelRefName": f"panel_{i}"
                }
                for i, viz in enumerate(visualizations)
            ]),
            "optionsJSON": json.dumps({
                "useMargins": True,
                "hidePanelTitles": False
            }),
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": []
                })
            }
        },
        "references": [
            {
                "name": f"panel_{i}",
                "type": "visualization",
                "id": viz["id"]
            }
            for i, viz in enumerate(visualizations)
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    return response.json()

def main():
    print("Creating index pattern...")
    index_pattern = create_index_pattern()
    
    print("Tạo top 10 video xu hướng")
    top_videos_viz = create_top_videos_visualization()
    
    print("Tạo xu hướng tương tác video theo thời gian")
    engagement_viz = create_engagement_ratio_visualization()
    
    print("Tạo dashboard")
    dashboard = create_dashboard([top_videos_viz, engagement_viz])
    
    print("Done! Truy cập dashboard tại:")
    print(f"{KIBANA_URL}/app/dashboards#/view/{dashboard['id']}")

if __name__ == "__main__":
    main() 