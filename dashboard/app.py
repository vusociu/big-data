import streamlit as st
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta

# MongoDB Connection
try:
    mongo_client = MongoClient("mongodb://localhost:27017/")
    db = mongo_client.youtube_analytics
except Exception as e:
    st.error(f"Error connecting to MongoDB: {e}")
    st.stop()

def get_channel_stats(hours_ago=24):
    """
    Get channel statistics from MongoDB for the last N hours
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_ago)

    try:
        # Query MongoDB for channel statistics
        cursor = db.channel_stats.find({
            "timestamp": {
                "$gte": start_time.isoformat(),
                "$lte": end_time.isoformat()
            }
        }).sort("timestamp", 1)
        
        # Convert to DataFrame
        records = list(cursor)
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching channel stats: {e}")
        return pd.DataFrame()

def get_video_stats(channel_id, hours_ago=24):
    """
    Get detailed video statistics for a specific channel
    """
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_ago)

    try:
        cursor = db.video_stats.find({
            "channel_id": channel_id,
            "timestamp": {
                "$gte": start_time.isoformat(),
                "$lte": end_time.isoformat()
            }
        }).sort("timestamp", 1)
        
        records = list(cursor)
        return pd.DataFrame(records) if records else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching video stats: {e}")
        return pd.DataFrame()

def main():
    st.title("YouTube Channel Analytics Dashboard")
    
    # Sidebar for filtering
    st.sidebar.header("Filters")
    hours = st.sidebar.slider("Hours to analyze", 1, 72, 24)
    
    # Get channel statistics
    df = get_channel_stats(hours)
    
    if df.empty:
        st.warning("No data available for the selected time range")
        return
    
    st.header("Overview")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Channels", df["channel_id"].nunique())
    with col2:
        st.metric("Total Videos", int(df["video_count"].sum()))
    with col3:
        avg_engagement = df["avg_engagement"].mean() if "avg_engagement" in df else 0
        st.metric("Avg. Engagement", f"{avg_engagement:.2%}")
    with col4:
        avg_likes = df["avg_likes"].mean() if "avg_likes" in df else 0
        st.metric("Avg. Likes", f"{int(avg_likes):,}")
    
    st.header("Engagement Over Time")
    if "timestamp" in df and "avg_engagement" in df:
        fig_engagement = px.line(
            df,
            x="timestamp",
            y="avg_engagement",
            color="channel_title",
            title="Channel Engagement Ratio Over Time"
        )
        st.plotly_chart(fig_engagement)
    else:
        st.warning("Insufficient data for engagement chart.")
    
    # Top channels by engagement
    st.header("Top Channels by Engagement")
    if "channel_title" in df and "avg_engagement" in df:
        top_channels = df.groupby("channel_title").agg({
            "avg_engagement": "mean",
            "avg_likes": "mean",
            "avg_views": "mean",
            "video_count": "sum"
        }).sort_values("avg_engagement", ascending=False).head(10)
        
        fig_top = go.Figure(data=[
            go.Bar(
                x=top_channels.index,
                y=top_channels["avg_engagement"],
                text=[f"{v:.2%}" for v in top_channels["avg_engagement"]],
                textposition="auto",
            )
        ])
        fig_top.update_layout(
            title="Top 10 Channels by Average Engagement",
            xaxis_title="Channel",
            yaxis_title="Average Engagement Ratio"
        )
        st.plotly_chart(fig_top)
    else:
        st.warning("Insufficient data for top channels.")
    
    # Detailed channel stats
    st.header("Channel Details")
    if "channel_title" in df:
        channel_select = st.selectbox(
            "Select Channel",
            options=df["channel_title"].unique()
        )
        
        channel_data = df[df["channel_title"] == channel_select]
        if not channel_data.empty:
            channel_id = channel_data["channel_id"].iloc[0]
            
            # Get detailed video stats for the selected channel
            video_df = get_video_stats(channel_id, hours)
            
            if not video_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    fig_likes = px.line(
                        video_df,
                        x="timestamp",
                        y="like_count",
                        title="Video Likes Over Time"
                    )
                    st.plotly_chart(fig_likes)
                
                with col2:
                    fig_views = px.line(
                        video_df,
                        x="timestamp",
                        y="view_count",
                        title="Video Views Over Time"
                    )
                    st.plotly_chart(fig_views)
                
                # Video details table
                st.header("Video Details")
                video_details = video_df[["title", "view_count", "like_count", "timestamp"]]
                st.dataframe(video_details)
            else:
                st.warning("No detailed video data available for this channel")
        else:
            st.warning("No data available for the selected channel")
    else:
        st.warning("Insufficient data for channel details.")

if __name__ == "__main__":
    main()
