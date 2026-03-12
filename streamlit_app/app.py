import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime

# Page config
st.set_page_config(
    page_title="🎵 TikTok Hashtag Analytics",
    page_icon="🎵",
    layout="wide"
)

# Title
st.title("🎵 TikTok Hashtag Analytics Dashboard")
st.markdown("---")

# Database connection (GIỐNG Y HỆT TEST.PY - ĐÃ WORK!)
@st.cache_resource
def get_connection():
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port="5433",
            database="tiktok_dw",
            user="postgres",
            password="postgres"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        return None

# Get data
def get_ranking_data():
    conn = get_connection()
    if conn:
        try:
            query = """
                SELECT 
                    daily_rank,
                    hashtag,
                    total_views,
                    report_date
                FROM agg_hashtag_rank 
                ORDER BY daily_rank
            """
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"❌ Query failed: {e}")
            return None
    return None

# Main app
def main():
    # Sidebar
    st.sidebar.title("📊 Dashboard Controls")
    st.sidebar.info("Data last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    st.sidebar.success("✅ Connected to PostgreSQL Docker")
    
    # Get data
    df = get_ranking_data()
    
    if df is not None and len(df) > 0:
        # Key metrics
        st.subheader("📈 Key Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                label="Total Hashtags Tracked",
                value=len(df),
                delta=None
            )
        
        with col2:
            top_hashtag = df.iloc[0]['hashtag']
            st.metric(
                label="🏆 Top Hashtag",
                value=top_hashtag,
                delta="Rank #1"
            )
        
        with col3:
            top_views = df.iloc[0]['total_views']
            st.metric(
                label="👀 Top Views",
                value=f"{top_views:,}",
                delta=None
            )
        
        st.markdown("---")
        
        # Charts
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.subheader("📊 Views by Hashtag")
            st.bar_chart(
                data=df.set_index("hashtag")["total_views"],
                color="#FF4B4B"
            )
        
        with col_chart2:
            st.subheader("🏅 Ranking Distribution")
            st.bar_chart(
                data=df.set_index("hashtag")["daily_rank"],
                color="#00CC00"
            )
        
        st.markdown("---")
        
        # Data table
        st.subheader("📋 Detailed Ranking Table")
        
        # Format the dataframe for display
        display_df = df.copy()
        display_df['total_views'] = display_df['total_views'].apply(lambda x: f"{x:,}")
        display_df['report_date'] = display_df['report_date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        
        # Add medals
        def add_medal(rank):
            if rank == 1:
                return "🥇"
            elif rank == 2:
                return "🥈"
            elif rank == 3:
                return "🥉"
            else:
                return ""
        
        display_df['Medal'] = display_df['daily_rank'].apply(add_medal)
        
        # Reorder columns
        display_df = display_df[['Medal', 'daily_rank', 'hashtag', 'total_views', 'report_date']]
        display_df.columns = ['Medal', 'Rank', 'Hashtag', 'Views', 'Date']
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Success message
        st.success("✅ Data loaded successfully from PostgreSQL!")
        
    else:
        st.warning("⚠️ No data found. Please run the Airflow DAG first.")
        st.info("Go to Airflow UI (localhost:8080) and trigger the DAG.")

if __name__ == "__main__":
    main()