import urllib.parse
import re
import pandas as pd
import datetime
import streamlit as st
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import googleapiclient.discovery
import mysql.connector


# Set Streamlit page configuration
st.set_page_config(
    page_title="Capstone 1",
    layout="wide",
    initial_sidebar_state="expanded",
)
# Define tabs
tabs = ["Home", "Insights", "About"]
selected_tab = st.sidebar.radio("Select Tab", tabs)

# Streamlit header and description
if selected_tab == "Home":
    st.header("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
    st.subheader("Automate data extraction within a Minute")
    st.markdown(
        'This Capstone Project is specifically crafted for extracting data from _YouTube_ and involves working on SQL '
        'queries'
    )

    # User Input:
    ch = st.text_input("Enter the channel Id")
    channel_list = [ch]

    submit = st.button("EXTRACT")

    if submit:
        st.info("Fetching channel data from YouTube API...")
        # YouTube API connection
        api_service_name = "youtube"
        api_version = "v3"
        api_key = 'YOUR API KEY'
        youtube = googleapiclient.discovery.build(
            api_service_name, api_version, developerKey=api_key
        )

        # MongoDB connection
        encoded_password = urllib.parse.quote_plus("Nirmal@2000")
        uri = f"mongodb+srv://usernamepassword@cluster0.2mrcbw2.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(uri, server_api=ServerApi('1'))

        db = client["Capstone_1"]
        col = db["Channel"]
        vid = db["Video"]
        cmt = db["Comments"]

        # Fetch channel data from YouTube API
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=ch
        )
        item = request.execute()


        def insert_channel_data(item):
            try:
                for response in item.get('items', []):
                    Channel_Name = response['snippet']['title']
                    Sub_count = response['statistics']['subscriberCount']
                    video_count = response['statistics']['videoCount']
                    view_Count = response['statistics']['viewCount']
                    Description = response['snippet']['description']
                    Channel_Id = response['id']

                    ch_Data = {
                        "Channel Name": Channel_Name,
                        "Subscriber Count": Sub_count,
                        "Video Count": video_count,
                        "View Count": view_Count,
                        "Description": Description,
                        "Channel Id": Channel_Id
                    }
                    col.insert_one(ch_Data)

            except KeyError as e:
                st.error(f"Error accessing key: {e}")
                st.text("Response structure:")


        # Call the function with the channel data
        st.info("Inserting channel data into MongoDB...")
        insert_channel_data(item)


        # Function to get channel videos from playlists
        def get_channel_videos(channel_id):
            try:
                # get Uploads playlist id
                res = youtube.channels().list(id=channel_id, part='contentDetails').execute()
                playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']

                videos = []
                next_page_token = None

                while 1:
                    res = youtube.playlistItems().list(playlistId=playlist_id,
                                                       part='snippet',
                                                       maxResults=50,
                                                       pageToken=next_page_token).execute()
                    videos += res['items']
                    next_page_token = res.get('nextPageToken')

                    if next_page_token is None:
                        break

                return videos

            except KeyError as e:
                st.error(f"Error accessing key: {e}")
                st.text("Response structure:")


        # Get channel videos
        videos = get_channel_videos(ch)


        # Function to process video details
        def process_video_details(video_id):
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response = request.execute()

            for item in response.get('items', []):
                vid_duration = item['contentDetails']['duration']
                match = re.match(r'PT(\d+)M(\d+)S', vid_duration)

                if match:
                    minutes, seconds = map(int, match.groups())
                    total_seconds = minutes * 60 + seconds

                    # Convert duration to HH:MM:SS format
                    duration_timedelta = datetime.timedelta(seconds=total_seconds)
                    formatted_duration = str(duration_timedelta)

                    # Video data
                    Channel_Name = item['snippet']['channelTitle']
                    vid_name = item['snippet']['title']
                    vid_desc = item['snippet']['description']
                    vid_id = item['id']
                    vid_pubdate = item['snippet']['publishedAt']
                    vid_viewcounts = item['statistics']['viewCount']
                    vid_likecount = item['statistics']['likeCount']
                    vid_favcount = item['statistics']['favoriteCount']
                    vid_commentcount = item['statistics']['commentCount']
                    vid_thumbnail = item['snippet']['thumbnails']['high']['url']
                    vid_caption = item['contentDetails']['caption']

                    vid_Data = {
                        "Channel Name": Channel_Name,
                        "Video Name": vid_name,
                        "Description": vid_desc,
                        "ID": vid_id,
                        "Published_Date": vid_pubdate,
                        "ViewCount": vid_viewcounts,
                        "Likes": vid_likecount,
                        "Favourite_counts": vid_favcount,
                        "Comments_Count": vid_commentcount,
                        "Duration": formatted_duration,
                        "Thumbnail": vid_thumbnail,
                        "Caption Status": vid_caption
                    }

                    vid.insert_one(vid_Data)


        # Call the function with the video ids
        st.info("Processing video details and inserting into MongoDB...")
        video_ids = [video['snippet']['resourceId']['videoId'] for video in videos]
        for video_id in video_ids:
            process_video_details(video_id)


        # Function to get comment information
        def get_comment_info(v_ids):
            for video_id in v_ids:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=1
                )
                response = request.execute()

                for item in response.get('items', []):
                    Comment_id = item['id']
                    Vid_ID = item['snippet']['videoId']
                    Comment_text = item['snippet']['topLevelComment']['snippet']['textOriginal']
                    Comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    Comment_publishedDate = item['snippet']['topLevelComment']['snippet']['updatedAt']

                    cmt_Data = {"Comment_ID": Comment_id,
                                "Video_ID": Vid_ID,
                                "Comment": Comment_text,
                                "Comment_Author": Comment_author,
                                "Published_Date": Comment_publishedDate}
                    cmt.insert_one(cmt_Data)


        # Call the function with the video ids
        video_ids = [video['snippet']['resourceId']['videoId'] for video in videos]
        get_comment_info(video_ids)
        st.info("Getting comment information and inserting into MongoDB...")

        # MySQL connection
        st.info("Connecting to MySQL database...")
        mysql_db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="YOUR PASSWORD",
            database="Capstone"
        )

        mysql_cursor = mysql_db.cursor()

        # Create Channels table
        create_channel_table_query = """
            CREATE TABLE IF NOT EXISTS Channels (
                ChannelID VARCHAR(255),
                ChannelName VARCHAR(255),
                SubscriberCount INT,
                ChannelViews INT,
                ChannelDescription TEXT,
                VideoCount INT,
                PRIMARY KEY (ChannelID)
            )
            """
        mysql_cursor.execute(create_channel_table_query)

        # Create Videos table
        create_video_table_query = """
            CREATE TABLE IF NOT EXISTS Videos (
                ChannelName VARCHAR(255),
                Title VARCHAR(255),
                Description TEXT,
                ID VARCHAR(255),
                Published_Date VARCHAR(255),
                ViewCount INT,
                Likes INT,
                Favourite_counts INT,
                Comments_Count INT,
                Duration VARCHAR(255),
                Thumbnail VARCHAR(255),
                Caption_Status VARCHAR(255),
                PRIMARY KEY (ID),
                CONSTRAINT unique_video_id UNIQUE (ID)
            )
            """
        mysql_cursor.execute(create_video_table_query)

        # Create Comments table
        create_comments_table_query = """
            CREATE TABLE IF NOT EXISTS Comments (
                Comment_Id VARCHAR(255),
                Video_iD VARCHAR(255),
                Comment TEXT,
                Published_Date VARCHAR(255),
                Comment_Author VARCHAR(255),
                PRIMARY KEY (Comment_ID),
                CONSTRAINT unique_comment_id UNIQUE (Comment_Id)
            )
            """
        mysql_cursor.execute(create_comments_table_query)
        st.info("Inserting data into MySQL database...")


        # Function to check if data already exists in MySQL
        def data_exists(table_name, unique_column, unique_value):
            query = f"SELECT * FROM {table_name} WHERE {unique_column} = %s"
            mysql_cursor.execute(query, (unique_value,))
            result = mysql_cursor.fetchone()
            return result is not None


        # Channel Data
        ch_data = col.find()
        for doc in ch_data:
            channel_id = doc.get("Channel Id", None)
            if channel_id is not None:

                if not data_exists("Channels", "ChannelID", channel_id):
                    query = (
                        "INSERT INTO Channels (ChannelID, ChannelName, SubscriberCount, ChannelViews, "
                        "ChannelDescription,"
                        "VideoCount)"
                        "VALUES (%s, %s, %s, %s, %s, %s)")
                    values = (
                        doc.get("Channel Id", None), doc.get("Channel Name", None), doc.get("Subscriber Count", None),
                        doc.get("View Count", None), doc.get("Description", None), doc.get("Video Count", None)
                    )
                    mysql_cursor.execute(query, values)

        # Video Data
        vid_data = vid.find()
        for doc in vid_data:
            video_id = doc.get("ID", None)
            if video_id is not None:
                if not data_exists("Videos", "ID", video_id):
                    query = (
                        "INSERT INTO Videos (ChannelName, Title, Description, ID, Published_Date, ViewCount, Likes, "
                        "Favourite_counts,"
                        "Comments_Count, Duration, Thumbnail, Caption_Status)"
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    values = (
                        doc.get("Channel Name", None),
                        doc.get("Video Name", None),
                        doc.get("Description", None),
                        doc.get("ID", None),
                        doc.get("Published_Date", None),
                        doc.get("ViewCount", None),
                        doc.get("Likes", None),
                        doc.get("Favourite_counts", None),
                        doc.get("Comments_Count", None),
                        doc.get("Duration", None),
                        doc.get("Thumbnail", None),
                        doc.get("Caption Status", None)
                    )
                    mysql_cursor.execute(query, values)

        # Comment Data
        cmt_data = cmt.find()
        for doc in cmt_data:
            comment_id = doc.get("Comment_ID", None)
            if comment_id is not None:
                if not data_exists("Comments", "Comment_Id", comment_id):
                    query = ("INSERT INTO Comments (Comment_Id, Video_iD, Comment, Published_Date, Comment_Author)"
                             "VALUES (%s, %s, %s, %s, %s)")
                    values = (
                        doc.get("Comment_ID", None),
                        doc.get("Video_ID", None),
                        doc.get("Comment", None),
                        doc.get("Published_Date", None),
                        doc.get("Comment_Author", None)
                    )
                    mysql_cursor.execute(query, values)

        mysql_db.commit()
        mysql_cursor.close()
        mysql_db.close()

        st.info("Uploading data to the database...")

        st.success("Data uploaded successfully!")
elif selected_tab == "Insights":
    st.write("# Insights ")
    with mysql.connector.connect(
            host="localhost",
            user="root",
            password="YOUR PASSWORD",
            database="Capstone"
    ) as connection:
        with st.sidebar:
            st.subheader(":orange[Select any questions to get Insights ?]")

        question = st.selectbox(
            'Please Select Your Question',
            ('1. What are the names of all the videos and their corresponding channels?',
             '2. Which channels have the most number of videos, and how many videos do they have?',
             '3. What are the top 10 most viewed videos and their respective channels?',
             '4. How many comments were made on each video, and what are their corresponding video names?',
             '5. Which videos have the highest number of likes, and what are their corresponding channel names? ',
             '6. What is the total number of likes  for each video, and what are their corresponding video names?',
             '7. What is the total number of views for each channel, and what are their corresponding channel names?',
             '8. What are the names of all the channels that have published videos in the year 2022? ',
             '9. What is the average duration of all videos in each channel, and what are their corresponding channel '
             'names?',
             '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

        if question == '1. What are the names of all the videos and their corresponding channels?':
            query = "SELECT Title, ChannelName FROM videos;"
        elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
            query = "SELECT ChannelName, VideoCount FROM channels ORDER BY VideoCount DESC limit 1;"
        elif question == '3. What are the top 10 most viewed videos and their respective channels?':
            query = "SELECT Title as Video_Name,  ViewCount as Views FROM videos ORDER BY ViewCount DESC LIMIT 10;"
        elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
            query = "SELECT Title as Video_Name, Comments_Count FROM videos;"
        elif question == ('5. Which videos have the highest number of likes, and what are their corresponding channel '
                          'names? '):
            query = "SELECT ChannelName, Likes FROM videos ORDER BY Likes DESC LIMIT 1;"
        elif question == ('6. What is the total number of likes  for each video, and what are their corresponding '
                          'video names?'):
            query = "SELECT ChannelName, Title, Likes FROM videos;"
        elif question == ('7. What is the total number of views for each channel, and what are their corresponding '
                          'channel names?'):
            query = "SELECT ChannelName, ChannelViews as Views FROM channels;"
        elif question == '8. What are the names of all the channels that have published videos in the year 2022? ':
            query = "SELECT Title, ChannelName, Published_Date FROM videos WHERE YEAR(Published_Date) = 2022;"
        elif question == ('9. What is the average duration of all videos in each channel, and what are their '
                          'corresponding channel names?'):
            query = ("SELECT ChannelName, AVG(time_to_sec(duration)) as average_duration FROM videos GROUP BY "
                     "ChannelName;")
        elif question == ('10. Which videos have the highest number of comments, and what are their corresponding '
                          'channel names?'):
            query = "SELECT ChannelName, Comments_Count FROM videos ORDER BY Comments_Count DESC LIMIT 1;"

        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            st.write(pd.DataFrame(result, columns=[desc[0] for desc in cursor.description]))


elif selected_tab == "About":
    st.write("# About")
    st.write("Welcome to the YouTube Data Harvesting and Warehousing Streamlit Application!")

    st.write("This application is designed to automate data extraction from YouTube using SQL, MongoDB, and Streamlit. It provides insights into channel data, video details, and comments, presenting them in an organized manner for analysis.")

    st.write("### Features:")
    st.write("- Extracts channel data using the YouTube API.")
    st.write("- Retrieves video details and comments, storing them in MongoDB.")
    st.write("- Populates a MySQL database with extracted data.")
    st.write("- Offers a streamlined user interface with Streamlit tabs for easy navigation.")
    st.write("- Provides insightful queries in the 'Insights' tab for data analysis.")

    st.write("### How to Use:")
    st.write("1. Navigate to the 'Home' tab.")
    st.write("2. Enter a YouTube channel ID and click 'EXTRACT' to fetch data.")
    st.write("3. View extracted data in the 'Insights' tab by selecting specific questions.")
    st.write("4. Explore additional details and insights about the application.")
