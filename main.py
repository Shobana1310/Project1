from googleapiclient.discovery import build
import pymongo
from sqlalchemy import text
from googleapiclient.errors import HttpError
import pandas as pd
import streamlit as st
import pymysql
import pymssql
import requests
from streamlit_lottie import st_lottie
from streamlit_option_menu import option_menu
import time
from PIL import Image
from pytz import timezone
import plotly.express as px
from streamlit_dynamic_filters import DynamicFilters
import calendar
import numpy as np
import re
import pyodbc
from sqlalchemy import create_engine
import urllib.parse
from textblob import TextBlob
from datetime import datetime, timedelta
import plotly.graph_objects as go
import streamlit.components.v1 as components


mysql_password = st.secrets["MYSQL_PASSWORD"] #MySQLpassword
api_key = st.secrets["API_KEY"] #Youtube_API_V3
mongo_atlas_user_name = st.secrets["MONGO_ATLAS_USER_NAME"] #Mongo_Atlas_User_name
mongo_atlas_password =  st.secrets["MONGO_ATLAS_PASSWORD"]  #Mongo_Atlas_password

server = st.secrets["SERVER"]
database = st.secrets["DATABASE"]
username = st.secrets["USERNAME"]
password = st.secrets["AZURE_PASSWORD"]

conn_str = f"mssql+pymssql://{username}:{password}@{server}/{database}?charset=utf8"

engine = create_engine(conn_str, echo=True)

# we connect engine for sql and cilent for mongodb
client=pymongo.MongoClient(f"mongodb+srv://{mongo_atlas_user_name}:{mongo_atlas_password}@cluster0.ehfepgy.mongodb.net/?retryWrites=true&w=majority")

#API Connection
def google_api_client():
    service=build("youtube","v3",developerKey=api_key)    #Build the YouTube API service using the provided API key
    return service
youtube=google_api_client()

# this is the function for get details of the channel
def get_details_of_channel(channel_id):
    response = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()
    data_list = [{
            "channel_name": i["snippet"]["title"],
            "channel_id": i["id"],
            "subscription_count": i["statistics"]["subscriberCount"],
            "channel_views": i["statistics"]["viewCount"],
            "Total_Videos" :i["statistics"]["videoCount"],
            "channel_description": i["snippet"]["localized"]["description"],
            "playlist_id": i["contentDetails"]["relatedPlaylists"]["uploads"],
            "profile_picture": i["snippet"]["thumbnails"]["high"]["url"],
            "joined_at": i["snippet"]["publishedAt"],
            "youtube_link": f"https://www.youtube.com/channel/{i['id']}"}
             for i in response["items"]]

    return data_list                                                   #Return the list containing details of the channel
def get_playlist_id(channel_id):
    response = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()
    playlist_id =response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    return playlist_id


# this function for to get video_ids for further steps
def to_get_videoids(playlist_id):

    video_ids=[]
    request=youtube.playlistItems().list(part="contentDetails",playlistId=playlist_id,maxResults=50)
    while request:                                                                                       #while loop check the truthy of request,it terminate when there is no pages to iterate
        response1=request.execute()
        for i in response1["items"]:
            video_ids.append(i["contentDetails"]["videoId"])
        request=youtube.playlistItems().list_next(request,response1)                                     #it update the request again
    return video_ids


# to get information of all videos in particular channel
def get_video_details(video_ids):
    video_datas = []
    for i in video_ids:
        response = youtube.videos().list(part="snippet,contentDetails,statistics", id=i).execute()
        for j in response["items"]:
            thumbnails = j['snippet']['thumbnails']
            high_quality_thumbnail = thumbnails.get('high', thumbnails.get('medium', thumbnails.get('default')))
            data = dict(
                Channel_Name = j['snippet']['channelTitle'],
                Channel_Id = j['snippet']['channelId'],
                Video_Id = j['id'],
                Title = j['snippet']['title'],
                Thumbnail = high_quality_thumbnail['url'],
                Description = j['snippet']['description'],
                Published_Date = j['snippet']['publishedAt'],
                Duration = j['contentDetails']['duration'],
                Views = j['statistics']['viewCount'],
                Likes = j['statistics'].get('likeCount'),
                Comments = j['statistics'].get('commentCount'),
                Favorite_Count = j['statistics']['favoriteCount'],
                Definition = j['contentDetails']['definition'],
                Caption_Status = j['contentDetails']['caption']
            )
            video_datas.append(data)
    return video_datas
                                                   



#this function for get the allcomments of particular channel
def get_comments_details(video_ids):
    comment_data=[]                                  #Initialize an empty list to store comment details
    
    for i in video_ids:
        next_page_token=None
        comments_disabled=False

        while True:
            try:                                      #make an API request to get comment details 
                response=youtube.commentThreads().list(part="snippet,replies",videoId=i,maxResults=100,
                                                    pageToken=next_page_token).execute()
                for item in response["items"]:         # Iterate through items in the API response and extract relevant information
                    data=dict(Channel_ID=item["snippet"]["channelId"],         # Create a dictionary containing comment details
                                    Comment_ID=item["snippet"]["topLevelComment"]["id"],
                                    Video_ID=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                                    Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                    Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    Comment_Published_Date=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                    comment_data.append(data)
                next_page_token=response.get("nextPageToken")
                if not next_page_token:
                    break
            except HttpError as e:                         #Handle the case where comments are disabled for the video
                if e.resp.status==403 and e.error_details[0]["reason"]=="commentsDisabled":
                    comments_disabled=True
                    print(f"comments are disabled for videoid:{i}")
                    break
                else:
                    raise
            if comments_disabled:                           #Exit the loop if comments are disabled for that video
                break
    return comment_data

database=client["youtube_data_harvesting"]                #create database in MongoDB
collection=database["channel_informations"]               #create a collection


#call the all functions in one function
def get_allthe_details_of_channel(channel_id):
    channel=get_details_of_channel(channel_id)             #Retrieve details of the channel using the provided channel_id
    playlist_id=get_playlist_id(channel_id)             
    video_id=to_get_videoids(playlist_id)                  # Retrieve video IDs associated with the channel's playlist
    videos=get_video_details(video_id)                     # Retrieve details of each video using the video IDs
    comments=get_comments_details(video_id)                # Retrieve details of comments for each video
    collection.insert_one({"channel_details":channel,"videos_details":videos,"comment_details":comments})   # Insert all the gathered details into a MongoDB collection
    return "yes! uploaded"


def update_the_channel_details(channel_id):
    channel=get_details_of_channel(channel_id)           
    playlist_id=get_playlist_id(channel_id)             
    video_id=to_get_videoids(playlist_id)                  
    videos=get_video_details(video_id)                    
    comments=get_comments_details(video_id)
    collection.update_one(
        {"channel_details.channel_id": channel_id},
        {"$set": {"channel_details": channel, "videos_details": videos, "comment_details": comments}})
    return "UPDATED SUCCESSFULLY !"


# This function for create a cahnnel table in MYSQl in SQL Alchemy method
def channels_table():
    with engine.connect()as conn:
        drop_query=text('''drop table if exists channels''')      #Drop the 'channels' table if it already exists
        conn.execute(drop_query)
        
    try:                                                          #In try block, we exucute the create query
        with engine.connect()as conn:
            create_query=text('''create table if not exists channels(
                            channel_name varchar(100),
                            channel_id varchar(100) primary key,
                            subscription_count int,
                            channel_views int,
                            total_videos int,
                            channel_description text,
                            playlist_id varchar(100),
                            profile_picture varchar(250),
                            joined_at varchar(200),
                            youtube_link varchar(200))''')
            conn.execute(create_query)
    except:
        print("channel table created already")              # Print a message if an exception occurs

    data=[]                                                  # Retrieve channel details from MongoDB and insert them into the 'channels' table
    database=client["youtube_data_harvesting"]
    collection=database["channel_informations"]
    for i in collection.find({},{"_id":0,"channel_details":1}):    # Extract channel details from the MongoDB collection
        for j in range(len(i["channel_details"])):
            data.append(i["channel_details"][j])
    df=pd.DataFrame(data)                                          # Create a DataFrame from the extracted channel details
    try:
        # Insert channel details into the 'channels' table
        df.to_sql('channels', con=engine, if_exists='append', index=False)
    except:
        print("channel values are already inserted into table")


        
#this function for create videos table
def videos_table():

    with engine.connect()as conn:
        drop_query=text('''drop table if exists videos''') 
        conn.execute(drop_query)

    try:
        with engine.connect()as conn:
            create_query=text('''create table if not exists videos(
                        Channel_Name varchar(200),
                        Channel_Id varchar(200),
                        Video_Id varchar(200) PRIMARY KEY, 
                        Title varchar(200),  
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date varchar(200) ,
                        Duration varchar(200), 
                        Views bigint, 
                        Likes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(200), 
                        Caption_Status varchar(100))''')
            conn.execute(create_query)
    except:
        print("channel table created already")

    
    data=[]                              # Retrieve channel details from MongoDB and insert them into the 'videos' table
    database=client["youtube_data_harvesting"]
    collection=database["channel_informations"]
    for i in collection.find({},{"_id":0,"videos_details":1}):          # Extract video details from the MongoDB collection
        for j in range(len(i["videos_details"])):
            data.append(i["videos_details"][j])
    df=pd.DataFrame(data)                                         # Create a DataFrame from the extracted video details

    try:
        df.to_sql('videos', con=engine, if_exists='append', index=False)       # Create a DataFrame from the extracted video details
    except:
        print("channel values are already inserted into table")


# this function for create comments table
def comments_table():

    with engine.connect()as conn:
        drop_query=text('''drop table if exists comments''') 
        conn.execute(drop_query)

    try:
        with engine.connect() as conn:
            create_query=text('''create table if not exists comments(
                                Channel_ID varchar(100),
                                Comment_ID varchar(100),
                                Video_ID varchar(100),
                                Comment_Text text,
                                Comment_Author varchar(200),
                                Comment_Published_Date varchar(225))''')
            conn.execute(create_query)
    except:
        print("channel table created already")

    data=[]                                                        # Retrieve channel details from MongoDB and insert them into the 'comments' table
    database=client["youtube_data_harvesting"]
    collection=database["channel_informations"]
    for i in collection.find({},{"_id":0,"comment_details":1}):          # Extract video details from the MongoDB collection
        for j in range(len(i["comment_details"])):
            data.append(i["comment_details"][j])
    df=pd.DataFrame(data)                                                # Create a DataFrame from the extracted video details

    try:
        df.to_sql('comments', con=engine, if_exists='append', index=False)         # Create a DataFrame from the extracted comments details
    except:
         print("channel values are already inserted into table")


# Call functions to create tables for channels, videos, and comments
def tables():
    channels_table()
    videos_table()
    comments_table()
    return "Tables Created !"

def parse_iso8601_duration(duration):
    pattern = re.compile(r'PT(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    if not match:
        return None
    
    minutes = int(match.group(1)) if match.group(1) else 0
    seconds = int(match.group(2)) if match.group(2) else 0
    return timedelta(minutes=minutes, seconds=seconds)

def format_num(views):
    if views >= 10000000:  
        return f'{views // 1000000}M'
    elif views >= 100000:  
        return f'{views // 100000}L'
    elif views >= 1000:  
        return f'{views // 1000}K'
    else:
        return str(views)
    
def analyze_sentiment(comment):
    blob = TextBlob(comment)
    polarity = blob.sentiment.polarity
    if polarity > 0.05:
        return 'Positive'
    elif polarity < -0.05:
        return 'Negative'
    else:
        return 'Neutral'
    
def load_lottiurl(url):       # Define a function to load Lottie animation JSON from a given URL
    r=requests.get(url)
    if r.status_code !=200:
        return None
    return r.json()

# Set the configuration for the Streamlit app page, including title, icon, and layout.
st.set_page_config(page_title="Analysis App",page_icon="📉",layout="wide")



# Create a sidebar with a header and an option menu for the main menu.
with st.sidebar:
    selected=option_menu(menu_title="MENU",options=["Home","Channel Analysis","Filter Options","About"], # Use a custom function option_menu to create a dropdown menu with icons.
                         icons=["bi bi-house-fill","youtube","bi bi-filter-circle-fill","bi bi-exclamation-square-fill"],
                         menu_icon="bi bi-menu-button-wide-fill",
                         default_index=0)

# Check the selected option from the sidebar and perform actions accordingly.
if selected=="Home":
    col1,col2,col3=st.columns([4,6,1])
    with col2:
        video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/logo%20video.mp4"
        video_html = f"""
            <video width="42%" autoplay loop muted playsinline>
                <source src="{video_url}" type="video/mp4">
                Your browser does not support the video tag.
            </video>
            """
        components.html(video_html, height=300)
    
    st.write('')
    st.write('')
    st.header(':orange[Key Features]')
    st.subheader('1.Comprehensive Data Scraping')
    st.subheader('2.Efficient Data Storage and Management')
    st.subheader('3.Detailed Analytics and Visualizations')
    st.subheader('4.Multi-Channel Comparison')
    st.header(':orange[Use Cases]')
    st.write('')
    st.write('')
    tab1,tab2=st.columns([1,3])
    with tab1:
        image="https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/creator.png"
        st.image(image,width=230)
    with tab2:
        st.write('')
        st.header(':violet[For YouTube Content Creators]')
        st.subheader('Understand your audience and optimize the content strategy.Identify top-performing videos also and aim to achieve similar success')
    tab1,tab2=st.columns([1,3])
    with tab1:
        st.write('')
        st.write('')
        st.image("https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/marketers.png",width=250)
    with tab2:
        st.write('')
        st.write('')
        st.write('')
        st.write('')
        st.header(':violet[For Marketers]')
        st.subheader('Analyze competitor channels and refine your marketing strategy.Measure the effectiveness of video campaigns and promotions')
    tab1,tab2=st.columns([1,3])
    with tab1:
        st.write('')
        st.write('')
        st.write('')
        st.image("https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/analyst.png",width=250)
    with tab2:
        st.write('')
        st.write('')
        st.write('')
        st.write('')
        st.write('')
        st.header(':violet[For Analyst]')
        st.subheader('Gain in-depth insights into YouTube channel performance.Use data-driven insights to make informed decisions')
    st.write('')
    st.write('')
    st.write('')
    st.header(':orange[Visual Overview]')
    col1,col2=st.columns(2)
    with col1:
        st.image('https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/Screenshot%201.png',width=700)
    with col2:
        st.image('https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/Screenshot%202.png',width=700)
    st.write('')
    st.write('')
    col1,col2=st.columns([2,3])
    with col1:
        st.image('https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/Screenshot%203.png',width=550)
    with col2:
        st.image('https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/Screenshot%204.png',width=800)






    st.write('')
    st.write('')
    st.write('')
    col1,col2,col3=st.columns([1,30,1])
    with col2:
        video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/slide.mp4"
        video_html = f"""
        <div style="display: flex; justify-content: center;">
            <video width="42%" autoplay loop muted playsinline>
                <source src="{video_url}" type="video/mp4">
                Your browser does not support the video tag.
                    </video>
                </div>
                """
        components.html(video_html, height=500)
   
    





elif selected=="Channel Analysis":
    col1,col2=st.columns(2)
    with col1:
       lottie_coding=load_lottiurl("https://lottie.host/aaf125ee-65f7-4fae-93bc-4f6c1a5c2ae5/J46grB2TED.json")
       st_lottie(lottie_coding,height=300,key="analysis")
    with col2:
        st.write('')
        st.write('')
        font_family = "Book Antiqua"
        youtube_logo_url = "https://upload.wikimedia.org/wikipedia/commons/4/42/YouTube_icon_%282013-2017%29.png"
        st.markdown(f"<h1 style='font-size: 45px; font-family: {font_family};'>START TO ANALYSE YOUR OWN YOUTUBE CHANNEL <img src='{youtube_logo_url}' width='65' height='45'></h1>", unsafe_allow_html=True)
    channel_id= st.text_input(label="", placeholder="Enter The Youtube Channel ID")
    if channel_id:
        # ids=[]
        # database=client["youtube_data_harvesting"]
        # collection=database["channel_informations"]
        # for i in collection.find({},{"_id":0,"channel_details":1}):      
        #         for j in range(len(i["channel_details"])):
        #             ids.append(i["channel_details"][j]["channel_id"])
        # if channel_id in ids:
        #     with st.spinner("Channel ID is Already Exist In The Database, It's Updating Now..."):
        #         update_the_channel_details(channel_id)
        #         tables()
        # else:
        #     with st.spinner("Getting Your Data..."):
        #         get_allthe_details_of_channel(channel_id)
        #         tables()

        def get_channel_data(channel_id):
            query = text('SELECT * FROM channels WHERE channel_id=:channel_id')
            data = pd.read_sql_query(query, engine, params={'channel_id': channel_id})
            return data
        data=get_channel_data(channel_id)
        channel_name = data.loc[0]['channel_name']
        subscription_count = data.loc[0]['subscription_count']
        total_videos = data.loc[0]['Total_Videos']
        channel_views=data.loc[0]['channel_views']
        channel_description = data.loc[0]['channel_description']
        profile_picture = data.loc[0]['profile_picture']
        joined_at = data.loc[0]['joined_at']
        youtube_link = data.loc[0]['youtube_link']
        timestamp = pd.Timestamp(joined_at)
        timestamp_ist = timestamp.tz_convert(timezone('UTC')).tz_convert(timezone('Asia/Kolkata'))
        formatted_date = timestamp_ist.strftime('%b %d, %Y')
        col1,col2=st.columns([3,1])      
        with col2:
            st.write(' ')
            st.write(' ')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write(' ')
            st.write(' ')
            image_path =profile_picture
            st.markdown(f"""
                    <style>
                    .round-image {{
                        width: 250px;
                        height: 250px;
                        border-radius: 50%;
                        object-fit: cover;
                    }}
                    </style>
                    <img src="{image_path}" class="round-image">
                    """,
                    unsafe_allow_html=True)
            with col1:
                st.header(channel_name)
                st.write(' ')

                description_icon = f"""
                <div style="display: flex; align-items: center;">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 448 512" width="40" height="40" style="margin-right: 10px;fill: white;">
                        <path d="M288 64c0 17.7-14.3 32-32 32H32C14.3 96 0 81.7 0 64S14.3 32 32 32H256c17.7 0 32 14.3 32 32zm0 256c0 17.7-14.3 32-32 32H32c-17.7 0-32-14.3-32-32s14.3-32 32-32H256c17.7 0 32 14.3 32 32zM0 192c0-17.7 14.3-32 32-32H416c17.7 0 32 14.3 32 32s-14.3 32-32 32H32c-17.7 0-32-14.3-32-32zM448 448c0 17.7-14.3 32-32 32H32c-17.7 0-32-14.3-32-32s14.3-32 32-32H416c17.7 0 32 14.3 32 32z"/></svg>
                    <span>{channel_description} </span>
                </div>
                """
                st.write(description_icon, unsafe_allow_html=True)

                st.write('')
                st.write('')


                video_html = f"""
                <div style="display: flex; align-items: center;">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 576 512" width="20" height="20" style="margin-right: 10px;fill: white;">
                        <path d="M549.7 124.1c-6.3-23.7-24.8-42.3-48.3-48.6C458.8 64 288 64 288 64S117.2 64 74.6 75.5c-23.5 6.3-42 24.9-48.3 48.6-11.4 42.9-11.4 132.3-11.4 132.3s0 89.4 11.4 132.3c6.3 23.7 24.8 41.5 48.3 47.8C117.2 448 288 448 288 448s170.8 0 213.4-11.5c23.5-6.3 42-24.2 48.3-47.8 11.4-42.9 11.4-132.3 11.4-132.3s0-89.4-11.4-132.3zm-317.5 213.5V175.2l142.7 81.2-142.7 81.2z"/>
                    </svg>
                    <span>{total_videos} Videos</span>
                </div>
                """

                st.write(video_html, unsafe_allow_html=True)

                st.write('')
                st.write('')


                subcription_html=f"""
                <div style="display: flex; align-items: center;">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 512" width="20" height="20" style="margin-right: 10px;fill: white;">
                        <path d="M72 88a56 56 0 1 1 112 0A56 56 0 1 1 72 88zM64 245.7C54 256.9 48 271.8 48 288s6 31.1 16 42.3V245.7zm144.4-49.3C178.7 222.7 160 261.2 160 304c0 34.3 12 65.8 32 90.5V416c0 17.7-14.3 32-32 32H96c-17.7 0-32-14.3-32-32V389.2C26.2 371.2 0 332.7 0 288c0-61.9 50.1-112 112-112h32c24 0 46.2 7.5 64.4 20.3zM448 416V394.5c20-24.7 32-56.2 32-90.5c0-42.8-18.7-81.3-48.4-107.7C449.8 183.5 472 176 496 176h32c61.9 0 112 50.1 112 112c0 44.7-26.2 83.2-64 101.2V416c0 17.7-14.3 32-32 32H480c-17.7 0-32-14.3-32-32zm8-328a56 56 0 1 1 112 0A56 56 0 1 1 456 88zM576 245.7v84.7c10-11.3 16-26.1 16-42.3s-6-31.1-16-42.3zM320 32a64 64 0 1 1 0 128 64 64 0 1 1 0-128zM240 304c0 16.2 6 31 16 42.3V261.7c-10 11.3-16 26.1-16 42.3zm144-42.3v84.7c10-11.3 16-26.1 16-42.3s-6-31.1-16-42.3zM448 304c0 44.7-26.2 83.2-64 101.2V448c0 17.7-14.3 32-32 32H288c-17.7 0-32-14.3-32-32V405.2c-37.8-18-64-56.5-64-101.2c0-61.9 50.1-112 112-112h32c61.9 0 112 50.1 112 112z"/>
                    </svg>
                    <span>{subscription_count} Subscribers</span>
                </div>
                """
                st.write(subcription_html, unsafe_allow_html=True)

                st.write('')
                st.write('')


                views_html=f"""
                <div style="display: flex; align-items: center;">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 576 512" width="20" height="20" style="margin-right: 10px;fill: white;">
                        <path d="M288 32c-80.8 0-145.5 36.8-192.6 80.6C48.6 156 17.3 208 2.5 243.7c-3.3 7.9-3.3 16.7 0 24.6C17.3 304 48.6 356 95.4 399.4C142.5 443.2 207.2 480 288 480s145.5-36.8 192.6-80.6c46.8-43.5 78.1-95.4 93-131.1c3.3-7.9 3.3-16.7 0-24.6c-14.9-35.7-46.2-87.7-93-131.1C433.5 68.8 368.8 32 288 32zM144 256a144 144 0 1 1 288 0 144 144 0 1 1 -288 0zm144-64c0 35.3-28.7 64-64 64c-7.1 0-13.9-1.2-20.3-3.3c-5.5-1.8-11.9 1.6-11.7 7.4c.3 6.9 1.3 13.8 3.2 20.7c13.7 51.2 66.4 81.6 117.6 67.9s81.6-66.4 67.9-117.6c-11.1-41.5-47.8-69.4-88.6-71.1c-5.8-.2-9.2 6.1-7.4 11.7c2.1 6.4 3.3 13.2 3.3 20.3z"/>
                    </svg>
                    <span>{channel_views} Views</span>
                </div>
                """
                st.write(views_html, unsafe_allow_html=True)

                st.write('')
                st.write('')


                joinat_html= f"""
                <div style="display: flex; align-items: center;">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512" width="20" height="20" style="margin-right: 10px;fill: white;">
                        <path d="M256 512A256 256 0 1 0 256 0a256 256 0 1 0 0 512zm0-384c13.3 0 24 10.7 24 24V264c0 13.3-10.7 24-24 24s-24-10.7-24-24V152c0-13.3 10.7-24 24-24zM224 352a32 32 0 1 1 64 0 32 32 0 1 1 -64 0z"/>
                    </svg>
                    <span> Joined At {formatted_date} </span>
                </div>
                """
                st.write(joinat_html, unsafe_allow_html=True)

                st.write('')
                st.write('')


              
                icon_html = f"""
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 640 512" width="20" height="20" style="margin-right: 10px;fill: white;">
                        <path d="M579.8 267.7c56.5-56.5 56.5-148 0-204.5c-50-50-128.8-56.5-186.3-15.4l-1.6 1.1c-14.4 10.3-17.7 30.3-7.4 44.6s30.3 17.7 44.6 7.4l1.6-1.1c32.1-22.9 76-19.3 103.8 8.6c31.5 31.5 31.5 82.5 0 114L422.3 334.8c-31.5 31.5-82.5 31.5-114 0c-27.9-27.9-31.5-71.8-8.6-103.8l1.1-1.6c10.3-14.4 6.9-34.4-7.4-44.6s-34.4-6.9-44.6 7.4l-1.1 1.6C206.5 251.2 213 330 263 380c56.5 56.5 148 56.5 204.5 0L579.8 267.7zM60.2 244.3c-56.5 56.5-56.5 148 0 204.5c50 50 128.8 56.5 186.3 15.4l1.6-1.1c14.4-10.3 17.7-30.3 7.4-44.6s-30.3-17.7-44.6-7.4l-1.6 1.1c-32.1 22.9-76 19.3-103.8-8.6C74 372 74 321 105.5 289.5L217.7 177.2c31.5-31.5 82.5-31.5 114 0c27.9 27.9 31.5 71.8 8.6 103.9l-1.1 1.6c-10.3 14.4-6.9 34.4 7.4 44.6s34.4 6.9 44.6-7.4l1.1-1.6C433.5 260.8 427 182 377 132c-56.5-56.5-148-56.5-204.5 0L60.2 244.3z"/>
                    </svg>
                """

                
                icon_link_html = f"""
                    <a href="{youtube_link}" target="_blank" rel="noopener noreferrer" style="text-decoration: none; color: inherit;">
                        {icon_html}
                        Visit the channel by Clicking Here
                    </a>
                """

                st.markdown(icon_link_html, unsafe_allow_html=True)
        def charts():
            query = text('select * from videos where channel_id=:channel_id')
            data=pd.read_sql_query(query,engine,params={'channel_id': channel_id})
            data['Views']=data['Views'].astype('int')
            data['Likes']=data['Likes'].fillna(0).astype('int')
            data['Comments'] = data['Comments'].fillna(0).astype('int')


            def video_publish_chart():
                data['Published_Date'] = pd.to_datetime(data['Published_Date'])
                data['Upload_Month'] = data['Published_Date'].dt.to_period('M')
                video_count_per_month = data.groupby('Upload_Month')['Video_Id'].count().reset_index()
                video_count_per_month.columns = ['Upload_Month', 'Video_Count']
                video_count_per_month['Upload_Month'] = video_count_per_month['Upload_Month'].dt.to_timestamp()
                fig = px.line(video_count_per_month, x='Upload_Month', y='Video_Count', title='Monthly Wise Video Upload Trend', markers=True)
                fig.update_traces(line=dict(color='yellow'))
                fig.update_layout(xaxis_title='Upload Month', yaxis_title='Number of Videos')
                st.plotly_chart(fig, use_container_width=True)   
            video_publish_chart()  


            def views_trends_chart():
                data['Published_Date'] = pd.to_datetime(data['Published_Date'])
                data['Upload_Month'] = data['Published_Date'].dt.to_period('M')
                views_per_month = data.groupby('Upload_Month')['Views'].sum().reset_index()
                views_per_month.columns = ['Upload_Month', 'Total_Views']
                views_per_month['Upload_Month'] = views_per_month['Upload_Month'].dt.to_timestamp()
                fig = px.line(views_per_month, x='Upload_Month', y='Total_Views', title='Views Over The Time', markers=True)
                fig.update_traces(line=dict(color='yellow'))  
                fig.update_layout(xaxis_title='Month', yaxis_title='Total Views')  
                st.plotly_chart(fig, use_container_width=True)
            views_trends_chart()

            def duration_vs_views():
                data['Duration_minutes'] = pd.to_timedelta(data['Duration']).dt.total_seconds() / 60

                fig = px.scatter(data, x='Duration_minutes', y='Views', color='Duration_minutes', 
                                hover_data=['Title', 'Channel_Name'], title='Video Duration vs Views',
                                color_continuous_scale='Viridis') 

                fig.update_layout(xaxis_title='Duration (minutes)', yaxis_title='Views')
                fig.update_traces(marker=dict(size=15, opacity=1), selector=dict(mode='markers'))  
                fig.update_coloraxes(colorbar_title='Duration (minutes)')

                st.plotly_chart(fig, use_container_width=True)
            duration_vs_views()
            
            def views():
                col1,col2=st.columns([3,2])
                data['Hour'] = data['Published_Date'].dt.strftime('%I %p') 
                data['Day_of_Week'] = data['Published_Date'].dt.day_name()

                hourly_performance = data.groupby('Hour').agg({'Views': 'sum'}).reset_index()
                weekday_performance = data.groupby('Day_of_Week').agg({'Views': 'sum'}).reset_index()

                weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                weekday_performance['Day_of_Week'] = pd.Categorical(weekday_performance['Day_of_Week'], categories=weekday_order, ordered=True)
                weekday_performance = weekday_performance.sort_values('Day_of_Week')

                colors = ['#8e44ad', '#3F0648'] 
                fig_views_hour = px.bar(hourly_performance, x='Hour', y='Views', title='Views Chart Based On The Time You Upload The Video', color='Views',
                                        color_continuous_scale=colors)
                fig_views_hour.update_layout(xaxis_title='Hour', yaxis_title='Views')
                with col1:
                    st.plotly_chart(fig_views_hour, use_container_width=True)
                    
                    fig_views_day = px.bar(weekday_performance, x='Day_of_Week', y='Views', title='Views Chart by Day of the Week', color='Views',
                                        color_continuous_scale=colors)
                    fig_views_day.update_layout(xaxis_title='Day of the Week', yaxis_title='Views')
                with col2:
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/upload.mp4"
                    video_html = f"""
                        <video width="80%" autoplay loop muted playsinline>
                            <source src="{video_url}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                        """
                    components.html(video_html, height=300)
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                col1,col2=st.columns([2,3])
                with col1:
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    st.write('')
                    video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/weekdays.mp4"
                    video_html = f"""
                            <video width="90%" autoplay loop muted playsinline>
                                <source src="{video_url}" type="video/mp4">
                                Your browser does not support the video tag.
                            </video>
                            """
                    components.html(video_html, height=300)
                with col2:
                    st.plotly_chart(fig_views_day, use_container_width=True)
            views()
            st.write('')
            st.write('')
            st.write('')
            col1,col2=st.columns([3,2])
            with col1:
                def top_10videos_by_view():
                    sorted_views = data.sort_values(by="Views", ascending=False).head(10)
                    colors = ['#8e44ad','#FDFDFF', '#3F0648'] 
                    fig_views = px.bar(sorted_views, x="Title", y="Views", title="Top 10 Videos by Views",color='Title',color_discrete_sequence=colors)
                    fig_views.update_layout(xaxis_title='Total Views', yaxis_title='', 
                                            plot_bgcolor='rgba(0,0,0,0)', 
                                            paper_bgcolor='rgba(0,0,0,0)',  
                                            font=dict(color='white'),  
                                            title_font_color='white',  
                                            showlegend=False,
                                            xaxis=dict(showticklabels=False)) 
                    st.plotly_chart(fig_views, use_container_width=True) 
                top_10videos_by_view()
            with col2:
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/views.mp4"
                video_html = f"""
                        <video width="98%" autoplay loop muted playsinline>
                            <source src="{video_url}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                        """
                components.html(video_html, height=300)
            
            st.write('')
            st.write('')
            st.write('')
            col1,col2=st.columns([2,3])
            with col2:
                def top_10_videos_by_like():
                    sorted_likes = data.sort_values(by="Likes", ascending=False).head(10)
                    colors = ['#8e44ad','#FDFDFF', '#3F0648']
                    fig_likes = px.bar(sorted_likes, x="Title", y="Likes", title="Top 10 Videos by Likes",color='Title',color_discrete_sequence=colors)
                    fig_likes.update_layout(xaxis_title='Total Likes', yaxis_title='',
                                            plot_bgcolor='rgba(0,0,0,0)',  
                                            paper_bgcolor='rgba(0,0,0,0)', 
                                            font=dict(color='white'), 
                                            title_font_color='white',  
                                            showlegend=False,
                                            xaxis=dict(showticklabels=False))
                    st.plotly_chart(fig_likes, use_container_width=True)
                top_10_videos_by_like()
            with col1:
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/likes.mp4"
                video_html = f"""
                        <video width="100%" autoplay loop muted playsinline>
                            <source src="{video_url}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                        """
                components.html(video_html, height=300)
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            col1,col2=st.columns([3,2])
            with col1:
                def sentiment_chart():
                    query = text('select * from comments where channel_id=:channel_id')
                    df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                    df['sentiment']=df['Comment_Text'].apply(analyze_sentiment)
                    sentiment_counts = df['sentiment'].value_counts()
                    #colors = ['gold', 'mediumturquoise', 'darkorange', 'lightgreen']
                    colors = ['gold','#3F0648','#FDFDFF']
                    fig = go.Figure(data=[go.Pie(
                        labels=sentiment_counts.index,
                        values=sentiment_counts.values,
                        marker=dict(colors=colors[:len(sentiment_counts)], line=dict(color='#FDFDFF', width=4)))])
                    fig.update_traces(
                        hoverinfo='label+percent', 
                        textinfo='value', 
                        textfont_size=20)

                    fig.update_layout(title_text='Sentiment Analysis For Comments')
                    st.plotly_chart(fig, use_container_width=True)
                sentiment_chart()
            with col2:
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                st.write('')
                video_url = "https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/comments.mp4"
                video_html = f"""
                        <video width="95%" autoplay loop muted playsinline>
                            <source src="{video_url}" type="video/mp4">
                            Your browser does not support the video tag.
                        </video>
                        """
                components.html(video_html, height=300)
        charts()


elif selected=='Filter Options':
    
    col1,col2=st.columns(2)
    with col1:
            st.image('https://github.com/Shobana1310/Youtube-Dataharvesting-and-Warehousing-using-sql-MongoDB-Streamlit/raw/main/images/compare.png',width=350)
    with col2:
        st.write('')
        st.write('')
        st.write('')
        st.title(':yellow[Compare The Others Channel Videos or Analyse Your Own Videos]')
    method = st.radio('Select The Options', ('All Channels', 'Your Channel'),horizontal=True, index=None, format_func=lambda x: x)
    if method is None:
        st.write('No option selected yet.')
        st.write('')
        st.write('')
        st.write('')
       
        
    if method=='Your Channel':
        channel_id= st.text_input(label="", placeholder="Enter The Youtube Channel ID")
        st.write('')
        st.write('')
        if channel_id:
            question = st.selectbox("Select The Query", [
                "Select a query...",
                "1.Show The List Of MonthWise Video",
                "2.Show All My Short Video With Views",
                "3.Show All My Long Video With Views",
                "4.Show Top 10 Videos In My channel With High Views and Likes",
                "5.Show Least Videos In My Channel with Low views(less than 500 views) and likes",
                "6.Show The Videos With Highest Comments"])

            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')

            
            if question=='1.Show The List Of MonthWise Video':
                def list_video():
                    query = text('select FORMAT(CAST(published_date AS datetime), \'MMMM\') AS month, YEAR(CAST(published_date AS datetime)) AS year, FORMAT(CAST(published_date AS datetime), \'MMM dd, yyyy\') AS date,thumbnail,title, description,published_date,views,likes,comments,duration from videos WHERE channel_id=:channel_id')
                    df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                    df['views']=df['views'].astype('int')
                    df['likes']=df['likes'].astype('int')
                    month=df['month'].unique()
                    year=np.sort((df['year']).unique())
                    tab1,tab2=st.columns(2)
                    with tab1:
                        container =  st.container(border = True)
                        container.subheader("Select The Month & Year")
                        month_radio= st.radio("Choose Month", month, horizontal=True, index=0)
                        year_radio= st.radio("Choose Year", year, horizontal=True, index=0)
                 

                    st.write('')
                    st.write('')
                    on = st.toggle("Show Without Expander")
                    st.write('')
                    st.write('')
                    st.write('')
                    
                    cols = 4
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['comments']=df['comments'].fillna(0)
                    df['comments']=df['comments'].astype('int64')
                    df['views'] = df['views'].apply(format_num)
                    df=df.loc[(df['month']==month_radio) & (df['year']==year_radio)][['thumbnail','title','views','likes','comments','date','duration']].sort_values(by='duration',ascending=False)
                    if not df.empty:
                        if not on:
                            for i in range(0, len(df), cols):
                                cols_list = st.columns(cols)
                                for j, col in enumerate(cols_list):
                                    if i + j < len(df):
                                        col.image(df["thumbnail"].iloc[i + j], width=250)
                                        with col.expander("**Show Details**"):
                                            st.write(f"**Title -** {df['title'].iloc[i + j]}")
                                            st.write(f"**Views -** {df['views'].iloc[i + j]}")
                                            st.write(f"**Likes -** {df['likes'].iloc[i + j]}")
                                            st.write(f"**Comments Count -** {df['comments'].iloc[i + j]}")
                                            st.write(f"**Uploaded Date -** {df['date'].iloc[i + j]}")
                                st.write('')
                                st.write('')
                                st.write('')
                                st.write('')
                                st.write('')
                                st.write('')
                                st.write('')
                    
                        if on:              
                            for i in range(0, len(df), cols):
                                cols_list = st.columns(cols)
                                for j, col in enumerate(cols_list):
                                    if i + j < len(df):
                                        col.image(df["thumbnail"].iloc[i + j], width=250)
                                        col.write(f"**Title -** {df['title'].iloc[i + j]}")
                                        col.write(f"**Views -** {df['views'].iloc[i + j]}")
                                        col.write(f"**Likes -** {df['likes'].iloc[i + j]}")
                                        col.write(f"**Comments Count -** {df['comments'].iloc[i + j]}")
                                        col.write(f"**Uploaded Date -** {df['date'].iloc[i + j]}")
                                col.write('')
                                col.write('')
                                col.write('')
                                col.write('')
                                col.write('')
                                col.write('')
                    else:
                        st.header(f'You Not Post Any Video On {month_radio} month & {year_radio} year')
                list_video()

            elif question=='2.Show All My Short Video With Views':
                query = text('select thumbnail,views,title,duration from videos where channel_id=:channel_id')
                df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                df['views']=df['views'].astype('int')
                df['title'] = df['title'].fillna('No Title')
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df=df.loc[df['duration_seconds']<=60].sort_values(by='views',ascending=False)
                df['views'] = df['views'].apply(format_num)

                cols=4
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["thumbnail"].iloc[i + j], width=250)
                                col.write(df['title'].iloc[i + j])
                                col.write(f" {df['views'].iloc[i + j]} Views")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
            elif question=='3.Show All My Long Video With Views':
                query = text('select thumbnail,views,title,duration from videos where channel_id=:channel_id')
                df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                df['views']=df['views'].astype('int')
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df=df.loc[df['duration_seconds']>60].sort_values(by='views',ascending=False)
                df['views'] = df['views'].apply(format_num)

                cols=4
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["thumbnail"].iloc[i + j], width=250)
                                col.write(df['title'].iloc[i + j])
                                col.write(f" {df['views'].iloc[i + j]} Views")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
            
            elif question=="4.Show Top 10 Videos In My channel With High Views and Likes":
                query = text('select thumbnail,views,likes,title from videos where channel_id=:channel_id')
                df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                df['views']=df['views'].astype('int')
                df['likes']=df['likes'].astype('int')
                df = df.sort_values(by=['views', 'likes'], ascending=False).head(10)
                df['views'] = df['views'].apply(format_num)
                df['likes']=df['likes'].apply(format_num)
                cols=3
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["thumbnail"].iloc[i + j], width=250)
                                col.write(df['title'].iloc[i + j])
                                col.write(f" {df['views'].iloc[i + j]} Views")
                                col.write(f" {df['likes'].iloc[i + j]} Likes")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
            
            elif question=="5.Show Least Videos In My Channel with Low views(less than 500 views) and likes":
                query = text('select thumbnail,views,likes,title from videos where channel_id=:channel_id and views <500 order by views asc,likes asc')
                df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                df['views']=df['views'].astype('int')
                df['likes']=df['likes'].astype('int')
                df=df.loc[df['views']<500].sort_values(by=['views', 'likes'], ascending=True)
                if not df.empty:
                    df['views'] = df['views'].apply(format_num)
                    df['likes']=df['likes'].apply(format_num)
                    cols=3
                    for i in range(0, len(df), cols):
                            cols_list = st.columns(cols)
                            for j, col in enumerate(cols_list):
                                if i + j < len(df):
                                    col.image(df["thumbnail"].iloc[i + j], width=250)
                                    col.write(df['title'].iloc[i + j])
                                    col.write(f" {df['views'].iloc[i + j]} Views")
                                    col.write(f" {df['likes'].iloc[i + j]} Likes")
                            col.write('')
                            col.write('')
                            col.write('')
                            col.write('')
                            col.write('')
                            col.write('')
                else:
                    st.header('There is no videos below 500 views in your Channel')
            elif question=="6.Show The Videos With Highest Comments":
                query = text('select  b.Thumbnail as thumbnail,b.title as title,count(a.comment_text)as cnt  from comments as a inner join videos as b on a.video_id=b.video_id where a.channel_id =:channel_id group by  b.Thumbnail, b.title ')
                df= pd.read_sql_query(query, engine, params={'channel_id': channel_id})
                df = df.sort_values(by='cnt', ascending=False).head(10)
                cols=3
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["thumbnail"].iloc[i + j], width=250)
                                col.write(df['title'].iloc[i + j])
                                col.write(f" {df['cnt'].iloc[i + j]} Comments")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')

    if method=='All Channels':
        method = st.radio('Select', ('Long Video Analysis', 'Short Video Analysis'),horizontal=True, index=None, format_func=lambda x: x)

        if method is not None:
            st.write(f'You selected: {method}')
        else:
            st.write('No option selected yet.')
        
        if method=='Long Video Analysis':

            question=st.selectbox("Compare The Channel", (  "Select a query...",
                                                            "1.Show all the Channels In the Database",
                                                            "2.Top 20 Famous Videos among all the channels",
                                                            "3.Top 10 most Liked videos by People",
                                                            "4.Show the Highest Comment Video",
                                                            "5.Channels publishing videos in By Year wise",
                                                            "6.Average video duration for each channel",
                                                        ))
            st.write("You selected:",question)
            st.write('')
            st.write('')
            st.write('')
            if 'delete_step' not in st.session_state:
                st.session_state['delete_step'] = 0
            if 'add_step' not in st.session_state:
                st.session_state['add_step'] = 0

            def reset_steps():
                st.session_state['delete_step'] = 0
                st.session_state['add_step'] = 0
            def handle_delete():
                Channel_name = st.text_input("Enter The YouTube Channel Name")
                if st.button("Confirm Delete"):
                    if Channel_name:
                        delete_query1 = text("DELETE FROM channels WHERE channel_name = :Channel_name")
                        delete_query2=text("DELETE FROM videos WHERE channel_name = :Channel_name")
                        with engine.connect() as conn:
                            #conn.execute(text("SET sql_safe_updates = 0;"))
                            conn.execute(delete_query1, {'Channel_name': Channel_name})
                            conn.execute(delete_query2, {'Channel_name': Channel_name})
                            #conn.execute(text("SET sql_safe_updates = 1;"))
                            conn.commit()
                        st.success(f'Channel "{Channel_name}" has been successfully deleted.')
                        result1 = collection.delete_many({"channel_details.channel_name": Channel_name})
                        result2= collection.delete_many({"videos_details.Channel_Name": Channel_name})
                        if result1.deleted_count > 0 or result2.deleted_count > 0:
                            st.success(f'Channel "{Channel_name}" has been successfully deleted from MongoDB.')
                        else:
                            st.warning(f'No document found in MongoDB with channel name "{Channel_name}".')
                        reset_steps()
            def handle_add():
                channel_id = st.text_input("Enter The YouTube Channel Id")
                if st.button("Confirm Add"):
                    if channel_id:
                        ids = []
                        database = client["youtube_data_harvesting"]
                        collection = database["channel_informations"]
                        for i in collection.find({}, {"_id": 0, "channel_details": 1}):      
                            for j in range(len(i["channel_details"])):
                                ids.append(i["channel_details"][j]["channel_id"])
                        
                        if channel_id in ids:
                            with st.spinner("Getting The Channel Data..."):
                                update_the_channel_details(channel_id)
                                tables()
                        else:
                            with st.spinner("Getting The Channel Data..."):
                                get_allthe_details_of_channel(channel_id)
                                tables()
                        st.success(f'Channel "{channel_id}" has been successfully Uploaded.')
                        reset_steps()

            if question == "1.Show all the Channels In the Database":
                st.write('')
                st.write('')
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(':red[If You Want to Delete Or Add The Channel To DataBase, Follow The Steps Below]')
                    
                    
                    if st.button("DELETE"):
                        st.session_state['delete_step'] = 1
                    
                    if st.button("ADD"):
                        st.session_state['add_step'] = 1 
                   
                    if st.session_state['delete_step'] == 1:
                        handle_delete()
                          
                    if st.session_state['add_step'] == 1:
                        handle_add()

                query = '''select channel_name, profile_picture from channels'''
                df = pd.read_sql_query(query, engine)
                st.write('')
                st.write('')
                st.write('')
                cols=4
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                profile_picture = df["profile_picture"].iloc[i + j]
                                if profile_picture is not None:
                                    col.image(profile_picture, width=230)
                                    col.write('')
                                    col.write('')
                                    col.write('')
                                col.write(df['channel_name'].iloc[i + j])
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                st.write('')
                st.write('')
                with st.expander('Show The Data Of This Channels'):
                    query = '''select channel_name as Name, subscription_count as Subscribers, total_videos as Videos, channel_views as Views from channels'''
                    df = pd.read_sql_query(query, engine)
                    df['Views']=df['Views'].astype('int')
                    df['Subscribers']=df['Subscribers'].astype('int')
                    df['Videos']=df['Videos'].astype('int')
                    df['Subscribers'] = df['Subscribers'].apply(format_num)
                    st.table(df)

                

            
            if question=="2.Top 20 Famous Videos among all the channels":
                query='''select channel_name as Name,Title,Thumbnail,duration,views from videos'''
                df=pd.read_sql_query(query,engine)
                df['views']=df['views'].astype('int')
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df = df.loc[df['duration_seconds'] > 120].nlargest(20, 'views')
                df['views'] = df['views'].apply(format_num)
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['Title'].iloc[i+j]}")
                                col.write(f" {df['views'].iloc[i + j]} views")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,Title,Thumbnail,duration,views from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['views']=df['views'].astype('int')
                    df = df.loc[df['duration_seconds'] > 120].nlargest(20, 'views')[['Name','Title','views']].reset_index(drop=True)
                    df['views'] = df['views'].apply(format_num)
                    st.table(df)
        

            if question=="3.Top 10 most Liked videos by People":
                query='''select channel_name as Name,Title,Thumbnail,likes,duration from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df['likes']=df['likes'].fillna(0)
                df['likes']=df['likes'].astype('int')
                df = df.loc[df['duration_seconds'] > 120].nlargest(10, 'likes')
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['Title'].iloc[i+j]}")
                                col.write(f" {df['likes'].iloc[i + j]} Likes")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,Title,Thumbnail,likes,duration from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['likes']=df['likes'].fillna(0)
                    df['likes']=df['likes'].astype('int')
                    df = df.loc[df['duration_seconds'] > 120].nlargest(10, 'likes')[['Name','Title','likes']].reset_index(drop=True)
                    df['likes']=df['likes'].astype('int')
                    st.table(df)
            
            if question=="4.Show the Highest Comment Video":
                query='''select channel_name as Name,title,Thumbnail,duration,comments from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df['comments']=df['comments'].fillna(0)
                df['comments']=df['comments'].astype('int')
                df = df.loc[df['duration_seconds'] > 120].nlargest(20, 'comments')
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['title'].iloc[i+j]}")
                                col.write(f" {df['comments'].iloc[i + j]} comments")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,title,Thumbnail,duration,comments from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['comments']=df['comments'].fillna(0)
                    df['comments']=df['comments'].astype('int')
                    df = df.loc[df['duration_seconds'] > 120].nlargest(20, 'comments')[['Name','title','comments']].reset_index(drop=True)
                    df['comments']=df['comments'].astype('int')
                    st.table(df)
            if question=="5.Channels publishing videos in By Year wise":
                query='''select channel_name as name,title,thumbnail,year(published_date)as year,duration  from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df = df.loc[df['duration_seconds'] > 120]

                years=df['year'].sort_values().unique()
                year_radio= st.radio("Choose Year", years, horizontal=True, index=0)

                df=df.loc[df['year']==year_radio]
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['title'].iloc[i+j]}")
                                col.write(df['year'].iloc[i + j])
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                with st.expander('Show The Data Of This Videos'):
                    df=df[['name','title','year']].reset_index(drop=True)
                    st.table(df)
            if question=="6.Average video duration for each channel":
                query='''select  channel_name as name,duration from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration_seconds'] = df['duration'].apply(lambda x: parse_iso8601_duration(x).total_seconds())
                df_grouped = df.groupby('name')['duration_seconds'].mean().reset_index() 
                df_grouped['duration_minutes'] = (df_grouped['duration_seconds'] / 60).round().astype('int')
                df_grouped.rename(columns={"duration_minutes" : "AverageDuration In Mins"},inplace=True)
                st.table(df_grouped[['name','AverageDuration In Mins']])
                
        if method=='Short Video Analysis':
            question=st.selectbox("Compare The Channel", (  "Select a query...",
                                                            "1.Top 20 Famous Shorts among all the channels",
                                                            "2.Top 20 most Liked Shorts by People",
                                                            "3.Show the Highest Comment ShortVideo",
                                                            "4.Channels publishing videos Short Videos More"
                                                        ))
            st.write("You selected:",question)
            st.write('')
            st.write('')
            st.write('')

            if question=='1.Top 20 Famous Shorts among all the channels':
                query='''select channel_name as Name,Title,Thumbnail,duration,views from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df['views']=df['views'].astype('int')
                df = df.loc[df['duration_seconds'] <=60].nlargest(20, 'views')
                df['views'] = df['views'].apply(format_num)
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['Title'].iloc[i+j]}")
                                col.write(f" {df['views'].iloc[i + j]} views")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,Title,Thumbnail,duration,views from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['views']=df['views'].astype('int')
                    df = df.loc[df['duration_seconds'] <= 60].nlargest(20, 'views')[['Name','Title','views']].reset_index(drop=True)
                    df['views'] = df['views'].apply(format_num)
                    st.table(df)
            
            if question=='2.Top 20 most Liked Shorts by People':
                query='''select channel_name as Name,Title,Thumbnail,likes,duration from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df['likes']=df['likes'].fillna(0)
                df['likes']=df['likes'].astype('int')
                df = df.loc[df['duration_seconds'] <= 60].nlargest(20, 'likes')
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['Title'].iloc[i+j]}")
                                col.write(f" {df['likes'].iloc[i + j]} Likes")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,Title,Thumbnail,likes,duration from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['likes']=df['likes'].fillna(0)
                    df['likes']=df['likes'].astype('int')
                    df = df.loc[df['duration_seconds'] <=60].nlargest(20, 'likes')[['Name','Title','likes']].reset_index(drop=True)
                    st.table(df)

            if question=='3.Show the Highest Comment ShortVideo':
                query='''select channel_name as Name,title,Thumbnail,duration,comments from videos'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df['comments']=df['comments'].fillna(0)
                df['comments']=df['comments'].astype('int')
                df = df.loc[df['duration_seconds'] <= 60].nlargest(20, 'comments')
                df['comments']=df['comments'].astype('int')
                cols=5
                for i in range(0, len(df), cols):
                        cols_list = st.columns(cols)
                        for j, col in enumerate(cols_list):
                            if i + j < len(df):
                                col.image(df["Thumbnail"].iloc[i + j], width=200)
                                col.write('')
                                col.write('')
                                col.write(df['Name'].iloc[i + j])
                                col.write(f"**VideoTitle-** {df['title'].iloc[i+j]}")
                                col.write(f" {df['comments'].iloc[i + j]} comments")
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                        col.write('')
                with st.expander('Show The Data Of This Videos'):
                    query='''select channel_name as Name,title,Thumbnail,duration,comments from videos'''
                    df=pd.read_sql_query(query,engine)
                    df['duration'] = df['duration'].apply(parse_iso8601_duration)
                    df['duration_seconds'] = df['duration'].dt.total_seconds()
                    df['comments']=df['comments'].fillna(0)
                    df['comments']=df['comments'].astype('int')
                    df = df.loc[df['duration_seconds'] <= 60].nlargest(20, 'comments')[['Name','title','comments']].reset_index(drop=True)
                    df['comments']=df['comments'].astype('int')
                    st.table(df)
            if question=='4.Channels publishing videos Short Videos More':
                query='''select a.channel_name as name,a.profile_picture as picture,video_id,duration,published_date from channels as a inner join videos as b on a.channel_id=b.channel_id'''
                df=pd.read_sql_query(query,engine)
                df['duration'] = df['duration'].apply(parse_iso8601_duration)
                df['duration_seconds'] = df['duration'].dt.total_seconds()
                df = df.loc[df['duration_seconds'] <= 60]
                df['published_date'] = pd.to_datetime(df['published_date'])
                df['year_month'] = df['published_date'].dt.to_period('M')

                df_count = df.groupby(['name', 'year_month']).size().reset_index(name='video_count')
                df_count['year_month'] = df_count['year_month'].astype(str)
                users = df_count['name'].unique()
                for user in users:
                    col1,col2=st.columns(2)
                    with col1:
                        st.write('')
                        st.write('')
                        st.write('')
                        st.write('')
                        st.write('')
                        st.write('')
                        st.write('')
                        st.write('')
                        profile_picture= df[df['name'] == user]['picture'].values[0]
                        shorts_count=df.groupby('name')['video_id'].count().reset_index()
                        count = shorts_count[shorts_count['name'] == user]['video_id'].iloc[0]
                        st.image(profile_picture, width=230)
                        st.write('')
                        st.write('')
                        st.write(f'{user} Shorts Count: {count}')
                    with col2:
                        user_df = df_count[df_count['name'] == user]
                        fig = px.line(user_df, x='year_month', y='video_count',
                                    title=f'Monthly Shorts Uploads Trend for {user}',
                                    labels={'year_month': 'Month', 'video_count': 'Number of Videos'},
                                    markers=True)
                        fig.update_layout(xaxis_title='Month',
                                        yaxis_title='Number of Shorts',
                                        legend_title='User')
                        fig.update_traces(line=dict(color='yellow'))
                        st.plotly_chart(fig, use_container_width=True)





elif selected=="About":
    st.header(':orange[How its Works]')
    st.write('')
    st.subheader(':violet[1.Data Scraping]')
    st.write('Begin by leveraging the YouTube API to extract essential public data from channels.API allows for real-time data retrieval and ensures accuracy in capturing the latest metrics.')
    st.write('')
    st.subheader(':violet[2.Data Storage]')
    st.write('Once data is retrieved from YouTube, store it in MongoDB. MongoDB is chosen for its flexibility in handling unstructured data and its scalability, which accommodates large volumes of data efficiently')
    st.write('')
    st.subheader(':violet[3.Transfer data from MongoDB to SQL]')
    st.write('To facilitate structured querying and deeper analysis, transfer the relevant data from MongoDB to SQL databases. SQL databases offer robust querying capabilities, making it easier to perform complex data manipulations and generate detailed reports')
    st.write('')
    st.subheader(':violet[4.Data Analysis]')
    st.write('Utilize Pandas, a powerful Python library, for data manipulation tasks such as cleaning, transforming, and aggregating data. Pandas provides a user-friendly and efficient way to handle data, ensuring it is prepared for analysis')
    st.write('')
    st.subheader(':violet[5.Create Comprehensive Charts with Plotly]')
    st.write('Utilize Plotly, a Python graphing library, to create interactive and visually appealing charts and graphs. Plotly supports a wide range of chart types, making it ideal for showcasing metrics like subscriber growth over time, view count trends, and audience engagement metrics')
    st.write('')
    st.write('')
    st.write('')

    st.header(':orange[About Me]')
    st.write(":dizzy: I am Shobana, A Passionate into Data Science And Business Solutions. With a Strong Foundation In Data Analysis And Machine Learning, I Thrive On Uncovering Actionable Insights From Complex Datasets to Drive Strategic Decision-making And Enhance Business Performance. I Am Dedicated To Continuous Learning, Always Staying On Latest Trends And Technologies In The Field")
    st.write("##")
    linkedin_logo="https://img.icons8.com/fluent/48/000000/linkedin.png"          
    linkedin_url="https://www.linkedin.com/in/shobana-v-534b472a2"
    st.markdown(f"[![LinkedIn]({linkedin_logo})]({linkedin_url})")
    st.header(":e-mail:mailbox: Get In Touch With Me!")
     
    contact_form='''<form action="https://formsubmit.co/shobana13102001@gmail.com" method="POST">   
     <input type="hidden" name="_next" value="https://yourdomain.co/thanks.html">
     <input type="hidden" name="_captcha" value="false">
     <input type="text" name="name" placeholder="Your Name" required>
     <input type="email" name="email" placeholder="Your E-Mail ID here" "required>
     <textarea name="message" placeholder="Your Message Here"></textarea>
     <button type="submit">Send</button>
     </form>'''


    st.markdown(contact_form,unsafe_allow_html=True)
    def local_css(file_name):
        with open(file_name)as f:
            st.markdown(f"<style>{f.read()}</style>",unsafe_allow_html=True)
    local_css("style.css")

  


 


    








   





  








        

                




        









