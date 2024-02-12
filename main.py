from googleapiclient.discovery import build
import pymongo
from sqlalchemy import create_engine
from sqlalchemy import text
from googleapiclient.errors import HttpError
import pandas as pd
from dotenv import load_dotenv
import os
import streamlit as st
import pymysql
import requests
from streamlit_lottie import st_lottie
from streamlit_option_menu import option_menu
import time
from PIL import Image

#These steps for keep our password safely by using Environment variables
load_dotenv()
mysql_password = os.getenv("MYSQL_PASSWORD") #MySQLpassword
api_key = os.getenv("API_KEY") #Youtube_API_V3
mongo_atlas_user_name = os.getenv("MONGO_ATLAS_USER_NAME") #Mongo_Atlas_User_name
mongo_atlas_password =  os.getenv("MONGO_ATLAS_PASSWORD")  #Mongo_Atlas_password

# we connect engine for sql and cilent for mongodb
engine=create_engine(f"mysql+pymysql://root:{mysql_password}@localhost:3306/youtubedata_harvest")
client=pymongo.MongoClient(f"mongodb+srv://{mongo_atlas_user_name}:{mongo_atlas_password}@cluster0.ehfepgy.mongodb.net/?retryWrites=true&w=majority")

#API Connection
def google_api_client():
    service=build("youtube","v3",developerKey=api_key)    #Build the YouTube API service using the provided API key
    return service
youtube=google_api_client()

# this is the function for get details of the channel
def get_details_of_channel(channel_id):
    response = youtube.channels().list(part="snippet,contentDetails,statistics", id=channel_id).execute()
    data_list = [{                                                         # retrive the data from response of API
            "channel_name": i["snippet"]["title"],
            "channel_id": i["id"],
            "subscription_count": i["statistics"]["subscriberCount"],
            "channel_views": i["statistics"]["viewCount"],
            "Total_Videos" :i["statistics"]["videoCount"],
            "channel_description": i["snippet"]["localized"]["description"],
            "playlist_id": i["contentDetails"]["relatedPlaylists"]["uploads"]}
             for i in response["items"]]

    return data_list                                                     #Return the list containing details of the channel
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
    video_datas=[]                                                        #Initialize an empty list to store video details
    for i in video_ids:
        response=youtube.videos().list(part="snippet,contentDetails,statistics",id=i).execute()
        for j in response["items"]:                                         #iterate through items in the API response and extract relevant information
            data=dict(Channel_Name = j['snippet']['channelTitle'],
                        Channel_Id = j['snippet']['channelId'],
                        Video_Id = j['id'],
                        Title = j['snippet']['title'],
                        Thumbnail = j['snippet']['thumbnails']['default']['url'],
                        Description = j['snippet']['description'],
                        Published_Date = j['snippet']['publishedAt'],
                        Duration = j['contentDetails']['duration'],
                        Views = j['statistics']['viewCount'],
                        Likes = j['statistics'].get('likeCount'),
                        Comments = j['statistics'].get('commentCount'),
                        Favorite_Count = j['statistics']['favoriteCount'],
                        Definition = j['contentDetails']['definition'],
                        Caption_Status = j['contentDetails']['caption']) 
        video_datas.append(data)                                               #Append the video details into the video_datas of the list
    return video_datas                                                         #Return the list containing details of the videos



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
    st.balloons()
    return "UPDATED SUCCESSFULLY !"
#----------------------------------------------------------------------Finished data scraping and mongodb pushing functions--------------------------------------------------

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
                            playlist_id varchar(100))''')
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
#----------------------------------------------------------finished table creating,start streamlit functions-------------------------------------------------------------------------------------

# Set the configuration for the Streamlit app page, including title, icon, and layout.
st.set_page_config(page_title="WebApplication",page_icon=":tada:",layout="wide")


# Create a sidebar with a header and an option menu for the main menu.
with st.sidebar:
    st.header("YOUTUBE DATAHARVEST AND WAREHOUSE")
    selected=option_menu(menu_title="MAIN MENU",options=["Home","Project","About Me"], # Use a custom function option_menu to create a dropdown menu with icons.
                         icons=["house","laptop","envelope"],
                         menu_icon="cast",
                         default_index=0)
# Check the selected option from the sidebar and perform actions accordingly.
if selected=="Home":
    def load_lottiurl(url):       # Define a function to load Lottie animation JSON from a given URL
        r=requests.get(url)
        if r.status_code !=200:
            return None
        return r.json()

    lottie_coding=load_lottiurl("https://lottie.host/feeb72f9-aa02-4069-a981-217545f12905/bPYVLXXjDU.json")    # Load Lottie animation JSON from a specific URL.

    with st.container():                   # Create a container with two columns for layout.
        left_column,right_column=st.columns(2)       # Left column content
        with left_column:
            st.header(":rainbow[Hello This is shobana:wave:]")
            st.header("##")
            st.header("##")
            st.header("##")
            st.write("CLICK HERE")
            if st.button("Technology Used in This Project"):
                st.toast("Python")
                st.toast(" MongoDB Cloud Atlas" )
                st.toast("MySQL WorkBench")
                st.toast("YouTube Data Api")
                st.toast("Streamlit Application")
    st.header("OVERVIEW OF PROJECT")
    video_file=open("C:/Users/Shobana/Videos/Captures/new.mp4","rb")
    video_bytes=video_file.read()
    st.video(video_bytes)
    with right_column:                        # Right column content
            st_lottie(lottie_coding,height=300,key="coding")


elif selected=="Project":
    #this function for show the channel dataframe in streamlit
    def display_channels_table():
        data=[]
        database=client["youtube_data_harvesting"]           # Access the MongoDB collection containing channel information
        collection=database["channel_informations"]
        for i in collection.find({},{"_id":0,"channel_details":1}):     # Extract channel details from the MongoDB collection
            for j in range(len(i["channel_details"])):
                data.append(i["channel_details"][j])
        table=st.dataframe(data)
        return table

    #this function for show the videos dataframe in streamlit
    def display_videos_table():
        data=[]
        database=client["youtube_data_harvesting"]
        collection=database["channel_informations"]
        for i in collection.find({},{"_id":0,"videos_details":1}):
            for j in range(len(i["videos_details"])):
                data.append(i["videos_details"][j])
        table=st.dataframe(data)
        return table
    #this function for show the comments dataframe in streamlit
    def display_comments_table():
        data=[]
        database=client["youtube_data_harvesting"]
        collection=database["channel_informations"]
        for i in collection.find({},{"_id":0,"comment_details":1}):
            for j in range(len(i["comment_details"])):
                data.append(i["comment_details"][j])
        table=st.dataframe(data)
        return table




    # User input: Enter Channel_id
    channel_id=st.text_input("Enter the YouTube Channel_id")
    
    # Button to collect and store data
    if st.button("Scrap the data and Push into MongoDB"):                 
        ids=[]
        database=client["youtube_data_harvesting"]
        collection=database["channel_informations"]
        for i in collection.find({},{"_id":0,"channel_details":1}):       # Extract channel_ids from the MongoDB collection
                for j in range(len(i["channel_details"])):
                    ids.append(i["channel_details"][j]["channel_id"])
        
        if channel_id in ids:                                    # Check if the entered channel_id already exists in the MongoDB collection
            st.info("Given The Channel_id is Already Exist,Updating...")
            with st.spinner("Updating data..."):
               insert = update_the_channel_details(channel_id)
               st.success(insert)
        else:
             with st.spinner("Scraping data and pushing into MongoDB..."):
                insert = get_allthe_details_of_channel(channel_id)
                st.success(insert)
    # Button to migrate data to SQL tables
    if st.button("Transfer the Data into MYSQL"):
        Table=tables()
        st.success(Table)
    # Radio button to select and display tables (CHANNELS, VIDEOS, COMMENTS)
    show_table=st.radio("Select the Table",("CHANNELS","VIDEOS","COMMENTS"))
    if show_table=="CHANNELS":
        display_channels_table()
    elif show_table=="VIDEOS":
        display_videos_table()
    elif show_table=="COMMENTS":
        display_comments_table()


    # connect the sql
    engine=create_engine(f"mysql+pymysql://root:{mysql_password}@localhost:3306/youtubedata_harvest")
    question=st.selectbox("Do some Data Analysis Here",("1.List all video names and their channels",
                                                    "2.Identify channels with the most videos and their counts",
                                                    "3.Top 10 most viewed videos and their channels",
                                                    "4.Comment counts for each video and their names",
                                                    "5.Videos with the highest likes and their channels",
                                                    "6.Total likes for each video, with names",
                                                    "7.Total views for each channel, with names",
                                                    "8.Channels publishing videos in 2022",
                                                    "9.Average video duration for each channel",
                                                    "10.Videos with the highest comments and their channels"))
    st.write("You selected:",question)


    if question=="1.List all video names and their channels":
        query='''select title as NameoftheVideo,channel_name as channelName from videos'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["NameoftheVideo","channelName"]))
            

    elif question=="2.Identify channels with the most videos and their counts":
        query='''select channel_name as channelName ,total_videos as videos_of_channel from channels order by total_videos desc'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["channelName","videos_of_channel"]))

    elif question=="3.Top 10 most viewed videos and their channels":
        query='''select channel_name as channelName ,title as VideoName,views from videos order by views desc limit 10'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["channelName","VideoName","views"]))

    elif question=="4.Comment counts for each video and their names":
        query='''select channel_name as ChannelName,title as videoNames,comments from videos order by comments desc '''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["ChannelName","videoNames","comments"]))

    elif question=="5.Videos with the highest likes and their channels":
        query='''select channel_name as channelName, title as videoName,likes as videoLikes from videos order by likes desc '''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["channelName","videoName","videoLikes"]))

    elif question=="6.Total likes for each video, with names":
        query='''select title as videonames ,likes from videos  order by likes desc; '''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["videonames","likes"]))

    elif question=="7.Total views for each channel, with names":
        query='''select channel_name as ChannelName,sum(views) as channel_views from videos group by channel_name '''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["ChannelName","channel_views"]))

    elif question=="8.Channels publishing videos in 2022":
        query='''select channel_name as ChannelName ,title as VideoName, substr(published_date,1,10) as 
        Video_Released_date from videos where extract(year from Published_Date)=2022'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["ChannelName","VideoName","Video_Released_date"]))

    elif question=="9.Average video duration for each channel":
        query='''SELECT channel_name,concat(AVG(CAST(SUBSTRING(duration, 3, LENGTH(duration) - 3) as 
        DECIMAL))," Mins") AS average_duration FROM videos
        GROUP BY channel_name'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["channel_name","average_duration"]))

    elif question=="10.Videos with the highest comments and their channels":
        query='''select channel_name as ChannelName,title as VideoName, comments from videos order by comments desc'''
        df=pd.read_sql_query(query,engine)
        st.write(pd.DataFrame(df,columns=["ChannelName","VideoName","comments"]))


# Check if the selected option is "About Me" in the sidebar.
elif selected=="About Me":
    st.header("About Me")
    st.write(":dizzy: I am Shobana.Embarking on a journey from the world of medicine to the realm of data science,"
             "I found my true passion in unraveling the stories hidden within data. As a graduate with a medical degree," 
             "my insatiable curiosity led me to explore the exciting and dynamic field of data science."
             "This is My first project in data science was a rollercoaster of excitement and challenges. Amidst the sea of errors",
             "I found myself confronted with moments that made me pause, reflect, and even sit around for a day."
             "As I fixed mistakes and got past each challenge, I found joy in small wins that showed my progress." 
             "Starting with the very first line of code and finishing my first data science project,"
             "each success motivated me to keep exploring and pushing my limits.")
    st.write("##")
    linkedin_logo="https://img.icons8.com/fluent/48/000000/linkedin.png"            # Display LinkedIn logo with a link to the profile.
    linkedin_url="https://www.linkedin.com/in/shobana-v-534b472a2"
    st.markdown(f"[![LinkedIn]({linkedin_logo})]({linkedin_url})")
    st.header(":e-mail:mailbox: Get In Touch With Me!")
      # Display the contact form using markdown, allowing HTML content.
    contact_form='''<form action="https://formsubmit.co/shobana13102001@gmail.com" method="POST">   
     <input type="hidden" name="_next" value="https://yourdomain.co/thanks.html">
     <input type="hidden" name="_captcha" value="false">
     <input type="text" name="name" placeholder="Your Name" required>
     <input type="email" name="email" placeholder="Your E-Mail ID here" "required>
     <textarea name="message" placeholder="Your Message Here"></textarea>
     <button type="submit">Send</button>
     </form>'''
    st.markdown(contact_form,unsafe_allow_html=True)
    # Define a function to apply local CSS style from an external file
    def local_css(file_name):
        with open(file_name)as f:
            st.markdown(f"<style>{f.read()}</style>",unsafe_allow_html=True)
    local_css("style.css")
   

   





  








        

                




        
















        

                




        









