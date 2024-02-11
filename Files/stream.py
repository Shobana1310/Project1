

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
channel_id=st.text_input("Enter Your Channel_id")
 
# Button to collect and store data
if st.button("Collect and Store the Data"):                   
    ids=[]
    database=client["youtube_data_harvesting"]
    collection=database["channel_informations"]
    for i in collection.find({},{"_id":0,"channel_details":1}):       # Extract channel_ids from the MongoDB collection
            for j in range(len(i["channel_details"])):
                ids.append(i["channel_details"][j]["channel_id"])
    
    if channel_id in ids:                                    # Check if the entered channel_id already exists in the MongoDB collection
         st.success("Given The Channel_id is Already Exist")
    else:
         insert=get_allthe_details_of_channel(channel_id)
         st.success(insert)
# Button to migrate data to SQL tables
if st.button("Migrate to SQL"):
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
    