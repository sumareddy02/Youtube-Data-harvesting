from  googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st




#Api key connection
def Api_connect():
    apiKey="AIzaSyDKBW8rtXoiY3zHF6-pwnwKCVdLhSmMmN0"
    api_service_name = "youtube"
    api_version = "v3"
    youtube =build(api_service_name, api_version,developerKey=apiKey)
    return youtube

youtube=Api_connect()
#youtube


#fetching the channel details
def getChannelDetails(channelId):
    request = youtube.channels().list(part="snippet,contentDetails,statistics",id=channelId)
    channelData = request.execute()
    if  channelData['items']:

        channel_informations = dict(channel_name = channelData['items'][0]['snippet']['title'], 
                                    channel_id =channelData['items'][0]['id'],
                                    subscription_count = channelData['items'][0]['statistics']['subscriberCount'],
                                    channel_view=channelData['items'][0]['statistics']['viewCount'],
                                    Total_Videos=channelData['items'][0]['statistics']['videoCount'],
                                    channel_description = channelData['items'][0]['snippet']['description'],
                                    playlists = channelData['items'][0]['contentDetails']['relatedPlaylists']['uploads']  
        )
        return channel_informations

ChannelDetails=getChannelDetails("UCoxMiptAzTKHliTkBB2oCoA")
#ChannelDetails

#Fetching video ids

def getVideo_ids(channel_id):
    #get the content details and see whether i have playlist id 
    request=youtube.channels().list(part="contentDetails",id=channel_id)
    response=request.execute()

    
    #now from content details am fetching playlist id
    playListId=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
   # print(playListId)

    #to store vedio ids am creating a variable 
    video_ids=[]
    #next page variable used to check whether there is next page or not 
    nextPage=None
    while True :

        # fetching all the playlist ids from playlist id 
        req = youtube.playlistItems().list(part="contentDetails", playlistId=playListId,maxResults=50, pageToken=nextPage)
        playlistItems =req.execute()
       # print(playlistItems)
    
        #jsonData=json.dumps(videoDetails,indent=1)
        #print(jsonData)
        #from play list items am fetching vedio ids 
        for i in playlistItems["items"]:
            video_ids.append(i["contentDetails"]["videoId"]) #adding vedio ids
        nextPage=playlistItems.get("nextPageToken")# here to check wheter next page is present print next page token 
        if not nextPage: #if next page is not present break 
            break
    return video_ids 
        

videoIds=getVideo_ids("UCLW1TRVF11_y1a-FNa-nUVA")
#videoIds


def getVideoDetails(videoIds):
    video_data=[]
 
    for i in videoIds:
        request = youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
        videoResponse = request.execute()
        #print(videoResponse)
        for i in  videoResponse["items"]:
            videoDetails = dict(channel_name =i['snippet']['title'], 
                                channel_id =i['id'],
                                Video_Id= i['id'],
                                Video_Name= i['snippet']['title'] if 'title' in videoResponse['items'][0]['snippet'] else "Not Available",
                                Video_Description= i['snippet']['description'],
                                Tags=i['snippet'].get('tags'),
                                PublishedAt=i['snippet']['publishedAt'],
                                View_count=i['statistics'].get('viewCount'),
                                Like_count=i['statistics'].get('likeCount'),
                                Dislike_count=i.get('dislikeCount'),
                                Favorite_count=i['statistics']['favoriteCount'],
                                Comment_count=i['statistics'].get('commentCount'),
                                Duration=i['contentDetails']['duration'],
                                Caption=i['contentDetails']['caption'],
                                Thumbnail=i['snippet']['thumbnails']['default']['url']
                                )
        video_data.append(videoDetails)
    return video_data
      
video_details=getVideoDetails(videoIds)
#video_details


def getCommentDetails(videoIds):
    try:
        commentData=[]
        for i in videoIds:
            request = youtube.commentThreads().list(part="snippet",videoId=i)
            response =request.execute()
        

            for i in response['items']:
                commentDetails = dict(
                                        Comment_Id=i['snippet']['topLevelComment']['id'],
                                        Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                        Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        Comment_PublishedAt=i['snippet']['topLevelComment']['snippet']['publishedAt']
                                    )
                commentData.append(commentDetails)
        
    except:
        pass
    return commentData  
CommentDetails=getCommentDetails(videoIds)
#CommentDetails



#connection to mongodb
client=pymongo.MongoClient("mongodb+srv://suma:sumareddy@cluster0.7ktjn45.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube_data"]


#inserting data to mongodb using one def fuction consist of all functions

def channelInformation(channelId):
    ChannelDetails=getChannelDetails(channelId) 
    videoIds=getVideo_ids(channelId) 
    video_details=getVideoDetails(videoIds)
    CommentDetails=getCommentDetails(videoIds)

    collec=db["channelInformation"]
    collec.insert_one({"ChannelDetails " :ChannelDetails,"videoIds":videoIds,"video_details":video_details,"CommentDetails":CommentDetails})

    return "upload complete succesfully"


#insert=channelInformation("UCLW1TRVF11_y1a-FNa-nUVA")#lokesh reddy
#insert=channelInformation("UCoxMiptAzTKHliTkBB2oCoA")#Two type programming 
#insert=channelInformation("UC53cAvFeWsNwsfCQnJmLjDQ")#satvic movement
#insert=channelInformation("UCG9m7Ckfrl0gCVlIEtg-XFg")#kadhar life style


#table creation in sql 

def channelTable(single_channelName):

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sumareddy",
                        database="youtube_data",
                        port="5432"

    )

    cursor=mydb.cursor()


    #if already a table is present it drops existing table to crea a new table 
  #  drop_query='''drop table if  exists channel'''
   # cursor.execute(drop_query)
   # mydb.commit()"""

   
    create_query='''create table if not exists channel( channel_name varchar(100),
                                                        channel_id varchar(80) primary key, 
                                                        subscription_count bigint,
                                                        channel_view bigint,
                                                        Total_Videos int,
                                                        channel_description text,
                                                        playlists varchar(80) 
    )'''
    cursor.execute(create_query)
    mydb.commit()

   

    #fetching data from mongodb

    single_channel_detail=[]
    db=client["Youtube_data"]
    coll=db["channelInformation"]

    for ch_data in coll.find({"ChannelDetails .channel_name":single_channelName },{"_id":0,}):
        single_channel_detail.append(ch_data["ChannelDetails "])

    df_scd=pd.DataFrame(single_channel_detail) 
        #df

    #inserting rows data  into sql


    for index,row in df_scd.iterrows():
        insert_query='''insert into channel(channel_name ,
                                            channel_id , 
                                            subscription_count,
                                            channel_view ,
                                            Total_Videos,
                                            channel_description ,
                                            playlists)  
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['subscription_count'],
                row['channel_view'],
                row['Total_Videos'],
                row['channel_description'],
                row['playlists']
                )

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            err=f"Your Provided Channel Name {single_channelName} is Already Exists"
            return err 



#creating table for video details

def videosTable(single_channelName):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sumareddy",
                        database="youtube_data",
                        port="5432"

    )

    cursor=mydb.cursor()

    #drop_query='''drop table if  exists videos'''
    #cursor.execute(drop_query)
    #mydb.commit()


   
    create_query='''create table if not exists videos(  channel_name varchar(100),
                                                        channel_id varchar(100),
                                                        Video_Id varchar(30) primary key, 
                                                        Video_Name varchar(100),
                                                        Video_Description text,
                                                        Tags text,
                                                        PublishedAt timestamp,
                                                        View_count bigint,
                                                        Like_count bigint,
                                                        Dislike_count bigint,
                                                        Favorite_count bigint,
                                                        Comment_count bigint,
                                                        Duration interval,
                                                        Caption varchar(100),
                                                        Thumbnail varchar(200) )'''
    cursor.execute(create_query)
    mydb.commit()

    

    single_video_detail=[]
    db=client["Youtube_data"]
    coll=db["channelInformation"]

    for ch_data in coll.find({"ChannelDetails .channel_name":single_channelName },{"_id":0,}):
        single_video_detail.append(ch_data["video_details"])

    df_svd=pd.DataFrame(single_video_detail[0]) 

    #df1


    #inserting video details as rows data  into sql


    for index,row in df_svd.iterrows():
        insert_query='''insert into videos( channel_name,
                                            channel_id,
                                            Video_Id ,
                                            Video_Name ,
                                            Video_Description ,
                                            Tags ,
                                            PublishedAt ,
                                            View_count ,
                                            Like_count ,
                                            Dislike_count, 
                                            Favorite_count, 
                                            Comment_count  ,
                                            Duration ,
                                            Caption ,
                                            Thumbnail )  
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_name'],
                row['channel_id'],
                row['Video_Id'],
                row['Video_Name'],
                row['Video_Description'],
                row['Tags'],
                row['PublishedAt'],
                row['View_count'],
                row['Like_count'],
                row['Dislike_count'],
                row['Favorite_count'],
                row['Comment_count'],
                row['Duration'],
                row['Caption'],
                row['Thumbnail'],
                )

        cursor.execute(insert_query,values)
        mydb.commit()
    

#creating table for comment details

def commentTable(single_channelName):
    
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="sumareddy",
                        database="youtube_data",
                        port="5432"

    )

    cursor=mydb.cursor()


    
    create_query='''create table if not exists comments( Comment_Id varchar(30) primary key,
                                                    Video_id varchar(50),
                                                    Comment_Text  text,
                                                    Comment_Author varchar(100),
                                                    Comment_PublishedAt timestamp
                                                )'''
    cursor.execute(create_query)
    mydb.commit()

    

    single_comment_detail=[]
    db=client["Youtube_data"]
    coll=db["channelInformation"]

    for ch_data in coll.find({"ChannelDetails .channel_name":single_channelName },{"_id":0,}):
        single_comment_detail.append(ch_data["CommentDetails"])

    df_scomd=pd.DataFrame(single_comment_detail[0]) 

    #df2
    #inserting comment details as rows data  into sql
    for index,row in df_scomd.iterrows():
        insert_query='''insert into comments(Comment_Id ,
                                            Video_id,
                                            Comment_Text  ,
                                            Comment_Author,
                                            Comment_PublishedAt )  
                                            
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_PublishedAt']
                )

       
        cursor.execute(insert_query,values)
        mydb.commit()
    
def  tables(single_channel):

    err=channelTable(single_channel)
    if err:
        return err
    else:
        videosTable(single_channel)
        commentTable(single_channel)

        return "tables created successfully"



#Tables=tables()


def show_channelTable():

    Ch_list=[]
    db=client["Youtube_data"]
    coll=db["channelInformation"]

    for ch_data in coll.find({},{"_id":0,"ChannelDetails ":1}):
        Ch_list.append(ch_data['ChannelDetails '])

    df=st.dataframe(Ch_list)
    #df


def show_videosTable():
    viList=[]
    db=client['Youtube_data']
    coll=db["channelInformation"]
    for viData in coll.find({},{'_id':0,"video_details":1}):
        for i in range(len(viData['video_details'])):
            viList.append(viData["video_details"][i])
    df1=st.dataframe(viList)

#df1


def show_commentsTable():
    commentList=[]
    db=client['Youtube_data']

    coll=db["channelInformation"]
    for commentData in coll.find({},{'_id':0,"CommentDetails":1}):
        for i in range(len(commentData['CommentDetails'])):
            commentList.append(commentData["CommentDetails"][i])
    df2=st.dataframe(commentList)
    #df2


    #streamlit part

# side bar is somethig which is peresent in sides of page here it is present in left side
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]") # tittle of the page
    st.header("Technology Skills")
    st.caption("Data Collection")
    st.caption("Data management using MongoDB and SQL")

# user giving input for streammlit application
channel_id=st.text_input("Enter the channel ID")


if st.button("collect and store data"):
    ch_ids=[]# store channel ids  
    db=client["Youtube_data"] #   channel name db
    coll=db["channelInformation"] # collecting complete details of db
    for ch_data in coll.find({},{"_id":0,"ChannelDetails ":1}):
        ch_ids.append(ch_data["ChannelDetails "]["channel_id"]) #adding channel ids from collection

    if channel_id in ch_ids :
        st.success("channel details of the given channel Id already exists")
    else:
        insert=channelInformation(channel_id)
        st.success(insert)

all_channels=[]

db=client["Youtube_data"]
coll=db["channelInformation"]

for ch_data in coll.find({},{"_id":0,"ChannelDetails ":1}):
    all_channels.append(ch_data["ChannelDetails "]["channel_name"])

unique_channel=st.selectbox("Select the channel",all_channels )



if st.button("store data into sql") :
    Tables=tables(unique_channel)
    st.success(Tables)

#show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS")) 

#if show_table=="CHANNELS":
#   show_channelTable()

#elif show_table=="VIDEOS":
  #  show_videosTable()

#elif show_table=="COMMENTS":
 #   show_commentsTable()


mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="sumareddy",
                    database="youtube_data",
                    port="5432")

cursor=mydb.cursor()



question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))


if question=="1. All the videos and the channel name":
    query1='''select video_name as tittle,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"]) # names of column is given by us
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channel 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select view_count as views,channel_name as channelname,video_name as videotitle from videos 
                where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comment_count as no_comments,video_name as videotitle from videos where comment_count is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select video_name as videotitle,channel_name as channelname,like_count as likecount
                from videos where like_count is not null order by like_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select like_count as likecount,video_name as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,channel_view as totalviews from channel'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select video_name as video_title,publishedat as videorelease,channel_name as channelname from videos
                where extract(year from publishedat)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select video_name as videotitle, channel_name as channelname,comment_count as comments from videos where comment_count is
                not null order by comment_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)





