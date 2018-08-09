# -*- coding: utf-8 -*-
import requests
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
#requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json
import time
import sys
from datetime import datetime
max_total_results = 100
radius_type = 'km'
LANG = 'en'
class OsintYouTube():
    
    def __init__(self, DB):
        
        # authentication pieces
        self.ytkey    = "AIzaSyCMRYRW7jkS4HFNnAENz3oDnGV7ziZkONo"
        
        # setup databases
        self.DB = DB
        
        # setup search
        self.timestamp = time.strftime('%Y-%b-%d_%H%M') 
    
    def setSearchID(self, eGUID):
        self.searchID = eGUID    
        
    def responseHandler(self, response, searchterm):
        
        if response.status_code == 200:
            results = json.loads((response.content).decode('utf_8', 'ignore'))
            return results        
        
        else:
            results = json.loads((response.content).decode('utf_8', 'ignore'))
            print("[!] <%s>:%s. %s." % (results['error']['code'],
                                        results['error']['errors'][0]['domain'],
                                        results['error']['errors'][0]['message']))
            return None       
        
    def ETLChannels2Graph(self, results, eGUIDs, TYPE):
        
        oGUIDs = []
        
        for r in results['items']:
            print(r)
            CATEGORY = "YouTubeChannel"
            DESC = 'Desc: %s, CommentCount-%s, subscriberCount-%s, viewCount-%s, videoCount-%s' % (
                         r['snippet']['description'],
                         r['statistics']['commentCount'],
                         r['statistics']['subscriberCount'],
                         r['statistics']['viewCount'],
                         r['statistics']['videoCount']
                         )
            DESC = "'%s'" % DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS1 = r['snippet']['title'].replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = r['statistics']['viewCount']
            CLASS3 = r['statistics']['subscriberCount']
            s = r['snippet']['publishedAt']
            s = s[:s.index('.')].replace(s[10], ' ')               
            ORIGIN = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(s,'%Y-%m-%d %H:%M:%S'))            
            ORIGINREF = "%s-%s" % (r['id'], ORIGIN)         
            LOGSOURCE = '%s' % r['snippet']['thumbnails']['default']['url'] 
            
            # Get the object ID and create an event based on the account creation ['publishedAt']
            oGUID = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE) 
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', oGUID, 'Object')
            CATEGORY = "AccountCreation"
            DESC = "%s created on %s." % (CLASS1, ORIGIN)
            CLASS1 = "AccountID: %s" % r['id']
            TIME = time.strftime('%H:%M:%S', time.strptime(s,'%Y-%m-%d %H:%M:%S'))
            DATE = time.strftime('%Y-%m-%d', time.strptime(s,'%Y-%m-%d %H:%M:%S'))
            ORIGIN = CLASS1   
            DTG = ('%s%s' % (DATE, TIME)).replace("-", "").replace(":", "")
            XCOORD = 0.0
            YCOORD = 0.0            
            eGUID = self.DB.insertEvent(TYPE, CATEGORY, DESC, CLASS1, TIME, DATE, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', eGUID, 'Event')
            self.DB.insertRelation(eGUID, "Event", "AccountCreation", oGUID, "Object")             
            oGUIDs.append({'guid': oGUID, 'chid': r['id']})
    
        # Iterate through the Videos and for each match the channel id to one in the Channels list. Each match is a relationship    
        for e in eGUIDs:
            for o in oGUIDs:
                if e['chid'] == o['chid']:
                    self.DB.insertRelation(o['guid'], 'Object', 'ChannelPosted', e['guid'], 'Event')        
        
    def ETLVideos2Graph(self, video_list): 

        TYPE = "SocialMedia"
        # Lists which will be filled by the ids of channels for follow on look-up and the Video GUIDs which will be matched for relationships
        channels = []
        eGUIDs = []
        
        # Go through the list of videos and for each build a 
        for video in video_list:
            
            CATEGORY = "YouTubeVideo"
            DESC = 'Title: %s, Desc-%s, viewCount-%s,' % (
                video['snippet']['title'],
                video['snippet']['description'],
                video['statistics']['viewCount']
            ) 
            DESC = "%s" % DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            # Calculate video populatiry for CLASS1. There isn't always a like and dislikecount
            if 'likeCount' in video['statistics']:
                LC = int(video['statistics']['likeCount'])
                if 'dislikeCount' in video['statistics']:
                    DC = int(video['statistics']['dislikeCount'])
                else:
                    DC = 0
            
            else:
                LC = 0
                DC = 0
            if 'viewCount' in video['statistics']:
                VC = int(video['statistics']['viewCount'])
            CLASS1 = VC + LC + VC
            s = video['snippet']['publishedAt']
            s = s[:s.index('.')].replace(s[10], ' ')  # Because there are trailing items in youtube's response             
            TIME = time.strftime('%H:%M:%S', time.strptime(s,'%Y-%m-%d %H:%M:%S'))
            DATE = time.strftime('%Y-%m-%d', time.strptime(s,'%Y-%m-%d %H:%M:%S'))
            ORIGIN = 'https://www.youtube.com/watch?v=%s' % video['id']
            ORIGINREF = '%s-%s' % (video['snippet']['channelId'], video['id'])
            LOGSOURCE = video['snippet']['thumbnails']['default']['url']
            DTG = ('%s%s' % (DATE, TIME)).replace("-", "").replace(":", "")
            XCOORD = 0.0
            YCOORD = 0.0
            eGUID =  self.DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', eGUID, 'Event')
            
            channels.append(video['snippet']['channelId'])
            eGUIDs.append({'guid': eGUID, 'chid': video['snippet']['channelId']})
        
        # Create a string from the unique channels in the list by making it into a set and remove annotations
        #https://www.googleapis.com/youtube/v3/channels?part=snippet%2Cstatistics&id=UCYYRqfr2_OohJXPWQlkMEPw%2C+UCL3V1vHUDLb3kgouAtcGWvA%2C+UC8khxeFUw54fVrB4jzJgy-w%2C+UCaZv3iMTUibHDRyagA5h2HA%2C+UC6ry6jhDUjceu5aXLe_LxjQ%2C+UC8kRrvGrZql5SHjtfgsBafw&key=AIzaSyCMRYRW7jkS4HFNnAENz3oDnGV7ziZkONo
        if len(channels) < 50:
            channelSet = str(set(channels)).replace('{', '').replace('}', '').replace("'", '').replace(',', '%2C+').replace(' ', '')
            results = self.sendChannelRequest(channelSet)
            if results != None:
                self.ETLChannels2Graph(results, eGUIDs, TYPE)     
        else:
            divPoint = int(len(channels)/2)
            channelSets = []
            channelSets.append(str(set(channels[:divPoint])).replace('{', '').replace('}', '').replace("'", '').replace(',', '%2C+').replace(' ', ''))
            channelSets.append(str(set(channels[divPoint:])).replace('{', '').replace('}', '').replace("'", '').replace(',', '%2C+').replace(' ', ''))
            for channelSet in channelSets:
                results = self.sendChannelRequest(channelSet)
                if results != None:
                    self.ETLChannels2Graph(results, eGUIDs, TYPE)                   

    
    def sendIDRequest(self, id_list):
    
        api_url  = "https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&"
        api_url += "id=%s&" % ",".join(id_list)
        api_url += "key=%s" % self.ytkey
        response = requests.get(api_url)
        results = self.responseHandler(response, id_list)
        return results
    
    def sendChannelRequest(self, channels):
        # channels.list?id=UC_x5XG1OV2P6uZZ5FSM9Ttw&part=snippet%2CcontentDetails%2Cstatistics
        api_url  = "https://www.googleapis.com/youtube/v3/channels?part=snippet%2Cstatistics&"
        api_url += "id=%s&" % channels
        api_url += "key=%s" % self.ytkey
        response = requests.get(api_url)
        results = self.responseHandler(response, channels)
        if results != None:
            return results
        else:
            print("   - ApiUrl used: %s" % api_url)
    
    def sendGeoRadiusRequest(self, latitude, longitude, radius, token=None):
        
        api_url  = "https://www.googleapis.com/youtube/v3/search?type=video&maxResults=50&"
        api_url += "locationRadius=14km&order=viewCount&part=id,snippet&"
        api_url += "location=%f,%f&" % (latitude, longitude)
        api_url += "locationRadius=%d%s" % (radius, radius_type)
        api_url += "&key=%s" % self.ytkey
        
        if token is not None:
            api_url += "&pageToken=%s" % token
        response = requests.get(api_url)
        results = self.responseHandler(response, '%s,%s' % (latitude, longitude))
        return results
        
    def sendKeyWordRequest(self, keywords, page=None):
        
        api_url  = "https://www.googleapis.com/youtube/v3/search?part=id,snippet&type=video&maxResults=50"
        api_url += "&q=%s" % keywords
        api_url += "&key=%s" % self.ytkey
        if page is not None:
            api_url += "&pageToken=%s" % page
        response = requests.get(api_url)
        results = self.responseHandler(response, keywords)
        return results
        
    def getVideosByLocation(self, latitude, longitude, radius):
        
        video_list = []
        next_page  = None
        first_run  = True
        results = self.sendGeoRadiusRequest(latitude, longitude, radius)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OYT-getVideosByLocation]: Total videos for %f,%f %d: %d" % (TS, latitude, longitude, radius, results['pageInfo']['totalResults']))          
        
        while next_page is not None or first_run is True:
            next_page = results.get("nextPageToken")
            
            if results is not None:
                id_list = []
                
                for video in results['items']:
                    id_list.append(video['id']['videoId'])
                    
                results = self.sendIDRequest(id_list)
                video_list.extend(results['items'])
                print("[*] Retrieved %d results" % len(video_list))
                if len(video_list) >= max_total_results:
                    break
    
            results = self.sendGeoRadiusRequest(latitude, longitude, radius, next_page)
            first_run = False
            
        self.ETLVideos2Graph(video_list)

    
    def getVideosByKeyWords(self, keywords):
        
        vlist = []
        next_page  = None
        first_run  = True
        results = self.sendKeyWordRequest(keywords)
        print("[*] Total videos for %s - %d" % (keywords, results['pageInfo']['totalResults']))
        
        while next_page is not None or first_run is True:
            next_page = results.get("nextPageToken")
            
            if results is not None:
                id_list = []
                
                for video in results['items']:
                    id_list.append(video['id']['videoId'])
                    
                video = self.sendIDRequest(id_list)
                vlist.extend(video['items'])
                print("[*] Retrieved %d results" % len(vlist))
                if len(vlist) >= max_total_results:
                    break
    
            # ask for the next set of search results
            results = self.sendKeyWordRequest(keywords, next_page)
            if results == None:
                return vlist
            first_run = False
        self.ETLVideos2Graph(vlist) 
        
        return
    
# Command Line
#oy = OsintYouTube()
#latitude    = 52.5200
#longitude   = 13.4050
#radius      = 20
#radius_type = "km"
#video_list        = oy.getVideosByKeyWords(search_term)
#Lvideo_list       = oy.getVideosByLocation(latitude, longitude, radius)
#oy.ETLVideos2Graph(video_list)
#oy.ETLVideos2Graph(Lvideo_list)
