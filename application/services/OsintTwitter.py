#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import requests
from bs4 import BeautifulSoup as BS4
from requests_oauthlib import OAuth1
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
import json, time, sys, os, _locale
debugging = False
_locale._getdefaultlocale = (lambda *args: ['en_US', 'utf8'])
E_LANG = 'English'

class OsintTwitter():

    def __init__(self, DB, auth):
        
        if auth == None:
            if '\\' in os.getcwd():
                if debugging == False:
                    self.AUTH = ('%s\\application\\services\\config\\AUTH_Twitter.json' % (os.getcwd()))
                else:
                    self.AUTH = ('%s\\config\\AUTH_Twitter.json' % (os.getcwd())) # debugging line  
            else:
                if debugging == False:
                    self.AUTH   = ('%s/application/services/config/AUTH_Twitter.json' % (os.getcwd()))
                else:
                    self.AUTH   = ('%s/data/AUTH_Twitter.json' % (os.getcwd())) # debugging line 

            client_key    = self.AUTH['client_key']
            client_secret = self.AUTH['client_secret']
            token         = self.AUTH['token']
            token_secret  = self.AUTH['token_secret']        
        
        else:
            client_key    = auth['client_key'][0]
            client_secret = auth['client_secret'][0]
            token         = auth['token'][0]
            token_secret  = auth['token_secret'][0]

        self.Locations = []
        self.Verbose = False
        self.request = 0
        self.requestmax = 2
        
        # Base URL for all Twitter calls
        self.base_twitter_url = "https://api.twitter.com/1.1/"
        
        # setup authentication
        self.oauth  = OAuth1(client_key,client_secret,token,token_secret)
        
        # setup search
        self.timestamp = time.strftime('%Y-%b-%d_%H%M')
        self.DB = DB
        
    def setSearchID(self, eGUID):
        self.searchID = eGUID
        
    def responseHandler(self, response, searchterm):
        
        if response.status_code == 401:

            print("[!] <401> User %s protects tweets" % searchterm)
            return "[!] <401> User %s protects tweets" % searchterm
    
        if response.status_code == 200:
            tweets = json.loads(response.text)
            return tweets
    
        if response.status_code == 429:
            timestamp = time.strftime('%Y-%b-%d_%H%M')
            print("[!] <429> Too many requests to Twitter. Sleep for 15 minutes started at: %s" % timestamp )
            time.sleep(900)
            response = self.DB.getResponse(url, self.oauth)
            tweets = json.loads(response.text)
            return tweets
        
        if response.status_code == 503:
            print("[!] <503> The Twitter servers are up, but overloaded with requests. Try again later: %s" % timestamp )
            time.sleep(5)
            response = self.DB.getResponse(url, self.oauth)
            tweets = self.responseHandler(response, searchterm)      
            return tweets
        
        else:
            print("[!] Error:%s" % response)
            return None
    
    def processCoordinates(self, tweet):
        
        #set up query in case GMaps 
        q = {}
        q['Type'] = 'address'
        
        if 'location' in tweet.keys():
            if tweet['location'] != '':
                    XCOORD = tweet['location']
                    YCOORD = tweet['location']
            else:
                XCOORD = 0.00
                YCOORD = 0.00
        
        elif tweet['coordinates'] != None:
            if isinstance(tweet['coordinates'], dict) == True:
                XCOORD = tweet['coordinates']['coordinates'][0]
                YCOORD = tweet['coordinates']['coordinates'][1]
            else:
                XCOORD = tweet['coordinates'][0]
                YCOORD = tweet['coordinates'][1]
               
        elif tweet['place'] != None:    
            if 'bounding_box' in tweet['place']:
                if 'coordinates' in tweet['place']['bounding_box']:
                    if len(tweet['place']['bounding_box']['coordinates']) == 1:
                        if len(tweet['place']['bounding_box']['coordinates'][0]) > 1:
                            XCOORD = tweet['place']['bounding_box']['coordinates'][0][0][1]
                            YCOORD = tweet['place']['bounding_box']['coordinates'][0][0][0]        
            elif 'name' in tweet['place']:
                    XCOORD = tweet['location']
                    YCOORD = tweet['location']

        elif tweet['user']['location'] != None:
            loc = tweet['user']['location']
            if len(loc) < 2 or loc == 'None':
                if tweet['retweeted'] != False:
                    loc = tweet['retweeted_status']['user']['location']
                else:
                    return 0, 0
            q['Qstr'] = loc.replace("-", " ")
            XCOORD = 0.0
            YCOORD = 0.0        
                    
        return XCOORD, YCOORD
         
    def processTweets(self, tweet_list, search_term):
        '''
        Procedure to iterate through a list of tweets based on a search_term.
        All entity resolution and redundancy checks are downstream in the graphDB operations
        '''
        for tweet in tweet_list:
            if tweet != 'search_metadata' and tweet != 'statuses':
                #if self.Verbose == True:
                print(tweet)
                print("[*] Processing tweet %s for user %s" % (tweet['id'], tweet['user']['screen_name']))
                newRel = self.ETLTweet2HANA(tweet, search_term)
                if newRel == False:
                    return False
        
        return True
    
    def ETLTweet2HANA(self, tweet, search_term):
        '''
        Extraction of raw tweet information in JSON/dict format into the appropriate Object, Event and Location
        nodes. Nodes are created with attributes and then related to each other. 
        All entity resolution and redundancy checks are downstream in the graphDB opeations per node.
        '''
        # Create the TwitterUser Account
        XCOORD, YCOORD = self.processCoordinates(tweet)
        
        O_TYPE = "SocialMedia"
        O_CATEGORY = "TwitterUser"   
        O_DESC = "%s with id %s created on %s in %s by %s. Followers: %s, Following: %s, Statuses: %s, Listed: %s, Verified: %s. Protected: %s, CollectDate: %s" % (
            tweet['user']['screen_name'],
            tweet['user']['id'], 
            time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
            tweet['user']['location'],
            tweet['user']['name'],
            tweet['user']['followers_count'],
            tweet['user']['friends_count'],
            tweet['user']['statuses_count'],
            tweet['user']['listed_count'],
            tweet['user']['verified'],
            tweet['user']['protected'],
            self.timestamp
            )
        O_CLASS1 = tweet['user']['screen_name']
        O_CLASS2 = tweet['user']['followers_count']
        O_CLASS3 = tweet['user']['friends_count']
        O_ORIGIN = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(tweet['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))      
        O_ORIGINREF = "Twitter%s-%s" % (tweet['user']['id'], tweet['user']['url'])
        O_LOGSOURCE = '%s' % tweet['user']['profile_image_url']
        
        entity = {'TYPE' : 'Object', 'LOOKUP' : '%s%s%s%s' % (O_ORIGIN, O_ORIGINREF, O_TYPE, O_CATEGORY)}
        O_GUID, found = self.DB.EntityResolve(entity)
        if found == 0:
            O_GUID = self.DB.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', O_GUID, 'Object')

            # Create an Event for the creation of the account 
            E_TYPE = "SocialMedia"
            E_CATEGORY = "AccountCreation"
            E_DESC = "%s created on %s in timezone %s." % (tweet['user']['screen_name'], tweet['user']['created_at'], tweet['user']['location'])
            E_CLASS1 = "AccountID: %s" % tweet['user']['id']       
            E_TIME = time.strftime('%H:%M:%S', time.strptime(tweet['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            E_DATE = time.strftime('%Y-%m-%d', time.strptime(tweet['user']['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            E_DTG  = ('%s%s' % (E_TIME, E_DATE)).replace(":", "").replace("-", "")
            E_ORIGIN = tweet['user']['screen_name']       
            E_ORIGINREF = "Twitter%s-%s" % (tweet['user']['id'], tweet['user']['url'])  
            E_LOGSOURCE = '%s' % tweet['user']['time_zone']
            E_XCOORD = XCOORD
            E_YCOORD = YCOORD
            E_LANG = tweet['lang']
            
            entity = {'TYPE' : 'Event', 'LOOKUP' : '%s' % (E_DESC)}  
            E_GUID, found = self.DB.EntityResolve(entity)
            if found == 0:
                E_GUID = self.DB.insertEvent(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
                newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', E_GUID, 'Event')
            newRel = self.DB.insertRelation(E_GUID, 'Event', 'AccountCreated', O_GUID, 'Object') 
            if newRel == False:
                return newRel
         
        # Create an Event for the Tweet of the user
        E_TYPE = "SocialMedia"
        E_CATEGORY = "Tweet"
        E_DESC = tweet['text'].replace('"', "'")
        E_CLASS1 = tweet['retweet_count']      
        E_TIME = time.strftime('%H:%M:%S', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
        E_DATE = time.strftime('%Y-%m-%d', time.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))   
        E_DTG  = ('%s%s' % (E_TIME, E_DATE)).replace(":", "").replace("-", "")
        E_ORIGIN = tweet['user']['screen_name']        
        E_ORIGINREF = "Twitter%s-%s" % (tweet['user']['id'], tweet['id'])
        E_LOGSOURCE = '%s' % tweet['user']['profile_image_url']
        E_XCOORD = XCOORD
        E_YCOORD = YCOORD
        E_LANG = tweet['lang']
        
        entity = {'TYPE' : 'Event', 'LOOKUP' : '%s' % (E_DESC)}  
        E_GUID, found = self.DB.EntityResolve(entity)
        if found == 0:
            E_GUID = self.DB.insertEvent(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            newRel = self.DB.insertRelation(O_GUID, 'Object', 'Tweeted', E_GUID, 'Event')
            if newRel == False:
                return newRel  
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', E_GUID, 'Event')
            L_GUID = self.DB.insertLocation('Location', 'Associated with Social Media', XCOORD, YCOORD, 0, O_CLASS2, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            newRel = self.DB.insertRelation(E_GUID, 'Event', 'TweetLocation', L_GUID, 'Location')
            if newRel == False:
                return newRel 
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', L_GUID, 'Location')
    
        print(E_GUID)
    
    def ETLUser2HANA(self, user, search_term):
        
        XCOORD, YCOORD = self.processCoordinates(user)
        
        O_TYPE = "SocialMedia"
        O_CATEGORY = "TwitterUser"
        O_DESC = "ScreenName:%s, Id:%s, CreatedOn:%s, Location:%s, PersonName:%s, Followers:%s, Following:%s, Statuses:%s, Listed:%s, Verified:%s, Protected:%s, Timezone:%s" % (
            user['screen_name'],
            user['id'],
            time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y')),
            user['location'],
            user['name'],
            user['followers_count'],
            user['friends_count'],
            user['statuses_count'],
            user['listed_count'],
            user['verified'],
            user['protected'],
            user['time_zone']
            ) 
        O_CLASS1 = user['screen_name']
        O_CLASS2 = user['followers_count']
        O_CLASS3 = user['friends_count']
        O_ORIGIN = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))       
        O_ORIGINREF = "Twitter%s-%s" % (user['id'], user['url'])
        O_LOGSOURCE = '%s' % user['profile_image_url']    
        
        entity = {'TYPE' : 'Object', 'LOOKUP' : '%s%s%s%s' % (O_ORIGIN, O_ORIGINREF, O_TYPE, O_CATEGORY)}
        O_GUID, found = self.DB.EntityResolve(entity)
        if found == 0:      
            self.DB.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE) 
            newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', O_GUID, 'Object')
            
            # Create an Event for the creation of the account
            E_TYPE = "SocialMedia"
            E_CATEGORY = "AccountCreation"
            E_DESC = "%s created on %s in timezone %s." % (user['screen_name'], user['created_at'], user['location'])
            E_CLASS1 = "AccountID: %s" % user['id']       
            E_TIME = time.strftime('%H:%M:%S', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            E_DATE = time.strftime('%Y-%m-%d', time.strptime(user['created_at'],'%a %b %d %H:%M:%S +0000 %Y'))
            E_DTG  = ('%s%s' % (E_TIME, E_DATE)).replace(":", "").replace("-", "")
            E_ORIGIN = user['screen_name']       
            E_ORIGINREF = "Twitter%s-%s" % (user['id'], user['url'])  
            E_LOGSOURCE = '%s' % user['time_zone']
            E_XCOORD = XCOORD
            E_YCOORD = YCOORD
            
            entity = {'TYPE' : 'Location', 'LOOKUP' : '%s%s' % (XCOORD, YCOORD)}
            L_GUID, found = self.DB.EntityResolve(entity)
            if found == 0:
                L_GUID = self.DB.insertLocation('Location', 'Associated with Social Media', XCOORD, YCOORD, 0, O_CLASS2, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)            
                newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', L_GUID, 'Location')
            entity = {'TYPE' : 'Event', 'LOOKUP' : '%s' % (E_DESC)}  
            E_GUID, found = self.DB.EntityResolve(entity)
            if found == 0:
                EGUID = self.DB.insertEvent(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
                newRel = self.DB.insertRelation(self.searchID, 'Event', 'FOUND', E_GUID, 'Event')
            newRel = self.DB.insertRelation(O_GUID, 'Object', 'CreatedAt', L_GUID, 'Location') 
            if newRel == False:
                return newRel            
            newRel = self.DB.insertRelation(E_GUID, 'Event', 'AccountCreation', L_GUID, 'Location')
            if newRel == False:
                return newRel            
            newRel = self.DB.insertRelation(E_GUID, 'Event', 'AccountCreation', O_GUID, 'Object') 
            if newRel == False:
                return newRel            
            
        return O_GUID                
    
    def sendRequest(self, username, relationship_type, next_cursor=None):
    
        url = "https://api.twitter.com/1.1/%s/ids.json?username,=%s&count=5000" % (relationship_type, username)
    
        if next_cursor is not None:
            url += "&cursor=%s" % next_cursor
    
        response = requests.get(url, auth=self.oauth, verify=False)
        self.request+=1
        time.sleep(3)
        tweets = self.responseHandler(response, username)
        
        return tweets
    
    def sendGeoRadiusRequest(self, latitude, longitude, radius, search_term, max_id=None):
    
        url = "https://api.twitter.com/1.1/search/tweets.json?q=&geocode=%f,%f,%fkm&count=200" % (latitude,longitude,radius)
    
        if max_id is not None:
            url += "&max_id=%d" % max_id
    
        # send request to Twitter
        response = requests.get(url, auth=self.oauth, verify=False)
        self.request+=1
        tweets = self.responseHandler(response, search_term)  
    
        return tweets    
    
    def getTweets(self, username, number_of_tweets, max_id=None):
        
        api_url  = "%s/statuses/user_timeline.json?" % self.base_twitter_url
        api_url += "screen_name=%s&" % username
        api_url += "count=%d" % number_of_tweets
        
        if max_id is not None:
            api_url += "&max_id=%d" % max_id
        # send request to Twitter
        response = requests.get(api_url, auth=self.oauth, verify=False) # if ssl error use verify=False
        self.request+=1
        tweets = self.responseHandler(response, username)
        return tweets
         
    def getHashtags(self, search_term, number_of_tweets, max_id=None):
        
        api_url = "%ssearch/tweets.json?" % self.base_twitter_url
        api_url += "q=%23"
        api_url += search_term
        #api_url += "&count=%d" % number_of_tweets
        
        # send request to Twitter
        response = requests.get(api_url,auth=self.oauth, verify=False) # if ssl error use verify=False
        print(response)
        self.request+=1
        tweets = self.responseHandler(response, search_term)
        return tweets  
    
    def getMentions(self, search_term, number_of_tweets, max_id=None):
        
        api_url = "%ssearch/tweets.json?" % self.base_twitter_url
        api_url += "q=%40"
        api_url += search_term
        #api_url += "&count=%d" % number_of_tweets
        
        # send request to Twitter
        response = requests.get(api_url,auth=self.oauth, verify=False) # if ssl error use verify=False
        self.request+=1
        tweets = self.responseHandler(response, search_term)
        return tweets     
    
    
    def extractLinkedText(self, link):
        p = BS4(requests.get(link).content, 'html.parser').findAll('p')
        FullText = ''
        for t in p:
            FullText = FullText + t.text + ' '  
        
        FullText = FullText.replace("'", "").replace('"', "")
        return FullText
        
    def getLast20Tweets(self, searchterm, searchtype):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OTW-getAllTweets]: process started." % (TS))  
        
        tweet_list      = []
        max_id          = 0
        #if self.Verbose == True:
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OTW-getAllTweets]: Getting 20 tweets for %s" % (TS, searchterm))              
        if searchtype == "username":
            tweet_list   = self.getTweets(searchterm,20)   
        elif searchtype == "hashtags":
            tweet_list   = self.getHashtags(searchterm,20)
        elif searchtype == "mentions":
            tweet_list   = self.getMentions(searchterm,20) 
        else:
            tweet_list   = self.getTweets(searchterm,20) 
        if tweet_list == None:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')           
            return "[%s_OTW-getAllTweets]: No tweets found" % (TS)
        
        # Get hyperlinked story and add it to the record to process
        for t in tweet_list:
            try:
                t['text'] = t['text'] + '\n' + self.extractLinkedText(t['entities']['urls'][0]['expanded_url'])
            except:
                pass
            
        # Put all the tweet descriptions with associated channels into a Real news table
        # Get fake news and put into a table
        # Get the combined table with appropriate tags
        # Train model
        # Analyze model
        # Serve model and update
        # Update model with new news from Real and new fakes
        newRels = self.processTweets(tweet_list, searchterm) 
        

    def getAllTweets(self, searchterm, searchtype):
        '''
        Procedure to manage collection of Tweets based on a term and type of search.
        Will limit batch collection to 200 tweets per GET. At first GET, determine oldest tweet
        and use it as a marker for where to start collection of next GET. After every GET,
        process the tweets.
        '''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OTW-getAllTweets]: process started." % (TS))  
        
        full_tweet_list = []
        tweet_list      = []
        max_id          = 0
        #if self.Verbose == True:
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OTW-getAllTweets]: Getting first 200 tweets for %s" % (TS, searchterm))              
        if searchtype == "username":
            tweet_list   = self.getTweets(searchterm,200)   
        elif searchtype == "hashtags":
            tweet_list   = self.getHashtags(searchterm,200)
        elif searchtype == "mentions":
            tweet_list   = self.getMentions(searchterm,200) 
        else:
            tweet_list   = self.getTweets(searchterm,200) 
        if tweet_list == None:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')           
            return "[%s_OTW-getAllTweets]: No tweets found" % (TS)
        
        # First batch sent to processing
        newRels = self.processTweets(tweet_list, searchterm)
        if newRels == False:
            return "Collection stopped based on previous entities existed."
        # grab the oldest Tweet with a try/except to handle different tweet_lists
        try:
            oldest_tweet = tweet_list[::-1][0]
        except:
            try:
                oldest_tweet = tweet_list['statuses'][0]
            except:
                return "User has no tweets."
        
        runs = 0
        # continue retrieving Tweets
        while max_id != oldest_tweet['id'] and self.request <= self.requestmax:
        #while runs < self.requestmax:
            runs+=1
        
            full_tweet_list.extend(tweet_list)
           # if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_OTW-getAllTweets]: Retrieved: %d Tweets (max_id: %d)." % (TS, len(full_tweet_list), max_id))                  
            
            # set max_id to latest max_id we retrieved
            max_id = oldest_tweet['id'] 
            
            # sleep to handle rate limiting
            time.sleep(1)
            # send next request with max_id set
            print(tweet_list)

            tweet_list = self.getTweets(searchterm,200, max_id-1)
            if tweet_list == None:
                return "[%s_OTW-getAllTweets] Tweetlist complete with %d tweets." % (TS, len(full_tweet_list))  
            newRels = self.processTweets(tweet_list, searchterm)
                
            if newRels == False:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')                
                return "[%s_OTW-getAllTweets] Collection stopped based on previous entities existed." % (TS)           
        
            # grab the oldest Tweet
            if len(tweet_list):
                oldest_tweet = tweet_list[-1]
     
        # add the last few Tweets
        full_tweet_list.extend(tweet_list)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')                
        return "[%s_OTW-getAllTweets] Tweetlist complete with %d tweets." % (TS, len(full_tweet_list))         
    
    def getEntityData(self, user_ids, sourceID, sourcename, reltype, username):
        
        i = 0
        # Set the url and build it from the names until 100 as a fail safe
        api_url = "https://api.twitter.com/1.1/users/lookup.json?user_id="
        while i < len(user_ids) and i < 100:
            api_url += ",%s" % user_ids[i]
            i+=1 
        response = requests.get(api_url, auth=self.oauth, verify=False) 
        users = self.responseHandler(response, username)  
        if users == None:
            if self.Verbose == True:
                print("[*] No users in list.")
            return
        for user in users:
            O_GUID = self.ETLUser2HANA(user, sourceID)
            if reltype == 'followers':
                self.DB.insertRelation(O_GUID, "Object", "Follows", sourceID, "Object")
            else:
                self.DB.insertRelation(O_GUID, "Object", "Following", sourceID, "Object")
              
    def getAssociates(self, username):
        
        # Set up the source user as the first look up
        api_url  = "%s/users/lookup.json?" % self.base_twitter_url
        api_url += "screen_name=%s&" % username      
        response = requests.get(api_url, auth=self.oauth, verify=False) # if ssl error use verify=False
        user = self.responseHandler(response, username)
        
        if response.status_code == 200:
            user = json.loads(response.text)
            user = user[0]
            O_GUID = self.ETLUser2HANA(user, username) 
            
            rels = ['followers', 'following']
            associate_list = []
            del associate_list[:]
            next_cursor = None
            
            for reltype in rels:
                associates = self.sendRequest(username, reltype)    
                
                # valid user account so start pulling relationships
                if associates is not None:
                    associate_list.extend(associates['ids'])
            
                    # while we have a cursor keep downloading friends/followers
                    while associates['next_cursor'] != 0 and associates['next_cursor'] != -1:
                        associates = self.sendRequest(username, "reltype", associates['next_cursor'])
            
                        if associates is not None:
                            associate_list.extend(associates['ids'])  
                        else:
                            break
                
                # Break down the associate list into groups of 100 to submit to UserInfo
                i = 0
                user_ids = []
                for user_id in associate_list:
                    
                    user_ids.append(user_id)
                    if len(user_ids) == 100:
                        if self.Verbose == True:
                            print("[*] 100 %s limit for entity. Request to Twitter." % reltype)
                        self.getEntityData(user_ids, O_GUID, user['screen_name'], reltype, username)
                        i=0
                        del user_ids[:]
                    if i == len(associate_list)-1:
                        if self.Verbose == True:
                            print("[*] %d %s for entity info. Request to Twitter." % (len(user_ids), reltype))
                        self.getEntityData(user_ids, O_GUID, user['screen_name'], reltype, username)
                    i+=1
                
    def getTweetsByLocation(self, latitude, longitude):
        
        if self.Verbose == True:
            print("[*] Getting tweets within 5 km of %f, %f" % (latitude,longitude))
        radius = 5
        geo_tweet_list  = []
        max_id          = 0
        search_term     = "Lat%sLon%s" % (latitude,longitude)
        i               = 0
        
        # get first 200 Tweets
        tweet_list      = self.sendGeoRadiusRequest(latitude, longitude, radius, search_term)
        # First batch sent to processing
        self.processTweets(tweet_list['statuses'], search_term)        
        # get oldest Tweet
        try:
            oldest_tweet = tweet_list['statuses'][-1]
        except:
            return("No tweets found at %s, %s." % (latitude, longitude))
        
        # continue retrieving Tweets
        while max_id != oldest_tweet['id'] and self.request <= self.requestmax:
        
            geo_tweet_list.extend(tweet_list['statuses'])
            # set max_id to latest max_id retrieved
            max_id = oldest_tweet['id'] 
            if self.Verbose == True:
                print("[*] Retrieved: %d Tweets (max_id: %d)" % (len(geo_tweet_list),max_id))
            # sleep to handle rate limiting
            time.sleep(3)
            
            # send next request with set max_id
            tweet_list = self.sendGeoRadiusRequest(latitude,longitude,radius, search_term, max_id-1)
            self.processTweets(tweet_list['statuses'], search_term)
        
            # get the oldest Tweet
            if len(tweet_list['statuses']):
                oldest_tweet = tweet_list['statuses'][-1]
            
            i+=1
            if i == 2:
                break
        
        geo_tweet_list.extend(tweet_list)
        if self.Verbose == True:
            print("[*] Tweetlist complete with %d tweets." % len(geo_tweet_list))        
    


# Command Line 

#savedaccounts = ['zaidbenjamin', 'WSJ', 'cnnarabic', 'Reuters', 'BBCArabic', 'guardiannews',  'nytimes', 'AlRiyadh Ã¢â‚¬ï¿½', 'AJArabic Ã¢â‚¬ï¿½', 'alhayatdaily']
#savedaccounts = ['AngelaMerkeICDU']
#ot = OsintTwitter(None, None)
#searchterm = 'BBCWorld'  
#searchtype = 'username'
#ot.getLast20Tweets(searchterm, searchtype)
#latitude    = 9.0941
#longitude   = 7.3340
#ot.getAssociates(searchterm)
#for searchterm in savedaccounts:
    #ot.getAllTweets(searchterm, searchtype)
    #ot.getAssociates(searchterm)
#ot.getTweetsByLocation(latitude, longitude)


'''
Abuja
latitude    = 9.0941
longitude   = 7.3340

Ankara
latitude    = 39.9529
longitude   = 32.8947

Athens
latitude    = 37.9921
longitude   = 23.7352

Barnsley
latitude    = 53.5470
longitude   = -1.4785

Berlin
latitude    = 52.5200
longitude   = 13.4050

Birmingham UK
latitude    = 52.4800
longitude   = -1.8635

Buenos Aires
latitude    = -34.6037
longitude   = -58.3816

Cairo
latitude    = 30.0674
longitude   = 31.5743

Cape Town
latitude    = -33.9598
longitude   = 18.6202

Caracas
latitude    = -10.4806
longitude   = -66.9036

Detroit
latitude    = 42.3985
longitude   = -83.1212

Dubai
latitude    = 22.2185
longitude   = 55.3017

Lagos
latitude    = 6.8732
longitude   = 3.6271

London
latitude    = 51.5074
longitude   = 0.1278

Milwaukee
latitude    = 43.0425
longitude   = -88.0312

Moscow
latitude    = -55.7423
longitude   = 37.6324

Nairobi
latitude    = -1.2883
longitude   = 36.8363

New York
latitude    = 40.7128
longitude   = -74.0059

Paris
latitude    = 48.8566
longitude   = 2.3522

Quito
latitude    = -0.1807
longitude   = -78.4678

Riyadh      
latitude    = 24.7136
longitude   = 46.6753

Tehran
latitude    = 35.6892
longitude   = 51.3890


'''





    
