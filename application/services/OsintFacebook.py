# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta
import random
import codecs
import csv
import sys
import os
import re
import _locale
_locale._getdefaultlocale = (lambda *args: ['en_US', 'utf8'])

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

debugging = False

class OsintFacebook():
    
    def __init__(self, auth, DB):
        
        self.setPath(auth)
        self.data = {}
        self.DB = DB
    
    def setPath(self, auth):
        
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
            
            try:
                self.fbEmail  = self.AUTH['client_key']
                self.fbPass   = self.AUTH['client_secret']      
            except:
                self.fbEmail  = ''
                self.fbPass   = ''                
        
        else:
            self.fbEmail  = auth['client_key']
            self.fbPass   = auth['client_secret']      

        if '\\' in os.getcwd():
            if debugging == False:
                self.chromePath = '%s\\application\\services\\config\\chromedriver.exe' % (os.getcwd())
                self.firefoxPath = '%s\\application\\services\\config\\geckodriver.exe' % (os.getcwd())
            else:
                self.chromePath = '%s\\config\\chromedriver.exe' % (os.getcwd()) # debugging line   
                self.firefoxPath = '%s\\config\\geckodriver.exe' % (os.getcwd()) # debugging line  
                
        else:
            if debugging == False:
                self.chromePath = '%s/application/services/config/chromedriver.exe' % (os.getcwd())  
                self.firefoxPath = '%s/application/services/config/geckodriver.exe' % (os.getcwd()) 
            else:
                self.chromePath = '%s/config/chromedriver.exe' % (os.getcwd()) # debugging line  
                self.firefoxPath = '%s/config/geckodriver.exe' % (os.getcwd()) # debugging line 
        
    # Driver Operations Module
    def autoOpen(self):
        
        # get the initial count of comments to determine page scrolling function
        commentbodies_start = len(self.driver.find_elements_by_class_name("UFICommentContent"))      
        # scroll to the bottom of the page and wait 1 second
        self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
        print("[Ops] Commentbodies %d" % commentbodies_start)
        time.sleep(3)
        
        # check elements again
        commentbodies = len(self.driver.find_elements_by_class_name("UFICommentContent"))
         
        # continue scrolling while there are still new comment blocks or max scroll is not met
        while commentbodies > commentbodies_start:
            
            commentbodies_start = commentbodies
            # scroll to the bottom again then wait
            self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")                   
            time.sleep(3)
            commentbodies = len(self.driver.find_elements_by_class_name("UFICommentContent"))
            print("[Ops] Commentbodies after: %s" % commentbodies)
            # scroll to the top of the page to reset the loading
            self.driver.execute_script("window.scrollTo(0,0);")
        
        else: # There are no more commentbodies to scroll through
              # Expand all the comment buttons to expose more content
            
            seemorecommentsbuttons = self.driver.find_elements_by_class_name("UFIPagerLink")
            for button in seemorecommentsbuttons:
                try:
                    button.click()
                    self.driver.switch_to_default_content()            
                except:
                    print("[!]Button %s unclickable" % button.text)
    
    # Driver Operations Module
    def logIn(self):
        
        self.driver = webdriver.Chrome(executable_path=self.chromePath)
        self.driver.get("https://www.facebook.com/")
        # login to Facebook
        elem = driver.find_element_by_id('email')
        elem.send_keys(self.fbEmail)
        elem = driver.find_element_by_id('pass')
        elem.send_keys(self.fbPass)
        elem.send_keys(Keys.RETURN)
        time.sleep(1)
    
    # Data Preparation Module
    def cleanText(self, TextObject):
        
        try: # TextObject is a selenium object like post
            try:
                TextObject = TextObject.text.encode('utf-8').strip()       
            except:
                try:
                    TextObject = TextObject.text.encode('cp850', errors='replace')                    
                except:
                    TextObject = TextObject.text.encode('ascii', 'replace') 
        except: # TextObject is a string
            try:
                TextObject = TextObject.encode('utf-8').strip()       
            except:
                try:
                    TextObject = TextObject.encode('cp850', errors='replace')                    
                except:
                    TextObject = "ASCII ERROR"
                    #TextObject = TextObject.encode('ascii', 'replace')        
                
        return str(TextObject)
    
    # Data Preparation Module
    def relateCommentersToTarget(ActorNameList, Source, SingleActor):
        '''
        1) Create the Object entities for each FB Profile
        2) Create the relationship between the target FB user and Page entities
        '''
        O_TYPE = "SocialMedia"
        O_CATEGORY = "FacebookUser"
        O_CLASS1 = 'https://web.facebook.com/'
        O_DESC = self.cleanText(Source)
        SOURCEGUID = self.DB.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, 'Facebook', None, 'A1') 
        timestamp = time.strftime('%Y-%b-%d_%H%M%S')
        if SingleActor == None:
            for associate in ActorNameList:
                O_DESC = cleanText(associate) 
                O_CLASS2 = O_DESC[:3]
                O_CLASS3 = None
                TARGETGUID = self.DB.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, 'Facebook', None, 'A1') 
                self.DB.insertRelation(SOURCEGUID, 'Object', 'Knows', TARGETGUID, 'Object')
        
        else:
            O_DESC = cleanText(SingleActor) 
            O_CLASS2 = O_DESC[:3]
            O_CLASS3 = None
            TARGETGUID = self.DB.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, 'Facebook', None, 'A1')
            self.DB.insertRelation(SOURCEGUID, 'Object', 'Knows', TARGETGUID, 'Object')
            
    # Data Preparation Module
    def cleanTime(rawDTG):
        
        print("[DPM]:CTM:%s" % rawDTG)
        months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        now = datetime.now()
        cleanDTG = rawDTG
    
        if "Yesterday" in rawDTG:
    
            hpos = rawDTG.index(':')
            
            if "pm" in rawDTG:
                hours = int(rawDTG[hpos-2:hpos])
                if hours <= 12:
                    hours = hours + 12                
            else:
                hours = int(rawDTG[hpos-2:hpos])
                
            minutes = int(rawDTG[hpos+1:hpos+3])
            cleanDTG = datetime(now.year, now.month, now.day-1, hours, minutes)
            cleanDTG = cleanDTG.strftime("%Y-%m-%d %H:%M:%S")  
            
        if "hrs" in rawDTG:
            try: # double digit hours
                hPos = rawDTG.index('hrs')-3
                hours = int(rawDTG[hPos:hPos+2])
            except: # single digit hours
                hPos = rawDTG.index('hrs')-2
                hours = int(rawDTG[hPos:hPos+1])            
            cleanDTG = now - timedelta(hours=hours)
            cleanDTG = cleanDTG.strftime("%Y-%m-%d %H:%M:%S")  
        
        else:
            m = 1
            for mo in months:
                if mo in rawDTG:
                    month = m
                    dPos = rawDTG.index(mo)+len(mo)+1
                    day = int(rawDTG[dPos:dPos+1])
                    hpos = rawDTG.index(':')
                    if "pm" in rawDTG:
                            hours = int(rawDTG[hpos-2:hpos])
                            if hours < 12:
                                hours = hours + 12
                    else:
                        hours = int(rawDTG[hpos-2:hpos])
                        
                    minutes = int(rawDTG[hpos+1:hpos+3])
                    cleanDTG = datetime(now.year, month, day, hours, minutes)
                    cleanDTG = cleanDTG.strftime("%Y-%m-%d %H:%M:%S")  
                m+=1   
        
        return cleanDTG
    
    # Data Preparation Module
    def resolveActorAndComment(ActorNameList, commentline, Irecord):
        
        print("[DPM]:RAC:%s" % commentline)
        n = 0
        for Actor in ActorNameList:
            if Actor in commentline:
                Irecord['ReportedBy'] = ActorNameList[n]
                line = commentline.replace(Irecord['ReportedBy'],"")
                Irecord['ENTITY_DESC'] = line + "\n" + Irecord['ENTITY_DESC']
                return Irecord
            else:
                Irecord['ENTITY_DESC'] = commentline 
            n+=1
                
        return Irecord
    
    # Data Preparation Module
    def resolveReportedBy(ActorNameList, Irecord, Rrecord, Source, commentline):
        
        if 'shared' and 'post.' and ('her' or 'his') in Irecord['ReportedBy']: # Condition where extra text is associated with the Actor name
            resolveActorAndComment(ActorNameList, commentline, Irecord)
            ActorNamePos = Irecord['ReportedBy'].index('shared')-1
            Irecord['ReportedBy'] = (Irecord['ReportedBy'])[0:ActorNamePos]
            
        if 'shared' and "'s" in Irecord['ReportedBy']: # Condition where the commenter shares another user's post or comment
            ActorNamePos = Irecord['ReportedBy'].index('shared')-1 # Move back to get rid of the space
            ActorName = (Irecord['ReportedBy'])[0:ActorNamePos]
            OtherNameSPos = Irecord['ReportedBy'].index('shared')+6 # Move up the length of the word shared
            OtherNameEPos = Irecord['ReportedBy'].index("'s")
            OtherName = (Irecord['ReportedBy'])[OtherNameSPos:OtherNameEPos]
            Rrecord['SourceType']   = "FBUser"
            Rrecord['TargetType']   = "FBUser"
            Rrecord['SourceRefID']  = ActorName
            Rrecord['TargetRefID']  = OtherName
            Rlogger.writerow(Rrecord)   
            print("[REL]: %s to %s" % (Rrecord['SourceRefID'], Rrecord['TargetRefID']))
            Irecord['ReportedBy'] = ActorName
             
        if Irecord['ReportedBy'] not in ActorNameList: # Condition where the Actor was not seen in the original pass so only create a relation to the Event
            relateCommentersToTarget(ActorNameList, Source, Irecord['ReportedBy'])
        
        return Irecord
    # Data Collection Module
    def getComments(posts, ActorNameList, Source):
        
        timestamp = time.strftime('%Y-%b-%d_%H%M')  
        Rrecord = {}
        Rrecord['SEARCH_DATE']   = timestamp
        Rrecord['Origin']        = "FBSelAPI"
        Rrecord['SourceSystem'] = "OSINT Engine"  
    
        Irecord = {}
        Irecord['SEARCH_DATE']   = timestamp
        Irecord['ORIGIN']        = "FBSelAPI"
        Irecord['SOURCE_SYSTEM'] = "OSINT Engine"    
        
        j = 1
        for rawpost in posts: # First put the text into a format we can ingest
            post = str(cleanText(rawpost))       
            splitpost = post.split('\n') # Second, split each line of the post into a list element
            L = 0
            
            # Go through the post line by line and change states to determine where in the thread the traversal is
            postcomments = 0
            firstPost = 1
            inComment = 0 # Marks where the original post ends and the comments start to define a border using the iterator
            endofPost = 0
            firstPass  = 1
            while L < len(splitpost) and endofPost == 0:
    
                LL = len(splitpost[L])
                line = splitpost[L]
                Orecord = {}
                Orecord['ENTITY_DESC'] = str("[%d]:%s |Len:%d" % (L,line,LL))
                
                if inComment == 0 and L > 1 and not (('Like Page' in line and LL == 5) 
                                                 or ('Like' in line and LL == 4) 
                                                 or ('Show more reactions' in line and LL == 19) 
                                                 or ('CommentShare' in line and LL == 12)): # For the posts with many lines
                    print("[INC]%s" % Orecord)         
                    
                    if firstPass == 1:
                        Irecord['ReportedBy']    = splitpost[0] # First element in a post is Name but sometimes there are other conditions...
                        Irecord = resolveReportedBy(ActorNameList, Irecord, Rrecord, Source, line)
                        Irecord['ENTITY_TIME']   = cleanTime(splitpost[1]) # Timestamp
                        Irecord['ENTITY_DESC']   = (splitpost[2] + '\n' + line) # Third is the first line of the message and add the next
                        Irecord['ENTITY_ID']     = "FBP%s%d%d" % (Source, j,L)
                        Irecord['ENTITY_GUID']   = Irecord['ENTITY_ID']
                        postID = Irecord['ENTITY_GUID']
                        firstPass = 0
                        print("[PFP]%s" % Orecord)
                    elif line not in Irecord['ENTITY_DESC']:
                        Irecord['ENTITY_DESC']   = Irecord['ENTITY_DESC'] + "\n" + line
                        print("[PNP]%s" % Orecord)
                    
                if LL == 20 and 'Press Enter to post.' in line: # end of post
                    endofPost = 1
                
                # If "Write a comment..." is after comment then there is nothing else to get from the post    
                if LL == 8 and 'Comment' in line:
                    print("[CMT]%s" % Orecord)             
                    LLP = len(splitpost[L+1])
                    LP  = splitpost[L+1]
                    
                    if LLP == 18 and 'Write a comment...' in LP: # This is the second to last line
                        endofPost = 1
                        print("[WAC]%s" % Orecord)
                    else:
                        print("[INP]%s" % Orecord)
                        inComment = L+1
                        postcomments+=1
                        firstpostPos = L+1 # This will be used to halt the backstep because the first post is not bounded with a Like Reply condition
                        
                # Commentshare is always followed by likes or by comments which means no likes.
                # Either case this is the last line
                if LL == 12 and 'CommentShare' in line: 
                    print("[CMS]%s" % Orecord)            
                    
                    try:
                        if 'Comments' in splitpost[L+1]:
                            Irecord['ReVerb'] = 0
                            print("[INC]%s" % Irecord)
                            inComment = 1                    
                        else:
                            Irecord['ReVerb'] = splitpost[L+1]
                            print("[INC]%s" % Irecord)
                            inComment = 1 
                    except:
                        inComment = 1
                        Irecord['ReVerb'] = 0
                        print("[INC]%s" % Irecord)
                        inComment = 1                     
                        
                    # Relate the FB User to the post who is also related to the page  (Event -> FBUser -> Post -> Comment <- FBUser)  
                    Rrecord['SourceType']   = "FBUser"
                    Rrecord['TargetType']   = "FBPost"
                    Rrecord['SourceRefID']  = Irecord['ReportedBy']
                    Rrecord['TargetRefID']  = postID
                    print("[REL]: %s to %s" % (Rrecord['SourceRefID'], Rrecord['TargetRefID']))
     
                if L > 7 and 'Like' in line and 'Reply' in line and inComment > 1:
                    print("[LRP] %s" % Orecord)              
                    backcheck = 0
                    c = 1
                    Irecord['ENTITY_TIME'] = cleanTime(line)
                        
                    while backcheck == 0 and (L-c) >= inComment:
                        linecheck = splitpost[L-c]
                        lenlineck = len(splitpost[L-c])
                        
                        print("[BCK] %d" % c)
                        if (L-c) == firstpostPos or ('Like' in linecheck and 'Reply' in linecheck): # Condition if backcheck is on the first comment
    
                            Irecord = resolveActorAndComment(ActorNameList, splitpost[L-c], Irecord)
                            Irecord = resolveReportedBy(ActorNameList, Irecord, Rrecord, Source, linecheck)
                            Irecord['ENTITY_ID']     = "FBC%s%d" % (postID, L)
                            Irecord['ENTITY_GUID']   = Irecord['ENTITY_ID']                        
                            backcheck = 1
                            print("[BCK]%s" % Irecord)
                            # Relate the post to the comment (Event -> FBUser -> Post -> Comment <- FBUser)
                            Rrecord['SourceType']   = "Post"
                            Rrecord['TargetType']   = "Comment"
                            Rrecord['SourceRefID']  = postID
                            Rrecord['TargetRefID']  = Irecord['ENTITY_GUID']
                            print("[BCK]: %s to %s" % (Rrecord['SourceRefID'], Rrecord['TargetRefID']))
                            # Relate the commenter to the comment (Event -> FBUser(poster) -> Post -> Comment <- FBUser(commenter))
                            Rrecord['SourceType']   = "FBUser"
                            Rrecord['SourceRefID']  = Irecord['ReportedBy']
                            print("[BCK]: %s to %s" % (Rrecord['SourceRefID'], Rrecord['TargetRefID']))                        
                                      
                        if not (('Like Page' in line and LL == 5) 
                                         or ('Like' in line and LL == 4) 
                                         or ('Show more reactions' in line and LL == 19) 
                                         or ('CommentShare' in line and LL == 12)): # For the posts with many lines
                            print("[APD]")
                            Irecord['ENTITY_DESC'] = linecheck + "\n" + Irecord['ENTITY_DESC'] 
                            print(Irecord)
                            c+=1      
                L+=1        
            j+=1
             
    # Data Collection Module
    def getUFICommentActorNames(driver):
        
        ActorNameList = []
        ActorNames = driver.find_elements_by_class_name("UFICommentActorName")
        # Set up the social network based on everyone who commenteded in the target's timeline
        for Actor in ActorNames:
            try:
                prep = Actor.text.encode('utf-8').strip()
             
            except:
                try:
                    prep = Actor.text.encode('cp850', errors='replace')              
    
                except:
                    prep = Actor.text.encode('ascii', 'replace')         
            ActorNameList.append(prep)
            
        # Make a set from the list to eliminate redundancies but switch back for iteration
        ActorNameList = set(ActorNameList)
        ActorNameList = list(ActorNameList)
        return ActorNameList
    
    # Data Collection Module
    def getEventStats(driver, Irecord):
        
        # Get the "Interested[0], Going[1], Invited[2]" statistics
        stats = driver.find_elements_by_class_name("_3eni")
        st = []
        i = 0
        drilldown = 0
        guestlist = driver.find_element_by_id("event_guest_list")
        print(guestlist.text)
        # #event_guest_list > div > div > div > table > tbody > tr > td._51m-.vTop._51mw > div > a
        # //*[@id="event_guest_list"]/div/div/div/table/tbody/tr/td[3]/div/a
        #action = webdriver.common.action_chains.ActionChains(driver)
        #action.move_to_element(guestlist)
        #action.click()
        #action.perform()
        LinkList = driver.find_elements_by_xpath("//*[@href]")
    
    
        # Fill the list of stats to manage labels
        for stat in stats:
            text = stat.text
            st.append(text)
            print("[%d]: %s" % (i, text))
            i+=1
            if drilldown == 0:
                try:
                    stat.click()
                    print("CLIcKED")
                    drilldown = 1
                except:
                    print("NOCLICK")
        
        # Most sites use the 3eni tag for the stats
        try:
            Irecord['ReportedByFriends']   = st[0] # Interested
            Irecord['ReportedByFollowers'] = st[1] # Going
            Irecord['ReportedBystatuses']  = st[2] # Invited
        
        # A few use 3enj
        except:
            try:
                stats = driver.find_elements_by_class_name("_3enj")
                for stat in stats:
                    text = stat.text
                    st.append(text)
                    print("[%d]: %s" % (i, text))
                    i+=1            
                    Irecord['ReportedByFriends']   = st[0] # Interested
                    Irecord['ReportedByFollowers'] = st[1] # Going
                    Irecord['ReportedBystatuses']  = st[2] # Invited
        
            except:
                print("[!] Unable to get stats for %s" % event)
        
        return Irecord
    
    # Detection Module
    def triggerWordsCheck():
    
        
        return
        
    # Direction Module
    def monitorEvents(events):
        
        timestamp = time.strftime('%Y-%b-%d_%H%M')     
        Irecord = {}
        Irecord['SEARCH_DATE'] = timestamp
        Irecord['ORIGIN']      = "FBSelAPI"
        Irecord['SOURCE_SYSTEM'] = "OSINT Engine"    
        
        # Cycle through each event and grab the key data points including:
        # Start time, End time, Name, Details, Host, Invited, Going, Interested
        i = 0
        for event in events:
            
            Irecord['SEARCH_TERM'] = event
            # Use selenium to get to the FB event site
            driver = logIn()
            driver.get(event)
            print("[DIR] Getting event: %s" % event)
    
            # Get the details of the event
            details = driver.find_element_by_id("reaction_units")
            Irecord['ENTITY_DESC'] = details.text
            title = driver.find_element_by_class_name("_5v1l")
            Irecord['ENTITY_DESC'] = (title.text + Irecord['ENTITY_DESC']) # Name appended to front of Description
            Irecord['ENTITY_DESC'] = cleanText(Irecord['ENTITY_DESC'])
            host = driver.find_element_by_xpath("//*[@id='event_featuring_line']/span[2]/a")
            Irecord['ReportedBy'] = host.text # Host      
            startendtime = driver.find_element_by_class_name("_xkh")
            Irecord['Startdate'] = startendtime.text
            title = title.text
            
            # Open the discussions page
            seeDiscussion = driver.find_element_by_class_name("_4b4x")
            time.sleep(2)
            seeDiscussion.click()
            autoOpen(driver)
            
            # Get the forecasted attendance
            Irecord = getEventStats(driver, Irecord)
            print("[EVT]%s" % Irecord)     
            
            ActorNameList = getUFICommentActorNames(driver)
            relateCommentersToTarget(ActorNameList, title, None)
            posts = driver.find_elements_by_class_name("_3ccb")
            getComments(posts, ActorNameList, title)
                  
            # close our browser
            driver.quit()
            i+=1
    
    # Direction Module
    def getFriends(username):
        
        # browse to the target page, allow for a pause, and open it up
        self.logIn()
        self.driver.get("https://www.facebook.com/%s" % username)
        time.sleep(5)
        autoOpen()
        
        # Get all the commentblocks to break down into individual components        
        posts = driver.find_elements_by_class_name("_3ccb")
        
        # Get all the Commenters of posts on the page
        ActorNameList = getUFICommentActorNames(driver)
        
        relateCommentersToTarget(ActorNameList, username, None)
        getComments(posts, ActorNameList, username)
        # close the browser
        driver.quit()
    
#----------------- Debugging Command Line -------------------#

#CommandLine
#target_profile = "nicole.otubo"
#target_profile = "murtala.zakariyya"
#events = ['https://www.facebook.com/events/1295610047127762/']

#getFriends(target_profile)
#monitorEvents(events)
'''
Ofile.close()
Ifile.close()
Rfile.close()'''
