#!/usr/bin/env python3
# -*- coding: utf-8 -*

import os, json, time, math
from datetime import datetime
from flask import flash, request
#from py2neo import Graph, Node, Relationship, Path, authenticate
#from py2neo.ext.calendar import GregorianCalendar
from passlib.hash import bcrypt
import uuid
from threading import Thread

debugging = False
if debugging == False:
    from application.services import OsintTwitter as ot
    #from application.services import OsintGraph as og
    from application.services import OsintPubDB as op
    from application.services import OsintYouTube as oy
    from application.services import OsintHANA as oh
    from application.services import OrientModels as om
    auth = '%s/application/services/config/AUTH_Neo4j.json' % (os.getcwd())
else:
    from services import OsintTwitter as ot #debugging line
    from services import OsintGraph as og #debugging line
    auth = '%s/services/config/AUTH_Neo4j.json' % (os.getcwd()) # debugging line
    
#keys = json.loads(open(auth).read())        
#authenticate(keys['host'], keys['name'], keys['password'])
#graph = Graph("http://localhost:7474/db/data/")
#calendar = GregorianCalendar(graph)
UI5 = False
#DB = og.OsintGraph()
#DB = oh.OsintHANA()
DB = om.OrientModel()
DB.openDB('POLER')
class User:

    def __init__(self, username):
        self.username = username
        self.is_authenticated = False
        self.is_active = False
        self.is_anonymous = True
        self.GUID = None
        #self.oGraph = og.OsintGraph()
        #self.oGraph.startGraph()
        self.authpath = '%s/application/services/config/%s' % (os.getcwd(), username)
        self.datapath = '%s/application/services/data/' % (os.getcwd())
        self.ODB = om.OrientModel()
        self.ODB.openDB('POLER')
        self.HDB = oh.OsintHANA()
        self.HDB.goLive()
        self.PubDB = op.OsintPubDB(ODB)
        self.user = self.find()
        self.threads = []
        self.HANA = False
        self.PIRcache = []
        self.STRcache = []
        self.Pcache = []
        self.Ocache = []
        self.Lcache = []
        self.Ecache = []
        self.Rcache = []
    
    def get_id(self):
        return str(self.id)
    def is_active(self):
        return True
    def is_anonymous(self):
        return False
    def is_authenticated(self):
        return True    
    
    def menus(self):
        '''
        Fills the lists used in menus by calling the DB method and fills its cache with 
        a dictionary of lists (JSON format ready) 
        '''
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-menus]: Front loading menu data with uaa %s" % (TS, self.location))
        menu = DB.menuFill(self.location)
        
        self.PIRcache = menu['PIR']
        self.STRcache = menu['STR']
        self.Lcache = menu['LOCATIONS']
        self.Pcache = menu['PERSONS']
        self.Ocache = menu['OBJECTS']
        self.Ecache = menu['EVENTS']
        self.Rcache = menu['RELS']  
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-menus]: %d relations loaded.:" % (TS, len(self.Rcache)))
        print(('[%s_APP-Model-menus]: User menus loaded with total of %d entities' % (TS, (len(self.Lcache) + len(self.PIRcache) + len(self.STRcache) + len(self.Pcache) + len(self.Ocache) + len(self.Ecache) + len(self.Rcache)))))
        return menu
    
    def update_user(self, iObj):
        message = DB.update_user(iObj)
        return message         
          
    def find(self):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-find]: Getting user:" % (TS))
        
        user = DB.findUser(self.username)
        if user == None:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_APP-Model-find]: No existing user:" % (TS))            
            
            return False
        print("[%s_APP-Model-find]: User found %s:" % (TS, user['utype']))
        self.GUID = user['GUID']
        self.email = user['email']
        self.tel = user['tel']
        self.location = user['location']
        self.utype = user['utype']
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-find]: User found %s:" % (TS, user))            
        return user
    
    def GUID(self):
        return self.GUID

    def get_task(self, GUID):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-get_task]: Getting task: %s" % (TS, GUID))        

        return DB.get_task(GUID)
    
    def get_entity_profile(self, GUID, TYPE):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-get_entity]: Getting entity profile: GUID:%s TYPE:%s" % (TS, GUID, TYPE))
        ProfilePageSize = 100
        Profile = {'ProfileType' : TYPE}
        
        Profile['Profile'] = DB.get_entity(GUID, TYPE)
        if Profile['Profile']:
            if 'Relations' not in Profile.keys():
                Profile['Relations'], Profile['pRelCount'], Profile['oRelCount'], Profile['lRelCount'], Profile['eRelCount'] = DB.get_entity_relations(GUID)
            Profile['TotalRel'] = len(Profile['Relations'])
            Profile['Person'] = []
            Profile['Object'] = []
            Profile['Location'] = []
            Profile['Event'] = []
            
            i = 0
            for e in Profile['Relations']:
                entity = DB.get_entity(int(e['GUID']), e['TYPE'].strip())
                if entity:
                    entity['RELTYP'] = e['REL']            
                    Profile['%s' % e['TYPE']].append(entity)
                    i+=1
                if i > ProfilePageSize:
                    break
            Profile['Profile']['DESC'] = "%s\nTotal of %d relationships.\nPeople:%d\nObjects:%d\nLocations:%d\nEvents:%s" % (Profile['Profile']['DESC'], Profile['TotalRel'], Profile['pRelCount'], Profile['oRelCount'], Profile['lRelCount'], Profile['eRelCount'])
            if Profile['TotalRel'] > ProfilePageSize:
                Profile['Profile']['DESC'] = Profile['Profile']['DESC']  + '\nShowing first %d.' % ProfilePageSize
        

        return Profile   
    
    def get_VP_entity_profile(self, GUID, TYPE, spath, epath):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-get_VP_entity_profile]: Getting entity profile: GUID:%s TYPE:%s paths (%d %d)" % (TS, GUID, TYPE, int(spath), int(epath)))
        Profile = self.get_entity_profile(GUID, TYPE)
        Profile = DB.Graph_VP_Risks(int(spath), int(epath), GUID, Profile)
        
        return Profile       
    
    def register(self, password, email, tel, location, image, utype):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-register]: Process started:" % (TS))            
        
        if self.find() == False:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_APP-Model-register]: Not found so creating a new user:" % (TS))             
            self.GUID = DB.insertUser(self.username, bcrypt.encrypt(password), email, tel, location, image, utype)
            if self.username == 'Tester1':
                startupthread = Thread(target=self.HDB.firstrun,)
                startupthread.start()
            return True
        print("[%s_APP-Model-register]: Username taken:" % (TS)) 
        return False

    def verify_password(self, password):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-verify_password]: Process started:" % (TS))         
        user = self.find()
        if not user:
            print("[%s_APP-Model-verify_password]: No user:" % (TS))
            return False

        print("[%s_APP-Model-verify_password]: entered:%s" % (TS, password))
        return bcrypt.verify(password, user["password"])
    
        
    def user_event(self, CATEGORY, CLASS1, DTG, DATE, TIME, intel_DESC, GUID):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-user_event]: Process started:" % (TS))            
        GUID = self.GUID
        LANG = 'en'
        TYPE = 'UserAction'
        CATEGORY = CATEGORY
        ORIGIN = GUID
        ORIGINREF = 'UserAction' + str(GUID) + str(DTG)
        LOGSOURCE = 'COIN'  
        XCOORD = 0.0
        YCOORD = 0.0
        DESC = '%s %s on %s at %s. %s' % (self.username, CATEGORY, DATE, TIME, intel_DESC)

        intel_GUID = DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
        print("[%s_APP-Model-user_event]: Try User intelGUID = %s" % (TS, intel_GUID))
        previousIntel = DB.insertRelation(self.GUID, 'User', CATEGORY, intel_GUID, 'Event')    
        
        return intel_GUID
    
    def pir_justification(self, PIRGUID, UserActionGUID, ActionType):
        
        if ActionType == 'Analysis':
            DB.insertRelation(UserActionGUID, 'Event', 'AnalysisToSupport', PIRGUID, 'Object')
        else:
            DB.insertRelation(UserActionGUID, 'Event', 'CollectionToSupport', PIRGUID, 'Object')
    
    def run_vulchild_recalc(self, spath, epath):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        intel_DESC = '%s running recalculation on vulnerable children set at %s.' % (TS, self.username)     
        today = datetime.now().strftime("%F %H:%M:%S")
        CATEGORY = 'DETECT_PATTERNS_VP'
        CLASS1  = 'Children'
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip()) 
        intel_id = self.user_event(CATEGORY, CLASS1, DTG, DATE, TIME, intel_DESC, self.GUID)           
        
        return DB.Graph_VP_CHILDREN(int(spath), int(epath))

    def run_ta(self, text, TA_CONFIG):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-run_ta]: Process started: %s\n\t%s" % (TS, text, TA_CONFIG))              
        today = datetime.now().strftime("%F %H:%M:%S")
        CATEGORY = 'TEXT_ANALYTICS'
        CLASS1 = len(text)
        intel_DESC = 'TA on text: %s using configuration %s by %s' % (text, TA_CONFIG, self.username)     
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip()) 
        
        # Create the event of this activity
        intel_id = self.user_event(CATEGORY, CLASS1, DTG, DATE, TIME, intel_DESC, self.GUID)        
        
        TA_RUN = {'GUID' : intel_id, 
                  'DESC' : intel_DESC, 
                  'DATE' : DATE, 
                  'CATEGORY' : CATEGORY,
                  'CLASS1' : CLASS1,
                  'Person' : [],
                  'Object' : [],
                  'Location' : [],
                  'Event'  : [],
                  'COUNTS' : {'Objects' : 0, 'Persons' : 0, 'GUID' : []}
                  }
        
        if CLASS1 < 5000:
            TA_VIEW, TA_RUN = HDB.TextAnalytics(TA_CONFIG, text, TA_RUN)
            TA_VIEW, TA_RUN = self.ta_run_counts(TA_VIEW, TA_RUN, intel_id)
        else:
            i = 0
            rounds = math.ceil(CLASS1/5000)
            while i < rounds:
                partialText = text[:5000]
                TA_VIEW, TA_RUN = HDB.TextAnalytics(TA_CONFIG, partialText, TA_RUN)
                TA_VIEW, TA_RUN = self.ta_run_counts(TA_VIEW, TA_RUN, intel_id)
                i+=1
                text = text[5000:CLASS1]
        
        return TA_RUN
    
    def ta_run_counts(self, TA_VIEW, TA_RUN, intel_id):
        
        for Node in TA_VIEW:
            if Node['TA_TYPE'] == 'Topic':
                DB.insertRelation(intel_id, 'Event', 'TA_REFERENCE', Node['GUID'], 'Object')
                TA_RUN['COUNTS']['Objects']+=1
                TA_RUN['COUNTS']['GUID'].append(Node['GUID'])
            elif Node['TA_TYPE'] == 'PERSON':
                DB.insertRelation(intel_id, 'Event', 'TA_REFERENCE', Node['GUID'], 'Person')
                TA_RUN['COUNTS']['Persons']+=1
                TA_RUN['COUNTS']['GUID'].append(Node['GUID'])        
        
        return TA_VIEW, TA_RUN
    
    def add_watchlist(self, listname, terms, names, locations, events):
        
        user = self.find()
        today = datetime.now().strftime("%F %H:%M:%S")
        LANG = 'en'
        TYPE = "UserAction"
        CATEGORY = "Watchlist"
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip()) 
        ORIGIN = self.username
        ORIGINREF = self.GUID
        LOGSOURCE = 'COIN'  
        XCOORD = 0.0
        YCOORD = 0.0
        wlGUID = DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE) 
        
        terms = [x.strip() for x in terms.lower().split(",")]
        terms = set(terms)  
        names = [x.strip() for x in names.lower().split(",")]
        names = set(names)    
        locations = [x.strip() for x in locations.lower().split(",")]
        locations = set(locations) 
        events = [x.strip() for x in events.lower().split(",")]
        events = set(events) 
        
        for term in terms:
            DESC = term
            termGUID = DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            DB.insertRelation(wlGUID, 'Event', 'INCLUDES_TERM', termGUID, 'Object')              
    
    def add_task(self, tasktype, subject, tags, actionUserGUID, PIRREF):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        user = self.find()
        today = datetime.now().strftime("%F %H:%M:%S")
        LANG = 'en'
        TYPE = "UserAction"
        CATEGORY = "Task"
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip()) 
        ORIGIN = 'Open'
        ORIGINREF = "%s%s%s" % (self.GUID, subject, TS)
        LOGSOURCE = 'A1'  
        CLASS1 = tasktype
        XCOORD = self.GUID
        YCOORD = actionUserGUID
        DESC = "%s task from %s with subject %s on %s in support of PIR %s." % (tasktype, self.username, subject, DATE, PIRREF)
        taskGUID = DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, int(XCOORD), int(YCOORD), ORIGIN, ORIGINREF, LOGSOURCE)
        DB.insertRelation(self.GUID, 'Object', 'PUBLISHED_TASK', taskGUID, 'Event')
        DB.insertRelation(taskGUID, 'Event', 'Supporting', PIRREF, 'Object')
        if actionUserGUID != None:
            DB.insertRelation(taskGUID, 'Event', 'TASKED_TO', actionUserGUID, 'Object')
        
        TASK = {'GUID' : str(taskGUID), 'CLASS1' : tasktype, 'DESC' : subject}    
        # TODO make an object for the task so it can be updated during the process

        tags = [x.strip() for x in tags.lower().split(",")]
        tags = set(tags)
        TYPE = "Tag"
        CATEGORY = "Term"
        CLASS2 = 0
        CLASS3 = 0   
        ORIGIN = 'COIN'
        ORIGINREF = 'COIN'       

        for tag in tags:
            DESC = tag
            CLASS1 = len(tag)
            tagGUID = DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            DB.insertRelation(taskGUID, 'Event', 'INCLUDES_TAG', tagGUID, 'Object')    

        return TASK
    
    def delete_user(self, username):
        MSG = {'messages' : []}
        MSG['messages'].append(DB.delete_user(username))
        return MSG
    
    def get_user_profile(self, GUID):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-get_user_profile]: Getting user:" % (TS))    
        
        return DB.get_user_profile(GUID)   
    
    def user_tokens(self, iObj, username):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        
        if iObj['TokenType'][0] == 'twitter':
            auth = '%s_AUTH_Twitter.json' % (self.authpath)
            print("[%s_APP-Model-user_tokens]: Creating tokens for %s at %s with %s:" % (TS, username, auth, iObj))    
            with open(auth, 'w') as outfile:
                json.dump(iObj, outfile)
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_APP-Model-user_tokens]: File %s containing %s added.:" % (TS, auth, iObj))                     
        
        return "%s tokens created for %s." % (iObj['TokenType'], username)
    
    def load_stored_procedure(self, StoredProcedureType, GUID):
        
        today = datetime.now().strftime("%F %H:%M:%S")
        CATEGORY = StoredProcedureType
        CLASS1 = 0
        intel_DESC = '%s stored procedure executed on %s by %s' % (StoredProcedureType, today, self.username)
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())             
        
        Locations = 'Locations'
        People    = 'People'
        Reset     = 'Reset'
        SPF       = 'SPF'
        MSG = {'messages' : []}
        intel_GUID = self.user_event(CATEGORY, CLASS1, DTG, DATE, TIME, intel_DESC, GUID)
        
        if StoredProcedureType == Locations:
            MSG['messages'].append(DB.preLoadLocations())
        elif StoredProcedureType == People:
            MSG['messages'].append(DB.preLoadPeople())
        elif StoredProcedureType == Reset:
            MSG['messages'].append(DB.initialize_reset())  
        elif StoredProcedureType == SPF:
            MSG['messages'].append(DB.SPF_Run_Full())           
          
        return MSG
    
    def load_user_tokens(self, TokenType, username):
        
        if TokenType == 'twitter':
            auth = '%s_AUTH_Twitter.json' % (self.authpath)
        with open(auth) as json_file:
            return json.load(json_file)  
        
    def get_News(self):
        newsChannels = ['BBCWorld']
        self.Twitter.getAllTweets(searchterm, searchtype)
        
    def from_SPF(self, iObj):
        
        MSG = {}
        MSG['messages'] = []
      
        if iObj['SOURCE_SPF_CRIMES2'][0] != 'NA':
            if iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_C_OFFENCE_CODE':
                MSG['messages'].append(DB.SPF_CRIMES2_C_OFFENCE_CODE())
            elif iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_TB_CONNECTED_REPORT':
                MSG['messages'].append(DB.SPF_CRIMES2_TB_CONNECTED_REPORT())
            elif iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_TB_PERSON_BIO':
                MSG['messages'].append(DB.SPF_CRIMES2_TB_PERSON_BIO())
            elif iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_TB_OFFENCE':
                MSG['messages'].append(DB.SPF_CRIMES2_TB_OFFENCE())
            elif iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_TB_CASE_PERSON_RESULT':
                MSG['messages'].append(DB.SPF_CRIMES2_TB_CASE_PERSON_RESULT())       
            elif iObj['SOURCE_SPF_CRIMES2'][0]  == 'SPF_CRIMES2_TEST_TB_CASE':
                MSG['messages'].append(DB.SPF_CRIMES2_TEST_TB_CASE())                    
        
        if iObj['SOURCE_SPF_OTTER'][0]  != 'NA':
            if iObj['SOURCE_SPF_OTTER'][0] == 'SPF_OTTER_VEHICLE_INFO':
                MSG['messages'].append(DB.SPF_OTTER_VEHICLE_INFO())
            if iObj['SOURCE_SPF_OTTER'][0] == 'SPF_OTTER_PHONE':
                MSG['messages'].append(DB.SPF_OTTER_PHONE())                
                   
        if iObj['SOURCE_SPF_PDS'][0]  != 'NA':
            if iObj['SOURCE_SPF_PDS'][0]  == 'SPF_PDS_TB_NRIC_INFO':
                MSG['messages'].append(DB.SPF_PDS_TB_NRIC_INFO())  
            if iObj['SOURCE_SPF_PDS'][0]  == 'SPF_PDS_TB_PERSON_VIEW':
                MSG['messages'].append(DB.SPF_PDS_TB_PERSON_VIEW())                   
        
        if iObj['SOURCE_SPF_FOCUS'][0]  != 'NA':
            if iObj['SOURCE_SPF_FOCUS'][0]  == 'SPF_FOCUS_TB_IR_VEHICLE':
                MSG['messages'].append(DB.SPF_FOCUS_TB_IR_VEHICLE()) 
            elif iObj['SOURCE_SPF_FOCUS'][0]  == 'SPF_FOCUS_TB_IR_INCIDENT':
                MSG['messages'].append(DB.SPF_FOCUS_TB_IR_INCIDENT()) 
            elif iObj['SOURCE_SPF_FOCUS'][0]  == 'SPF_FOCUS_TB_IR_PERSON_INVOLVED':
                MSG['messages'].append(DB.SPF_FOCUS_TB_IR_PERSON_INVOLVED())             
                
                
                   
        return MSG
    
    def from_HSS(self, iObj):
        
        MSG = {}
        MSG['messages'] = []
      
        if iObj['SOCIAL_SERVICES'][0] != 'NA':
            if iObj['SOCIAL_SERVICES'][0]  == 'INTAKE_REGISTRY':
                self.PubDB.ETLSocial2Graph(DB.getSocialData())
        if iObj['HEALTH_SERVICES'][0]  != 'NA':
            if iObj['HEALTH_SERVICES'][0] == 'INTAKE_REGISTRY':
                MSG['messages'].append(DB.HEALTH_SERVICES_INTAKE_REGISTRY(self.PubDB))           
        print(MSG)       
        return MSG    
    
    def add_intel(self, iObj):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-add_intel]: Process started:" % (TS))          
        
        today = datetime.now().strftime("%F %H:%M:%S")
        CATEGORY = 'ADD_INTEL_%s' % iObj['iType']
        CLASS1 = len(iObj['Description'])
        intel_DESC = '%s intel: %s by %s' % (iObj['iType'], iObj['Description'], self.username)
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())         
        
        messages = []
        today = datetime.now().strftime("%F %H:%M:%S")
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())
        CLASS1 = 0
        CLASS2 = 0
        CLASS3 = 0
        ZCOORD = 0
        XCOORD = 0
        YCOORD = 0
        LANG = 'en'
        
        # Create event for adding the intel and relate it to the user
        if iObj['iType'].lower() == 'person':
            GUID = DB.insertPerson(iObj['GEN'], iObj['FName'], iObj['LName'], iObj['DOB'], iObj['POB'], iObj['ORIGIN'], iObj['ORIGINREF'], iObj['LOGSOURCE'], iObj['Description'])
            NAME = "%s %s" % (iObj['FName'], iObj['LName'])
            intel_DESC = 'Person named %s %s' % (iObj['FName'], iObj['LName'])
            intel_GUID = self.user_event('PUBLISHED_INTEL', iObj['iType'], DTG, DATE, TIME, intel_DESC, self.GUID) 
            DB.insertRelation(GUID, 'Person', 'BORN_IN', iObj['POB'], 'Location')
            BIRTH = DB.insertEvent('Birth', 'Human', '%s %s born on %s.' % (iObj['FName'], iObj['LName'], iObj['DOB']), 'en', '1', TIME, iObj['DOB'], DTG, XCOORD, YCOORD, iObj['ORIGIN'], iObj['ORIGINREF'], iObj['LOGSOURCE'])
            DB.insertRelation(GUID, 'Person', 'BORN_ON', BIRTH, 'Event')
            DB.insertRelation(BIRTH, 'Event', 'OccurredAt', iObj['POB'], 'Location')
            DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
            previousIntel = DB.insertRelation(intel_GUID, 'Event', 'INVOLVES', GUID, iObj['iType']) 
            iObj['iType'] = 'PERSONS'
             
        elif iObj['iType'].lower() == 'object':
            NAME = "%s %s" % (iObj['oCATEGORY'], iObj['oDESC'])
            GUID = DB.insertObject(iObj['oTYPE'], iObj['oCATEGORY'], iObj['oDESC'], iObj['oCLASS1'] , iObj['oCLASS2'] , iObj['oCLASS3'], iObj['ORIGIN'], iObj['ORIGINREF'], iObj['LOGSOURCE'])
            intel_DESC = 'Object type %s and category %s with description %s.' % (iObj['oTYPE'], iObj['oCATEGORY'], iObj['oDESC'])
            intel_GUID = self.user_event('PUBLISHED_INTEL', iObj['iType'], DTG, DATE, TIME, intel_DESC, self.GUID)  
            DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
            if iObj['Locations'] != 'NA':
                DB.insertRelation(GUID, 'Object', 'REPORTED_AT', iObj['Locations'], 'Location')
            previousIntel = DB.insertRelation(intel_GUID, 'Event', 'INVOLVES', GUID, iObj['iType'])            
            iObj['iType'] = 'OBJECTS'
        
        elif iObj['iType'].lower() == 'location':
            GUID = DB.insertLocation(iObj['lTYPE'], iObj['lDESC'], iObj['lXCOORD'], iObj['lYCOORD'], iObj['lZCOORD'], iObj['lCLASS1'], iObj['ORIGIN'], iObj['ORIGINREF'], iObj['LOGSOURCE'])
            NAME = "%s" % (iObj['lDESC'])
            intel_DESC = 'Location type %s with description %s at %s, %s' % (iObj['lTYPE'], iObj['lDESC'], iObj['lXCOORD'], iObj['lYCOORD'])
            intel_GUID = self.user_event('PUBLISHED_INTEL', iObj['iType'], DTG, DATE, TIME, intel_DESC, self.GUID) 
            DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
            if iObj['Locations'] != 'NA':
                DB.insertRelation(GUID, 'Location', 'REPORTED_WITH', iObj['Locations'], 'Location')  
            previousIntel = DB.insertRelation(intel_GUID, 'Event', 'INVOLVES', GUID, iObj['iType'])
            iObj['iType'] = 'LOCATIONS'
            
        elif iObj['iType'].lower() == 'event':
            print("[%s_APP-Model-add_intel]: Adding Event:" % (TS)) 
            GUID = self.HDB.insertEvent(iObj['eTYPE'], iObj['eCATEGORY'], iObj['eDESC'], LANG, iObj['eCLASS1'], iObj['eTIME'], iObj['eDATE'], DTG, XCOORD, YCOORD, iObj['ORIGIN'], iObj['ORIGINREF'], iObj['LOGSOURCE'])            
            NAME = "%s" % (iObj['eDESC'])
            intel_DESC = 'Event %s %s with description %s at %s %s' % (iObj['eTYPE'], iObj['eCATEGORY'], iObj['eDESC'], DATE, TIME)
            intel_GUID = self.user_event('PUBLISHED_INTEL', iObj['iType'], DTG, DATE, TIME, intel_DESC, self.GUID) 
            DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
            if iObj['Locations'] != 'NA' :
                self.HDB.insertRelation(GUID, 'Location', 'REPORTED_AT', iObj['Locations'], 'Location')     
            previousIntel = self.HDB.insertRelation(intel_GUID, 'Event', 'INVOLVES', GUID, iObj['iType'])
            iObj['iType'] = 'EVENTS'
            
        elif iObj['iType'].lower() == 'relation':
            
            if iObj['pAGUID'] != '0':
                stype = 'Person'
                AGUID = iObj['pAGUID']                    
            elif iObj['oAGUID'] != '0':
                stype = 'Object'
                AGUID = iObj['oAGUID'] 
            elif iObj['AGUIDfree'] != '':
                AGUID = iObj['AGUIDfree'] 
                if str(AGUID[0]) == '1':
                    stype = 'Person'
                elif str(AGUID[0]) == '2':
                    stype = 'Object'
                elif str(AGUID[0]) == '3':
                    stype = 'Location'                    
                elif str(AGUID[0]) == '4':
                    stype = 'Event'             
            else:
                stype = 'Event'
                AGUID = iObj['eAGUID'] 
            

            if iObj['pBGUID'] != '0':
                ttype = 'Person'
                BGUID = iObj['pBGUID']                    
            elif iObj['oBGUID'] != '0':
                ttype = 'Object'
                BGUID = iObj['oBGUID'] 
            elif iObj['BGUIDfree'] != '':
                BGUID = iObj['BGUIDfree'] 
                if str(BGUID[0]) == '1':
                    ttype = 'Person'
                elif str(BGUID[0]) == '2':
                    ttype = 'Object'
                elif str(BGUID[0]) == '3':
                    ttype = 'Location'                    
                elif str(BGUID[0]) == '4':
                    ttype = 'Event'             
            else:
                ttype = 'Event'
                BGUID = iObj['eBGUID'] 
                    
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_APP-add_intel]: Adding %s relation from %s to %s-%s:" % (TS, iObj['RELTYP'], AGUID, ttype, BGUID))                  
                
            GUID = DB.insertRelation(AGUID, stype, iObj['RELTYP'], BGUID, ttype)

            if GUID != True:
                messages.append('Relationship %s exists between %s and %s.' % (iObj['RELTYP'], AGUID, BGUID)) 
                
            else:
                intel_DESC = 'Relationship of type %s created between %s and %s.' % (iObj['RELTYP'], AGUID, BGUID)
                DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
                GUID = self.user_event('PUBLISHED_INTEL', iObj['iType'], DTG, DATE, TIME, intel_DESC, self.GUID)                      
                messages.append('Relationship %s created between %s and %s.' % (iObj['RELTYP'], AGUID, BGUID))
                GUID = DB.insertRelation(intel_GUID, 'Event', 'INVOLVES', AGUID, stype)
                GUID = DB.insertRelation(intel_GUID, 'Event', 'INVOLVES', BGUID, ttype)                       
        
        self.pir_justification(iObj['ORIGIN'], intel_GUID, 'Analysis')
        
        newObj = {'GUID' : GUID,
                  'TYPE' : iObj['iType'],
                  'NAME' : NAME,
                  'DESC' : intel_DESC}
        print("[*] Message returning : %s" % newObj)
        # Trace the intel creation to the user and object    
        return newObj
         
    def recent_all(self):
        query = '''MATCH (a:User)-[r]-(b)-[]-(c) WHERE b.TYPE = 'UserAction' RETURN a.username AS username, type(r) AS uatype, b.DESC AS description, b.TIME AS time, b.DATE AS date, b.GUID AS GUID, COUNT(c) AS resultcount, b.DTG AS DTG ORDER BY DTG DESC'''
        #qRun = self.oGraph.run(query)
        results = []
        '''
        for e in qRun:
            if UI5 == True:
                data = {}
                data['username']      = e['username']
                data['uatype']        = e['uatype']
                data['description']   = e['description']
                data['date']          = e['date']
                data['GUID']          = e['GUID']
                data['resultcount']   = e['resultcount']
                data['DTG']           = e['DTG']
                results.append(data)      
            else:
                results.append(e)  
                
        if UI5 == True:
            results = json.dumps(results)   
        '''
        return results
       
    def recent_posts(self, n):
        query = """
        MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
        WHERE user.username = '%s'
        RETURN post, COLLECT(tag.name) AS tags
        ORDER BY post.timestamp DESC LIMIT %d
        """ % (self.username, n)
       
        results = []
        '''
        qRun = self.oGraph.run(query)
        
        for e in qRun:
            results.append(e)  
            
        '''
        return results
    
    def recent_intel(self, n):
        query = "MATCH (user:User)-[:PUBLISHED_INTEL]->(a) WHERE user.username = '%s' RETURN user.username AS username, a.GUID AS idintel, a.ORIGIN AS date, user.GUID AS iduser, a.DESC AS description ORDER BY date DESC LIMIT %d" % (self.username, n)
        results = []
        '''
        qRun = self.oGraph.run(query)
        
        for e in qRun:
            results.append(e) 
        '''
        return results  
    
    def recent_searches(self, n):
        query = "MATCH (user:User)-[:SEARCHED]-(a) WHERE user.username = '%s' RETURN user.username AS username, a.DESC AS description, a.DATE AS date ORDER BY date DESC LIMIT %d" % (self.username, n)    

        return []
    
    def recent_tasks(self):
        query = "MATCH (user:User)-[:TASKED_TO]-(a) WHERE user.username = '%s' RETURN user.username AS username, a.DESC AS description, a.DATE AS date, a.TIME AS time, a.CLASS1 AS type ORDER BY date DESC" % (self.username)      
        return []
    
    def similar_users(self, n):
        query = """
        MATCH (user1:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag:Tag),
              (user2:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag)
        WHERE user1.username = '%s' AND user1 <> user2
        WITH user2, COLLECT(DISTINCT tag.name) AS tags, COUNT(DISTINCT tag.name) AS tag_count
        ORDER BY tag_count DESC LIMIT %d
        RETURN user2.username AS similar_user, tags
        """ % (self.username, n)
        return []

    def commonality_of_user(self, user):
        query1 = """
        MATCH (user1:User)-[:PUBLISHED]->(post:Post)<-[:LIKES]-(user2:User)
        WHERE user1.username = '%s' AND user2.username = '%s'
        RETURN COUNT(post) AS likes
        """ % (self.username, user.username)

        likes = []
        likes = 0 if not likes else likes

        query2 = """
        MATCH (user1:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag:Tag),
              (user2:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag)
        WHERE user1.username = '%s' AND user2.username = '%s'
        RETURN COLLECT(DISTINCT tag.name) AS tags
        """ % (self.username, user.username)

        tags = []

        return {"likes": likes, "tags": tags}
    
    def run_youtube(self, PIRREF, searchtype, searchterm, latitude, longitude, radius):
        
        messages = []
        channel = 'youtube'
        searchtermplus = "%s-%s-%s-%s" % (searchterm, latitude, longitude, radius)
        NoPreviousSearch, eGUID = self.log_user_search(channel, searchtype, searchtermplus)
        self.pir_justification(PIRREF, eGUID, 'Collection')
        if NoPreviousSearch != False:
            self.YouTube.setSearchID(eGUID)
            for st in searchtype:
                if st == 'term':
                    t1 = Thread(target=self.YouTube.getVideosByKeyWords, args=(searchterm,))
                    t1.start()  
                    messages.append('YouTube collection on %s started.' % searchterm)
                if st == 'location':
                    latitude = float(latitude)
                    longitude = float(longitude)
                    radius = int(radius)
                    t = Thread(target=self.YouTube.getVideosByLocation, args=(latitude, longitude, radius, ))
                    t.start()
                    messages.append('YouTube location collection on %s, %s started.' % (latitude, longitude))
        elif isinstance(NoPreviousSearch, str) == True :
            messages.append('Previous Search %s' % NoPreviousSearch )
        else:
            messages.append('Search too recently executed.')
        return messages        
    
    def run_twitter(self, PIRREF, searchtype, searchterm, latitude, longitude, username, origin):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-run_twitter]: Received: %s %s" % (TS, searchtype, searchterm))           
        
        MSG = {}
        CATEGORY   = 'TwitterSearch'
        CLASS1     = searchtype
        intel_DESC = '%s: %s runs Twitter search type %s with %s %s %s %s.' % (TS, username, searchtype, searchterm, latitude, longitude, origin)
        DATE = str(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
        TIME = str(datetime.fromtimestamp(time.time()).strftime('%H:%M:%S'))
        DTG  = int(("%s%s" % (DATE, TIME)).replace("-", "").replace(":", "").strip())
        MSG['messages'] = []
        MSG['GUID'] = self.user_event(CATEGORY, CLASS1, DTG, DATE, TIME, intel_DESC, self.GUID)    

        auth = self.load_user_tokens('twitter', username)        
        self.Twitter = ot.OsintTwitter(DB, auth)

        MSG['messages'].append("[%s_APP-Model-run_twitter]: Received: %s %s. But no token found." % (TS, searchtype, searchterm))

        channel = 'twitter'
        searchtermplus = "%s-%s-%s" % (searchterm, latitude, longitude)
        NoPreviousSearch, MSG['GUID'] = self.log_user_search(channel, searchtype, searchtermplus)
        self.pir_justification(PIRREF, MSG['GUID'], 'Collection')
        NoPreviousSearch = True
        if NoPreviousSearch != False:
            
            self.Twitter.setSearchID(MSG['GUID'])
            st = searchterm
            if searchtype == 'username':
                t1 = Thread(target=self.Twitter.getAllTweets, args=(st, searchtype,))
                t1.start()  
                #self.Twitter.getAllTweets(st, searchtype)
                message  = 'Twitter username collection on %s started.' % searchterm
                MSG['messages'] .append(message)
            elif searchtype == 'term':
                t2 = Thread(target=self.Twitter.getAllTweets, args=(st, searchtype,))
                t2.start() 
                self.Twitter.getAllTweets(searchterm, 'hashtags')
                message = 'Twitter term collection on %s started.' % searchterm
                MSG['messages'].append(message)                
            elif searchtype == 'associates':
                t3 = Thread(target=self.Twitter.getAssociates, args=(st,))
                t3.start()
                message = 'Twitter associates collection on %s started.' % searchterm
                MSG['messages'].append(message)
            elif searchtype == 'location':
                latitude = float(latitude)
                longitude = float(longitude)
                t4 = Thread(target=self.Twitter.getTweetsByLocation, args=(latitude, longitude, ))
                t4.start()
                message = 'Twitter location collection on %d, %d started.' % (latitude, longitude)
                MSG['messages'].append(message) 
            elif searchtype == 'mentions':
                t5 = Thread(target=self.Twitter.getAllTweets, args=(st, searchtype,))
                t5.start()  
                MSG['messages'].append('Twitter mentions collection on %s started.' % searchterm)
            elif searchtype == 'hashtags':
                t6 = Thread(target=self.Twitter.getAllTweets, args=(st, searchtype,))
                t6.start()  
                MSG['messages'].append('Twitter hashtag collection on %s started.' % searchterm)
                    
        elif isinstance(NoPreviousSearch, str) == True :
            message = 'Previous Search %s' % NoPreviousSearch 
            MSG['messages'].append(message)
    
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-run_twitter]: Received: %s %s" % (TS, searchtype, searchterm))            

        return MSG
        
    def run_acled(self, PIRREF, searchdate, searchlocation):

        searchtermplus = "%s-%s-%s" % (PIRREF, searchdate, searchlocation)
        NoPreviousSearch, eGUID = self.log_user_search('ACLED', 'API_PULL', searchtermplus)
        self.pir_justification(PIRREF, eGUID, 'Collection')
        if NoPreviousSearch != False:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            r = self.PubDB.getAcledData_with(eGUID, searchdate, searchlocation)
            t = Thread(target=self.PubDB.getAcledDataThread, args=(r, eGUID,))
            t.start()       
            return {'messages' : ['Search complete with ID %s. Extracting ACLED data to POLE' % eGUID]}
        else:
            message = {'messages' : ['Search already made with %s.' % eGUID]}
            return message    
        
    def run_ucdp(self, PIRREF, startDate, endDate, countries, geography):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        searchtermplus = "%s-%s-%s-%s-%s" % (PIRREF, startDate, endDate, countries, geography)
        NoPreviousSearch, eGUID = self.log_user_search('UCDP', 'API_PULL', 'DATE')
        print('%s_APP-Model-run_ucdp]%s, %s,%s' % (TS, endDate, countries, geography))
        self.pir_justification(PIRREF, eGUID, 'Collection')
        if NoPreviousSearch != False:
            r = self.PubDB.getUcpdData_with(eGUID, startDate, endDate, countries, geography)
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print('%s_APP-Model-run_ucdp] nextURL: %s, %s results in %s pages' % (TS, r['NextPageUrl'], r['TotalCount'], r['TotalPages']))
            t = Thread(target=self.PubDB.getUcpdDataThread, args=(r, eGUID,))
            t.start()
            return({'messages' : ['Search resulted in %d records which are being extracted into POLE format now.' % r['TotalCount']]})
        else:
            return({'messages' : ['Search already made.']})
        
        
    def run_search(self, channel, searchtype, searchterm):
        
        channel = channel.lower()
        searchtype = searchtype.lower()
        searchterm = searchterm.lower()
        NoPreviousSearch, eGUID = self.log_user_search(channel, searchtype, searchterm)
        if NoPreviousSearch != False:
            if channel == 'twitter':
                self.Twitter.searchID = eGUID
                if searchtype == 'username':
                    t1 = Thread(target=self.Twitter.getAllTweets, args=(searchterm, searchtype,))
                    t1.start()                    
                    t2 = Thread(target=self.Twitter.getAssociates, args=(searchterm,))
                    t2.start()
                    message = 'Twitter user collection on %s started.' % searchterm
                else:
                    XCOORD = float(searchterm[:searchterm.find(",")])
                    YCOORD = float((searchterm[searchterm.find(",")+2:]).replace(",", "").strip())                
                    t = Thread(target=self.Twitter.getTweetsByLocation, args=(XCOORD, YCOORD, ))
                    t.start()
                    message = 'Twitter location collection on %d, %d started.' % (XCOORD, YCOORD)
        elif isinstance(NoPreviousSearch, str) == True :
            message = NoPreviousSearch
        
        else:
            message = "Just searched. Wait a bit."
        
        return message
            
    def log_user_search(self, channel, searchtype, searchterm):
        
        if isinstance(searchtype, list):
            CLASS1 = ""
            for s in searchtype:
                CLASS1 = s.upper() + '-' + CLASS1
        else:
            CLASS1   = searchtype.upper()
        today = datetime.now()
        # Log the search as an event
        TYPE     = 'UserAction'
        CATEGORY = 'OSINT-%s' % channel
        LANG     = 'en'
        TIME     = today.strftime("%H:%M:%S")
        DATE     = today.strftime("%Y-%m-%d")
        DTG      = int(today.strftime("%Y%m%d%H%M%S"))
        XCOORD   = 48.8566
        YCOORD   = 2.3522    
        ORIGIN   = self.username
        ORIGINREF = "%s%s%s" % (channel, searchtype, searchterm) 
        LOGSOURCE = 'COIN' 
        DESC     = 'Search type %s on channel %s for %s on %s.' % (searchtype, channel, searchterm, DATE)
        
        eGUID = DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
        NoPreviousSearch = DB.insertRelation(self.GUID, 'User', 'SEARCHED', eGUID, 'Event') 
        # The search was executed before, but how long ago
        if NoPreviousSearch == False:

            return NoPreviousSearch, "Search conducted"
        
        return NoPreviousSearch, eGUID
    
    def merge_entities(self, iObj):
        
        messages = {}
        messages['message'] = DB.merge_entities('person', iObj['AGUID'], iObj['PGUID'])
      
        return messages
    
    def from_file(self, filename, fileType, fileURL, GUID):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Model-from_file]: Received: %s %s %s" % (TS, filename, fileType, fileURL))  
        
        today = datetime.now().strftime("%F %H:%M:%S")
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())  
        intel_GUID = self.user_event('PROCESSED_INTEL', fileType, DTG, DATE, TIME, 'Extract from %s' % filename, GUID)
        DB.insertRelation(intel_GUID, 'Event', 'AnalysisToSupport', iObj['ORIGIN'], 'Object')
        self.PubDB.setProcessID(intel_GUID)
        view = self.PubDB.getFile(fileURL)

        if fileType == 'POLICE':
            t = Thread(target=self.PubDB.ETLPolice2Graph, args=(view,))
            t.start()
        
        elif fileType == 'SOCIAL':
            t = Thread(target=self.PubDB.ETLSocial2Graph, args=(view,))
            t.start()        
        
        elif fileType == 'EDUCATION':
            message = self
        
        elif fileType == 'ACLED':
            t = Thread(target=self.PubDB.ETLACLED2Graph, args=(view,))
            t.start()
        
        elif fileType == 'GTD':
            t = Thread(target=self.PubDB.ETLGDELT2Graph, args=(view,))
            t.start()
                
        elif fileType == 'HUMINT':
            t = Thread(target=self.PubDB.ETLGDELT2Graph, args=(view,))
            t.start()     
               
        return "%s Started extraction %s." % (TS, filename)

    def add_pir(self, PIR):
        
        today = datetime.now().strftime("%F %H:%M:%S")    
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())       
        # Change to ID of strat 
        intel_DESC = "%s PIR published by %s on %s %s. Description:%s" % (PIR['CATEGORY'], self.username, DATE, TIME, PIR['DESC'])    
        actionGUID = self.user_event('PUBLISHED_PIR', PIR['CATEGORY'], DTG, DATE, TIME, intel_DESC, self.GUID)
    
        ORIGIN = self.username
        ORIGINREF = "%s%s" % (self.GUID, PIR['DESC'])
        LOGSOURCE = 'A1'  
        CLASS1 = PIR['CLASS1']
        XCOORD = 0.0
        YCOORD = 0.0    
        tags = [x.strip() for x in PIR['CLASS3'].lower().split(",")]
        tags = set(tags)
        PIR['CLASS3'] = len(tags)
        PIRGUID = DB.insertObject('PIR', PIR['CATEGORY'], PIR['DESC'], PIR['CLASS1'], PIR['CLASS2'], PIR['CLASS3'], ORIGIN, ORIGINREF, LOGSOURCE) 
        DB.insertRelation(actionGUID, 'Event', 'INVOLVES', PIRGUID, 'Object')            
        DB.insertRelation(PIRGUID, 'Object', 'AnalysisToSupport', int(CLASS1), 'Object')
        if PIR['CLASS3'] != '': 
            TYPE = "Tag"
            CATEGORY = "Term"
            CLASS2 = 0
            CLASS3 = 0   
            ORIGIN = 'COIN%s' % (str(self.GUID))
            for tag in tags:
                ORIGINREF = 'COINTAG%s' % tag
                tagGUID = DB.insertObject(TYPE, CATEGORY, tag, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                DB.insertRelation(PIRGUID, 'Object', 'INCLUDES_TAG', tagGUID, 'Object')               
        
        for loc in PIR['Locations']:
            DB.insertRelation(PIRGUID, 'Object', 'INVOLVES', loc, 'Location')        

        # Add relation to parent and child (supporting/answers) to other fields.

        newPIR = {'GUID'     : PIRGUID,
                  'CATEGORY' : PIR['CATEGORY'],
                  'ORIGIN'   : self.username,
                  'NAME'     : PIR['DESC'],
                  'CLASS2'   : PIR['CLASS2'] }
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-Models-add_pir]: Added PIR %s from action %s" % (TS, newPIR, actionGUID))        
        
        return newPIR
    
    def add_strat(self, STRAT):
        
        today = datetime.now().strftime("%F %H:%M:%S")    
        DATE = today[:10]
        TIME = today[-8:]
        DTG = int(today.replace("-", "").replace(":", "").replace(" ", "").strip())       
        intel_DESC = "%s STRAT published by %s on %s %s. Description:%s" % (STRAT['CATEGORY'], self.username, DATE, TIME, STRAT['DESC'])    
        actionGUID = self.user_event('PUBLISHED_STRAT', STRAT['CATEGORY'], DTG, DATE, TIME, intel_DESC, self.GUID)
    
        ORIGIN = self.username
        ORIGINREF = "%s%s" % (self.GUID, STRAT['DESC'])
        LOGSOURCE = 'A1'  
        CLASS1 = STRAT['CLASS1']
        XCOORD = 0.0
        YCOORD = 0.0 
        STRAT['CLASS2'] = 1
        STRAT['CLASS3'] = len(CLASS1)
        STRATGUID = DB.insertObject('STRAT', STRAT['CATEGORY'], STRAT['DESC'], STRAT['CLASS1'], STRAT['CLASS2'], STRAT['CLASS3'], ORIGIN, ORIGINREF, LOGSOURCE) 
        DB.insertRelation(actionGUID, 'Event', 'INVOLVES', STRATGUID, 'Object')  
        
        # Get the locations in the strategy
        if STRAT['Locations'] != None:
            for loc in STRAT['Locations']:
                DB.insertRelation(STRATGUID, 'Event', 'INVOLVES', loc, 'Location')        
        newSTRAT = {'GUID' : STRATGUID,
                    'DESC' : intel_DESC,
                    'NAME' : "%s %s" % (STRAT['CATEGORY'], STRAT['CLASS1'])
                    }
        
        return newSTRAT
    
    def update_condis(self):
        '''
        Only used when graph is attached
        '''
        query = '''MATCH(a:Location) RETURN a.GUID AS L_GUID, a.TYPE AS L_TYPE, a.DESC AS L_DESC, a.XCOORD AS L_XCOORD, 
        a.YCOORD AS L_YCOORD, a.ZCOORD AS L_ZCOORD, a.CLASS1 AS L_CLASS1, a.ORIGIN AS L_ORIGIN, a.ORIGINREF AS L_ORIGINREF, 
        a.LOGSOURCE AS L_LOGSOURCE'''
        '''
        locations = self.oGraph.run(query)
        for l in locations:
            self.HDB.insertLocation(l['L_GUID'], l['L_TYPE'], l['L_DESC'], l['L_XCOORD'], l['L_YCOORD'], l['L_ZCOORD'], l['L_CLASS1'], l['L_ORIGIN'], l['L_ORIGINREF'], l['L_LOGSOURCE'])       
        '''    
    
    
def todays_recent_posts(n):
    today = datetime.now().strftime("%F")
    query = """
    MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
    WHERE post.date = '%s'
    RETURN user.username AS username, post, COLLECT(tag.name) AS tags
    ORDER BY post.timestamp DESC LIMIT %d
    """ % (today, n)
    results = []
    '''
    qRun = graph.run(query)
    
    for e in qRun:
        results.append(e)  
    
    '''
    return results

def todays_recent_intel(n):
    today = datetime.now().strftime("%F")
    query = "MATCH (user:User)-[:PUBLISHED_INTEL]->(a) RETURN user.username AS username, a.GUID AS idintel, a.ORIGIN AS date, user.GUID AS iduser, a.DESC AS description ORDER BY date DESC LIMIT %d" % (n)
    
    results = []
    '''
    qRun = graph.run(query)
    
    for e in qRun:
        if UI5 == True:
            data = {}
            data['username']    = e['username']
            data['idintel']     = e['idintel']
            data['date']        = e['date']
            data['iduser']      = e['iduser']
            data['description'] = e['description']            
            results.append(data)      
        else:
            results.append(e)   
    '''
    return results

def todays_recent_searches(n):
    today = datetime.now().strftime("%F")
    query = "MATCH (user:User)-[:SEARCHED]-(a) RETURN user.username AS username, a.DESC AS description, a.DATE AS date ORDER BY date DESC LIMIT %d" % (n)
    results = []
    '''
    qRun = graph.run(query)
    
    for e in qRun:
        if UI5 == True:
            data = {}
            data['username']      = e['username']
            data['description']   = e['description']
            data['date']          = e['date']
            results.append(data)      
        else:
            results.append(e)  
            
    if UI5 == True:
        results = json.dumps(results) 
    '''
    return results

def recent_all():
    query = '''
    MATCH (a:User)-[r]-(b)-[]-(c) 
    WHERE b.TYPE = 'UserAction' RETURN a.username AS username, 
    type(r) AS uatype, b.DESC AS description, b.TIME AS time, b.DATE AS date, b.GUID as GUID, COUNT(c) AS resultcount, b.DTG AS DTG
    ORDER BY DTG DESC LIMIT 25'''
    
    results = []
    '''
    qRun = graph.run(query)
    for e in qRun:
        data = {}
        data['username']      = e['username']
        data['uatype']        = e['uatype']
        data['description']   = e['description']
        data['date']          = e['date']
        data['GUID']          = e['GUID']
        data['resultcount']   = e['resultcount']
        data['DTG']           = e['DTG']
        results.append(e)       
    '''                    
    return results

def recent_OSINT():
    query = '''
    MATCH (a:User)-[r]-(b)-[]-(c) 
    WHERE b.CATEGORY CONTAINS 'OSINT-' RETURN a.username AS username, 
    type(r) AS uatype, b.DESC AS description, b.TIME AS time, b.DATE AS date, b.GUID as GUID, COUNT(c) AS resultcount, b.DTG AS DTG
    ORDER BY DTG DESC'''
    results = []
    '''
    qRun = graph.run(query)
    
    for e in qRun:
        data = {}
        data['username']      = e['username']
        data['uatype']        = e['uatype']
        data['description']   = e['description'].replace(']', '').replace('[', '')
        data['date']          = e['date']
        data['GUID']          = e['GUID']
        data['resultcount']   = e['resultcount']
        data['DTG']           = e['DTG']
        results.append(e)       
    '''                    
    return results

def tileStats():
    '''
    Returns a JSON style result of all tile KPIs
    
    '''
    tile_stats = DB.tileStats()
    return tile_stats 

def recent_PIR():
    query = '''
    MATCH (a:User)-[r:PUBLISHED_PIR]-(b)-[]-(c) 
    WHERE b.TYPE = 'UserAction' RETURN a.username AS username, 
    type(r) AS uatype, b.DESC AS description, b.TIME AS time, b.DATE AS date, b.GUID as GUID, COUNT(c) AS resultcount, b.DTG AS DTG
    ORDER BY DTG DESC LIMIT 25'''
    
    results = []
    '''
    qRun = graph.run(query)
    
    
    for e in qRun:
        data = {}
        data['username']      = e['username']
        data['uatype']        = e['uatype']
        data['description']   = e['description']
        data['date']          = e['date']
        data['GUID']          = e['GUID']
        data['resultcount']   = e['resultcount']
        data['DTG']           = e['DTG']
        results.append(e)       
    '''                  
    return results        

def dashboard(dashtype):
    # Get the latest Event and the resulting entities from that event
    if dashtype == 'geo':
        query = '''MATCH(a:Location) WHERE a.XCOORD > 0 OR a.XCOORD < 0 RETURN a.GUID, a.DESC, a.XCOORD, a.YCOORD'''
    else:
        query = '''
        MATCH (a:Event) WITH MAX(a.DTG) AS maxDTG MATCH (a:Event)-[]-(b) 
        WHERE a.DTG = maxDTG 
        RETURN a.DTG AS DTG, a.DATE as date, a.TIME as time, a.CATEGORY, a.DESC as eventdesc, LABELS(b)[0] AS entitytype, 
        b.TYPE AS subtype, a.CATEGORY AS category, b.DESC AS odesc, b.FNAME AS description'''
    
    results = []
    '''
    qRun = graph.run(query)
    
    for e in qRun:
        if UI5 == True:
            data = {}
            data['DTG']         = e['DTG']
            data['date']        = e['date']
            data['time']        = e['time']
            data['eventdesc']   = e['eventdesc']
            data['entitytype']  = e['entitytype']
            data['subtype']     = e['subtype']
            data['category']    = e['category']
            data['odesc']       = e['odesc']
            data['description'] = e['description']
            results.append(data)      
        else:
            results.append(e)  
            
    if UI5 == True:
        results = json.dumps(results)    
    '''     
    return results

def get_uploads():
    query = '''
    MATCH(a:User)-[r:PROCESSED_INTEL]-(b)-[]-(c) 
    RETURN a.username AS username, COUNT(c) AS resultcount, b.DTG AS DTG, b.DATE as date, b.TIME as time, 
    b.CATEGORY AS filetype, b.GUID AS GUID, b.DESC AS description ORDER BY DTG DESC
    '''
    results = []
    '''
    entities = graph.run(query)
    
    for e in entities:
        results.append(e)
    '''
    return results   

def get_TAruns():
    query = '''
    MATCH(a:User)-[r:TEXT_ANALYTICS]-(b)-[]-(c) 
    RETURN a.username AS username, COUNT(c) AS resultcount, b.DTG AS DTG, b.DATE as date, b.TIME as time, 
    b.GUID AS GUID, b.DESC AS description ORDER BY DTG DESC
    '''
    results = []
    '''
    entities = graph.run(query)
    
    for e in entities:
        results.append(e)
    '''
    return results 
    
def get_tasks():
    
    query = '''
    MATCH(a:User)-[r:PUBLISHED_TASK]-(b)-[]-(c:User) 
    RETURN a.username AS tasker, c.username AS tasked, b.DTG AS DTG, b.DATE as date, b.TIME as time, 
    b.DESC AS description, b.GUID AS GUID ORDER BY DTG DESC
    '''
    
    results = []
    '''
    entities = graph.run(query)
    for e in entities:
        results.append(e)
    
    '''
    return results    
    
def get_users():
    
    results = DB.get_users()
    return results

def get_locations():
    '''
    DB returns list of dictionary (JSON) 
    {'GUID' : '', 'LOCNAME', ''}
    ordered by distinct location names
    '''
    results = DB.get_locations()
    return results

def load_locations():
    '''
    Load up locations from the excel book attached to the package
    '''
    t = Thread(target=DB.preLoadLocations,)
    t.start()    
        
def get_PIR():
    '''
    DB returns list of dictionary (JSON) 
    {'GUID' : '', 'CATEGORY', ''}
    ordered by Category
    '''
    results = DB.get_PIR()
    return results

def get_STR():
    '''
    DB returns list of dictionary (JSON) 
    {'GUID' : '', 'CATEGORY', '', 'CLASS1', '', 'DESC', ''}}
    ordered by Description
    '''
    results = DB.get_STR()
    return results 
    
def get_entity_profile(GUID):
    
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-Model-get_entity_profile]: Getting user:" % (TS))    
     
    query = '''
    MATCH (n)-[r]-(e) 
    WHERE n.GUID = %s 
    RETURN n.GUID AS nGUID, LABELS(n)[0] AS ENTITY, n.DESC AS DESC, n.DATE AS DATE, n.CATEGORY AS CATEGORY, n.CLASS1 AS CLASS1, n.CLASS2 AS CLASS2, n.TIME AS TIME, n.ORIGIN AS ORIGIN, 
    n.POB AS POB, n.GEN AS GEN, n.DOB AS DOB, n.FNAME AS FNAME, n.LNAME AS LNAME, n.XCOORD AS XCOORD, n.YCOORD AS YCOORD, n.ZCOORD AS ZCOORD,n.CLASS3 AS CLASS3, n.ORIGINREF AS ORIGINREF, n.LOGSOURCE AS LOGSOURCE,
    e.GUID AS eGUID, LABELS(e)[0] AS eENTITY, e.DESC AS eDESC, e.DATE AS eDATE, e.TIME AS eTIME, e.CATEGORY AS eCATEGORY, 
    e.CLASS1 AS eCLASS1, e.CLASS2 AS eCLASS2, e.CLASS3 AS eCLASS3, e.DOB AS eDOB, e.DTG AS eDTG, e.FNAME AS eFNAME, e.LNAME AS eLNAME, 
    e.POB AS ePOB, e.TYPE AS eTYPE, e.XCOORD AS eXCOORD, e.YCOORD AS eYCOORD,
    type(r) AS reltyp''' % GUID
    
    results = []
    '''
    qRun = graph.run(query)
    
    
    pcount = 0
    ocount = 0
    lcount = 0
    ecount = 0
    rcount = 0
    
    for e in qRun:
        data = {}
        # All data about the profile node
        data['nGUID']     = e['nGUID']
        data['ENTITY']    = e['ENTITY']
        data['DESC']      = e['DESC']
        data['DATE']      = e['DATE']
        data['CATEGORY']  = e['CATEGORY']
        data['CLASS1']    = e['CLASS1']
        data['TIME']      = e['TIME']
        data['POB']       = e['POB']
        data['DOB']       = e['DOB']
        data['FNAME']     = e['FNAME']
        data['LNAME']     = e['LNAME']
        data['GEN']       = e['GEN']
        data['CLASS1']    = e['CLASS1']
        data['CLASS2']    = e['CLASS2']
        data['CLASS3']    = e['CLASS3']    
        data['XCOORD']    = e['XCOORD']
        data['YCOORD']    = e['YCOORD']
        data['ZCOORD']    = e['ZCOORD']  
        data['ORIGIN']    = e['ORIGIN']
        data['LOGSOURCE'] = e['LOGSOURCE']
        data['ORIGINREF'] = e['ORIGINREF']         
        
        # All the data about related entities
        data['eGUID']     = e['eGUID']
        data['eENTITY']   = e['eENTITY']
        data['eDESC']     = e['eDESC']
        data['eDATE']     = e['eDATE']
        data['eTIME']     = e['eTIME']
        data['eCATEGORY'] = e['eCATEGORY']  
        data['eCLASS1']   = e['eCLASS1']
        data['eCLASS2']   = e['eCLASS2']
        data['eCLASS3']   = e['eCLASS3']
        data['ePOB']      = e['ePOB']  
        data['eDOB']      = e['eDOB']
        data['eDTG']      = e['eDTG'] 
        data['eFNAME']    = e['eFNAME']
        data['eLNAME']    = e['eLNAME']
        data['eTYPE']     = e['eTYPE']
        data['eXCOORD']   = e['eXCOORD'] 
        data['eYCOORD']   = e['eYCOORD'] 
        data['rcount']    = rcount
        data['pcount']    = pcount
        data['ocount']    = ocount
        data['lcount']    = lcount
        data['ecount']    = ecount
        data['reltyp']    = e['reltyp']
        
        rcount+=1
        if e['eENTITY'] == 'Person':
            pcount+=1
        elif e['eENTITY'] == 'Object':
            ocount+=1
            
        elif e['eENTITY'] == 'Location':
            lcount+=1
                
        elif e['eENTITY'] == 'Event':
            ecount+=1
            
        results.append(data)

    if UI5 == True:
        results = json.dumps(results)
        
    results[0]['pcount'] = pcount
    results[0]['ocount'] = ocount
    results[0]['lcount'] = lcount
    results[0]['ecount'] = ecount
    results[0]['rcount'] = rcount
    
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-Model-get_entity_profile]: User Data: %s" % (TS, results))       
    '''
    return results




'''
iType = 'Person'
Atr1 = 'GUID'
Var1 = 1000000042
Atr2 = 'GUID'
Var2 = 1000000043  
u = User('Tester')
channel = 'twitter'
searchtype = 'location'
searchterm = '51.5074, 0.1268'
#u.run_search(channel, searchtype, searchterm)
message = u.merge_entities(iType, Var1, Var2)
print(message)
'''
