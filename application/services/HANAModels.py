#!/usr/bin/env python3
# -*- coding: utf-8 -*
import time, json, requests, random, jsonify
from passlib.hash import bcrypt
from datetime import datetime, timedelta
import os, uuid
import pandas as pd
import pyhdb
from openpyxl import load_workbook
from openpyxl import Workbook
from threading import Thread


debugging = False

class HANAModel():

    def __init__(self):

        if '\\' in os.getcwd():
            auth = '%s\\application\\services\\config\\AUTH_HANAg.json' % (os.getcwd())
            self.BaseBook = '%s\\application\\services\\data\\BaseBook.xlsx' % (os.getcwd())
            self.SocialPath = '%s\\application\\services\\data\\Social.csv' % (os.getcwd())
        else:
            try:
                auth = '%s/application/services/config/AUTH_HANA.json' % (os.getcwd())
                self.BaseBook   = '%s/application/services/data/BaseBook.xlsx' % (os.getcwd()) # debugging line
                self.SocialPath = '%s/application/services/data/Social.csv' % (os.getcwd())

            except:
                auth = '%s/config/AUTH_HANA.json' % (os.getcwd())
                self.BaseBook   = '%s/data/BaseBook.xlsx' % (os.getcwd()) # debugging line
                self.SocialPath   = '%s/data/Social.csv' % (os.getcwd()) # debugging line

        keys = json.loads(open(auth).read())
        self.cursor     = None
        self.Live       = False
        self.host       = keys['host']
        self.port       = keys['port']
        self.user       = keys['user']
        self.password   = keys['password']
        self.userCondis = keys['userCondis']
        self.pswdCondis = keys['pswdCondis']
        self.curCondis = None
        self.autocommit = True
        self.targetGUID = None
        self.connected  = False
        self.simdata = False
        self.Locations  = []
        #self.http_proxy = os.environ['HTTP_PROXY']
        #self.proxies    = {'http' : self.http_proxy, 'https' : self.http_proxy}
        self.ConDisSrc  = 'COIN_APP'
        self.timeformat = '%Y%m%d'
        self.ResEvent    = []
        self.ResLocation = []
        self.ResObject   = []
        self.ResPerson   = []
        self.ResRelation = []
        self.Verbose = True
        self.GoogleMapsAPI = 'https://maps.googleapis.com/maps/api/js?key=AIzaSyBIXb-g4Z-AP8qI1v_bgRq4rc4GGBtKqmM&callback=initMap'

    def initialize(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-initialize]: process started." % (TS))

        if self.connected == False:
            self.ConnectToHANA()
        check = ''' SELECT "P_GUID" FROM "POLER"."PERSON" WHERE CONTAINS (("P_ORIGINREF"), ' ')'''
        if len(self.cursor.execute(check).fetchall()) < 1:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            message = "[%s_HDB-initialize]: Creating POLER schema and system user." % TS
            self.initialize_POLER()
            self.initialize_users()
            self.preLoadPeopleThread()
            self.preLoadVPScene1()
            self.preLoadPIRandSTRAT()
            self.preLoadLocationsThread()

        check = ''' SELECT * FROM "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" WHERE "REL_DESC" = 'AccountCreated'; '''
        if len(self.curCondis.execute(check).fetchall()) < 1:
            self.initialize_CONDIS_Customization()
            self.initialize_users() # This is done on the first time only and if there are no custmoizations then it means no resets have occurred so user needs to be created

        return message

    def set_user_auth(self, user):


        return message

    def set_auth_menu(self, menu, user):

        return menu


    def initialize_users(self):
        password = 'welcome123'
        self.insertUser('SYSTEM', bcrypt.encrypt(password), 'SYSTEM@email.com', '000-000', 'A1A2A3B1C1', 'None', 'Admin')
        password = 'test123'
        self.insertUser('Tim S', bcrypt.encrypt(password), 'TimSoshel@email.com', '555-5555', 'A1A2A3B1', 'None', 'Social')
        password = 'test123'
        self.insertUser('Patty', bcrypt.encrypt(password), 'Patty@email.com', '555-5555', 'A1A2A3B1C1', 'None', 'Field')
        password = 'test123'
        self.insertUser('Farah', bcrypt.encrypt(password), 'Farah@email.com', '555-5555', 'A1A2A3B1C1', 'None', 'Manager')
        password = 'test123'
        self.insertUser('Hakim', bcrypt.encrypt(password), 'Hakim@email.com', '555-5555', 'A1A2A3B1B2', 'None', 'Health')
        password = 'test123'
        self.insertUser('حكيم', bcrypt.encrypt(password), 'HakimArabic@email.com', '555-5555', 'A1A2A3B1B2', 'None', 'Arabic')
        self.insertUser('Hans', bcrypt.encrypt(password), 'Hans@email.com', '555-5555', 'A1A2A3B1C1', 'None', 'Analyst')
        password = 'cantloginbecauserolewontseeanything'
        self.insertUser('Open Task', bcrypt.encrypt(password), 'OpenTasks@email.com', '555-5555', 'Open Task', 'None', 'Open to any role')

    def initialize_POLER(self):
        # Called on the initial setup of the application
        if self.cursor == None:
            self.ConnectToHANA()
        sql = '''
            DROP schema POLER CASCADE;
            '''
        try:
            self.cursor.execute(sql)
        except:
            pass
        sql = '''
        create schema POLER;
        '''
        self.cursor.execute(sql)
        sql = '''
        create column table "POLER"."PERSON"(
	"P_GUID" 		NVARCHAR (27) PRIMARY KEY,
	"P_GEN" 		NVARCHAR (1),
	"P_FNAME" 		NVARCHAR (60),
	"P_LNAME" 		NVARCHAR (60),
	"P_DOB"   		SECONDDATE,
	"P_POB"   		NVARCHAR (60),
	"P_ORIGIN"		NVARCHAR (60),
	"P_ORIGINREF" 		NVARCHAR (2000),
	"P_LOGSOURCE" 		NVARCHAR (200),
	"NODE_TYPE"		NVARCHAR (32)
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        create column table "POLER"."OBJECT"(
	"O_GUID" 		NVARCHAR (27) PRIMARY KEY,
	"O_TYPE" 		NVARCHAR (60),
	"O_CATEGORY" 		NVARCHAR (60),
	"O_DESC" 		NVARCHAR (5000),
	"O_CLASS1"   		NVARCHAR (200),
	"O_CLASS2"   		NVARCHAR (200),
	"O_CLASS3"   		NVARCHAR (200),
	"O_ORIGIN"		NVARCHAR (200),
	"O_ORIGINREF" 		NVARCHAR (2000),
	"O_LOGSOURCE" 		NVARCHAR (200),
	"NODE_TYPE"		NVARCHAR (32)
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        create column table "POLER"."LOCATION"(
	"L_GUID" 		NVARCHAR (27) PRIMARY KEY,
	"L_TYPE" 		NVARCHAR (40),
	"L_DESC" 		NVARCHAR (5000),
	"L_XCOORD"   		NVARCHAR (60),
	"L_YCOORD"   		NVARCHAR (60),
	"L_ZCOORD"   		NVARCHAR (60),
	"L_CLASS1"   		NVARCHAR (60),
	"L_ORIGIN"		NVARCHAR (200),
	"L_ORIGINREF" 		NVARCHAR (2000),
	"L_LOGSOURCE" 		NVARCHAR (200),
	"NODE_TYPE"		NVARCHAR (32)
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        create column table "POLER"."EVENT"(
	"E_GUID" 		NVARCHAR (27) PRIMARY KEY,
	"E_TYPE" 		NVARCHAR (40),
	"E_CATEGORY" 		NVARCHAR (40),
	"E_DESC" 		TEXT,
	"E_LANG"		NVARCHAR (40),
	"E_CLASS1"   		NVARCHAR (200),
	"E_TIME"   		TIME,
	"E_DATE"   		DATE,
	"E_DTG"			NVARCHAR (60),
	"E_XCOORD"   		NVARCHAR (60),
	"E_YCOORD"   		NVARCHAR (60),
	"E_ORIGIN"		NVARCHAR (200),
	"E_ORIGINREF"	 	NVARCHAR (2000),
	"E_LOGSOURCE" 		NVARCHAR (200),
	"NODE_TYPE"		NVARCHAR (32)
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        create column table "POLER"."RELATION"(
	"GUID" 			NVARCHAR (27) PRIMARY KEY,
	"TYPE" 			NVARCHAR (40),
	"SOURCETYPE" 		NVARCHAR (40),
	"TARGETTYPE" 		NVARCHAR (40),
	"SOURCEGUID"   		NVARCHAR (27) not null,
	"TARGETGUID"   		NVARCHAR (27) not null
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        CREATE COLUMN TABLE "POLER"."MASTER_NODE"(
	ENTITY_GUID 		NVARCHAR(27) PRIMARY KEY,
	POLER_CLASS 		NVARCHAR(40),
	ENTITY_TYPE 		NVARCHAR(40),
	ENTITY_CATEGORY 	NVARCHAR(40),
	ENTITY_DATE 		DATE,
	DESCRIPTION 		NVARCHAR(2000),
	CLASS_1 		NVARCHAR(200),
	CLASS_2 		NVARCHAR(200),
	CLASS_3 		NVARCHAR(200)
        );
        '''
        self.cursor.execute(sql)
        sql = '''
        GRANT SELECT ON SCHEMA POLER TO _SYS_REPO WITH GRANT OPTION;
        '''
        self.cursor.execute(sql)
        sql = '''
        CREATE GRAPH WORKSPACE "POLER"."GRAPH"
	EDGE TABLE "POLER"."RELATION"
		SOURCE COLUMN "SOURCEGUID"
		TARGET COLUMN "TARGETGUID"
		KEY COLUMN "GUID"
	VERTEX TABLE "POLER"."MASTER_NODE"
		KEY COLUMN "ENTITY_GUID"
        '''
        self.cursor.execute(sql)

        sql = '''
        CREATE COLUMN TABLE "POLER"."MASTER_TA"(
	ENTITY_GUID 		NVARCHAR(27) PRIMARY KEY,
	RAW_TEXT 		NCLOB
        );
        '''
        self.cursor.execute(sql)

        sql = '''
        CREATE FULLTEXT INDEX RTT on POLER.MASTER_TA (RAW_TEXT)
        LANGUAGE DETECTION ('en','ar')
        FAST PREPROCESS OFF
        ASYNC
        configuration 'EXTRACTION_CORE'
        TEXT ANALYSIS ON
        TEXT MINING ON;
        '''
        self.cursor.execute(sql)
        '''
        insert into POLER.MASTER_TA2 select * from POLER.MASTER_TA;
        alter table POLER.MASTER_TA2 add (DETECTED_LANG NVARCHAR(2) null);

        update MASTER_TA2
          set DETECTED_LANG=TA_LANGUAGE
          from MASTER_TA2, "$TA_RTT" t2 where MASTER_TA2.ENTITY_GUID=t2.ENTITY_GUID;

        '''

        #sql = '''
        #    CREATE FULLTEXT INDEX "TA_RT_CORE" ON "POLER"."MASTER_TA"("RAW_TEXT")
        #    TEXT ANALYSIS ON
        #    CONFIGURATION 'EXTRACTION_CORE_VOICEOFCUSTOMER';
        #    '''
        #self.cursor.execute(sql)


    def initialize_CONDIS_Customization(self):
        if self.curCondis == None:
            self.ConnectToHANA()
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'AC1', 'AccountCreated');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'AN1', 'AnalysisToSupport');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'BO1', 'BornOn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'BO2', 'BornIn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CH1', 'ChargedWith');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CO1', 'CollectionToSupport');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CO2', 'CommittedCrime');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CR1', 'CreatedAt');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CR2', 'CreatedBy');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'CR3', 'CreatedOn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'DO1', 'DocumentIn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'DO2', 'DocumentMentioning');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'DO3', 'DocumentedBy');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'FM1', 'Family');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'FO1', 'Follows');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'FO2', 'FOUND');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'FR1', 'FromFile');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'HA1', 'HasAttribute');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'HA2', 'HasStatus');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'IN1', 'IncludesTag');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'IN2', 'Involves');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'KN1', 'Knows');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'LI1', 'LivesAt');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'LO1', 'LocatedAt');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'MO1', 'ModifiedBy');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'MO2', 'ModifiedOn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'ON1', 'On');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'OC1', 'OccurredAt');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'OF1', 'OfType');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'OW1', 'Owns');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'PA1', 'PartOf');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'PR1', 'ProcessedIntel');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'PU1', 'Published');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'PU2', 'PublishedIntel');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'PU3', 'PublishedTask');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'RE1', 'ReportedAt');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'RE2', 'RegisteredOn');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'RE3', 'ReferenceLink');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'RE4', 'RecordedBy');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'SE1', 'Searched');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'SU1', 'SubjectofContact');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'SU2', 'Supporting');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TA1', 'Tagged');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TA2', 'TaskedTo');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TA3', 'TAReference');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TA4', 'TextAnalytics');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TW1', 'Tweeted');'''
        self.curCondis.execute(sql)
        sql = '''INSERT INTO "CONDIS_SCHEMA"."com.sap.condis::content.TT_RELATION" VALUES ('en', 'TW2', 'TweetLocation');'''
        self.curCondis.execute(sql)

    def initialize_reset(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-initialize_reset]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION";'''
        self.curCondis.execute(sql)
        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_NODES";'''
        self.curCondis.execute(sql)
        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::content.OBJECT";'''
        self.curCondis.execute(sql)
        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::content.PERSON";'''
        self.curCondis.execute(sql)
        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::content.LOCATION";'''
        self.curCondis.execute(sql)
        sql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::content.INCIDENT";'''
        self.curCondis.execute(sql)
        sql = ''' call "CONDIS_SCHEMA"."com.sap.condis::content.SP_DemoDataCreation"(); '''
        self.curCondis.execute(sql)
        try:
            sql = ''' DELETE FROM "POLER"."EVENT";'''
            self.cursor.execute(sql)
            sql = ''' DELETE FROM "POLER"."PERSON";'''
            self.cursor.execute(sql)
            sql = ''' DELETE FROM "POLER"."LOCATION";'''
            self.cursor.execute(sql)
            sql = ''' DELETE FROM "POLER"."RELATION";'''
            self.cursor.execute(sql)
            sql = ''' DELETE FROM "POLER"."OBJECT";'''
            self.cursor.execute(sql)
            sql = ''' DELETE FROM "POLER"."MASTER_NODE";'''
            self.cursor.execute(sql)
        except:
            self.initialize_POLER()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-initialize_reset]: All clear." % (TS))

    def goLive(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-goLive]: process started." % (TS))

        if self.connected == False:
            self.ConnectToHANA()
        if self.Live == False:
            self.Live == True
        '''
        if 'HTTP_PROXY' not in os.environ.keys():
            print("[*] No http proxy detected. Setting now.")
            os.environ['HTTP_PROXY'] = self.http_proxy
        if 'HTTPS_PROXY' not in os.environ.keys():
            print("[*] No https proxy detected. Setting now.")
            os.environ['HTTPS_PROXY'] = self.http_proxy
        if 'NO_PROXY' not in os.environ.keys():
            print("[*] No NO proxy detected. Setting now.")
            os.environ['NO_PROXY'] = self.no_proxy
        '''
        print("[*] Collection and Database connected for live streaming.")

    def firstrun(self):
            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting Locations." % (TS))
            self.preLoadLocations()

            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting People." % (TS))
            self.preLoadPeople()

            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting Objects." % (TS))
            self.preLoadObjects()

            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting GTD." % (TS))
            self.ETLGTD2Graph()

            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting ACLED." % (TS))
            self.ETLACLED2Graph()

            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-firstrun]: Starting UCDP." % (TS))
            self.ETLUCDP2Graph()

    def getResponse(self, url, auth):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-getResponse]: process started." % (TS))

        if auth == None:
            try:
                response = requests.get(url)
            except:
                s = requests.Session()
                s.trust_env = False
                response = s.get(url)
        else:
            try:
                response = requests.get(url, auth=auth)
            except:
                s = requests.Session()
                s.trust_env = False
                response = s.get(url, auth=auth)

        return response

    def menuFill(self, uaa):

        if self.cursor == None:
            self.ConnectToHANA()

        menu = {'LOCATIONS' : [],
                'PIR'       : [],
                'STR'       : [],
                'PERSONS'   : [],
                'OBJECTS'   : [],
                'EVENTS'    : [],
                'RELS'      : [],
                'TILESTATS' : {},
                'USERS'     : [],
                'TARUNS'    : [],
                'TASKS'     : [],
                'Tprofile'  : [],
                'Pprofile'  : [],
                'Oprofile'  : [],
                'Lprofile'  : [],
                'Eprofile'  : [],
                'UCDP'      : [],
                'VULCHILD'  : [],
                'VULADULT'  : [],
                'POLERIZE'  : [],
                'POLEATTR'  : [],
                'POLENODE'  : [],
                'CURRENT'  : [],
                'POLERELS'  : [],
                'FILES'     : [],
                'MAPS'      : []
                }

        # Timestamp the process
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: Process started..." % (TS))

        menu['USERS']    = self.get_users()
        menu['VULCHILD'] = self.Graph_VP_CHILDREN(1, 5)
        menu['VULADULT'] = self.Graph_VP_ADULTS()
        menu['VulChildCount'] = len(menu['VULCHILD'])
        menu['TARUNS']   = self.get_ta_runs()
        #menu['UCDP'] = self.ucdp_menu()
        #menu['Vehicles'] = self.Graph_get_vehicles()
        print(uaa)
        sql = '''
        SELECT * FROM (SELECT "L_GUID", "L_ORIGIN", "L_XCOORD", "L_YCOORD", "L_LOGSOURCE", ROW_NUMBER()
        OVER(PARTITION BY "L_ORIGIN" ORDER BY "L_ORIGIN" DESC) rn FROM "POLER"."LOCATION") a
        WHERE rn = 1 ORDER BY "L_ORIGIN"
        '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            p = {}
            p['GUID']   = e[0]
            p['NAME']   = e[1]
            p['XCOORD'] = e[2]
            p['YCOORD'] = e[3]
            if len(p['NAME']) > 3:
                if e[4] in uaa:
                    menu['LOCATIONS'].append(p)
        menu['LOCATIONS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['LOCATIONS'] = sorted(menu['LOCATIONS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Locations loaded" % (TS, len(menu['LOCATIONS'])))

        sql = '''SELECT "O_GUID", "O_CATEGORY", "O_ORIGIN", "O_DESC", "O_CLASS2", "O_LOGSOURCE" FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'PIR' ORDER BY "O_CATEGORY"'''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            d['GUID']     = e[0]
            d['CATEGORY'] = e[1]
            d['ORIGIN']   = e[2]
            d['NAME']     = "%s %s" % (e[1], e[3])
            d['CLASS2']   = e[4]
            print(e)
            if str(e[5]) in uaa:
                menu['PIR'].append(d)
        menu['PIR'].append({'NAME' : '0', 'GUID' : 0})
        menu['PIR'] = sorted(menu['PIR'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d PIR loaded" % (TS, len(menu['PIR'])))

        sql = '''SELECT "O_GUID", "O_CATEGORY", "O_CLASS1", "O_DESC", "O_LOGSOURCE"  FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'STRAT' ORDER BY "O_DESC" '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            d['GUID']     = e[0]
            d['CATEGORY'] = e[1]
            d['CLASS1']   = e[2]
            d['NAME']     = "%s: %s" % (e[1], e[3])
            if e[4] in uaa:
                menu['STR'].append(d)
        menu['STR'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['STR'] = sorted(menu['STR'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d STRAT loaded" % (TS, len(menu['STR'])))

        sql = '''SELECT "P_FNAME", "P_LNAME", "P_GUID", "P_LOGSOURCE" FROM "POLER"."PERSON" '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            d['NAME'] = e[0] + ' ' + e[1]
            d['GUID'] = e[2]
            if e[3] in uaa:
                menu['PERSONS'].append(d)
        menu['PERSONS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['PERSONS'] = sorted(menu['PERSONS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Persons loaded" % (TS, len(menu['PERSONS'])))

        sql = '''SELECT "O_CATEGORY", "O_GUID", "O_CLASS1", "O_CLASS2", "O_CLASS3", "O_TYPE", "O_DESC", "O_LOGSOURCE"  FROM "POLER"."OBJECT" WHERE "O_TYPE" != 'User' ORDER BY "O_DESC"  '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            class1 = str(e[2])[:10]
            d['NAME'] = e[0] + ' ' + e[5]
            d['DESC'] = class1 + ' ' + e[3] + ' ' + e[4]
            d['GUID'] = e[1]
            if e[7] in uaa:
                menu['OBJECTS'].append(d)
        menu['OBJECTS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['OBJECTS'] = sorted(menu['OBJECTS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Objects loaded" % (TS, len(menu['OBJECTS'])))

        sql = '''SELECT "E_DTG", "E_GUID", "E_LOGSOURCE" FROM "POLER"."EVENT"  ORDER BY "E_DTG" DESC '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            d['NAME'] = e[0]
            d['GUID'] = e[1]
            if e[2] in uaa:
                menu['EVENTS'].append(d)
        menu['EVENTS'].append({'NAME' : '00000', 'GUID' : 0})
        menu['EVENTS'] = sorted(menu['EVENTS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Events loaded" % (TS, len(menu['EVENTS'])))

        sql = '''SELECT "E_GUID",  "E_CATEGORY", "E_DESC", "E_CLASS1", "E_DATE", "E_DTG", "E_ORIGIN", "E_XCOORD", "E_YCOORD", "E_LOGSOURCE" FROM "POLER"."EVENT" WHERE "E_CATEGORY" = 'Task'  '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            print(e)
            d = {}
            d['GUID']   = e[0]
            d['NAME']   = e[1]
            d['DESC']   = str(e[2]),
            d['CLASS1'] = e[3],
            d['DATE']   = e[4].strftime('%d %b %Y')
            d['DTG']    = int(e[5])
            d['STATUS'] = e[6]
            d['FROM']   = e[7]
            d['TO']     = e[8]
            if e[9] in uaa:
                menu['TASKS'].append(d)
        menu['TASKS'] = sorted(menu['TASKS'], key=lambda i: i['DTG'], reverse=True)
        menu['Tprofile'].append(d)
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Tasks loaded" % (TS, len(menu['TASKS'])))

        sql = ''' SELECT DISTINCT "TYPE" FROM "POLER"."RELATION" ORDER BY "TYPE" '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            d = {}
            d['RELTYP'] = e[0]
            menu['RELS'].append(d)
        menu['RELS'] = sorted(menu['RELS'], key=lambda i: i['RELTYP'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-menuFill]: %d Relation types loaded" % (TS, len(menu['RELS'])))

        return menu

    def get_task(self, GUID):

        if GUID != 'None':
            sql = '''SELECT "E_GUID",  "E_CATEGORY", "E_DESC", "E_CLASS1", "E_DATE", "E_DTG", "E_ORIGIN", "E_XCOORD", "E_YCOORD" FROM "POLER"."EVENT" WHERE "E_GUID" = '%d'  ''' % GUID

        else:
            sql = '''SELECT "E_GUID",  "E_CATEGORY", "E_DESC", "E_CLASS1", "E_DATE", "E_DTG", "E_ORIGIN" FROM "POLER"."EVENT" WHERE "E_CATEGORY" = 'Task'  '''

        r = self.cursor.execute(sql).fetchone()
        task = {'GUID'   : r[0],
                'NAME'   : r[1],
                'DESC'   : str(r[2]),
                'CLASS1' : r[3],
                'DATE'   : r[4].strftime('%d %b %Y'),
                'DTG'    : int(r[5]),
                'STATUS' : r[6]
                }
        if GUID == 'None':
            GUID = int(task['GUID'])

        sqlFROM   = '''SELECT "SOURCEGUID" FROM "POLER"."RELATION" WHERE "TARGETGUID" = '%d' AND "TYPE" = 'PUBLISHED_TASK'; ''' % GUID
        sqlTO = '''SELECT "TARGETGUID" FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%d' AND "TYPE" = 'TASKED_TO';''' % GUID
        FROM = int((self.cursor.execute(sqlFROM).fetchone())[0])
        TO = int((self.cursor.execute(sqlTO).fetchone())[0])
        sqlFROM = ''' SELECT "O_CLASS1" FROM "POLER"."OBJECT" WHERE "O_GUID" = '%d' ''' % FROM
        sqlTO = ''' SELECT "O_CLASS1" FROM "POLER"."OBJECT" WHERE "O_GUID" = '%d' ''' % TO
        FROM = (self.cursor.execute(sqlFROM).fetchone())[0]
        TO = (self.cursor.execute(sqlTO).fetchone())[0]


        task['FROM'] = FROM
        task['TO'] = TO

        return task

    def get_ta_runs(self):

        sql = ''' SELECT "E_GUID", "E_CLASS1", "E_DESC"  FROM "POLER"."EVENT" WHERE "E_CATEGORY" = 'TEXT_ANALYTICS'; '''
        qRun = self.cursor.execute(sql).fetchall()
        results = []
        for e in qRun:
            r = {}
            r['GUID'] = e[0]
            r['CATEGORY'] = e[1]
            r['DESC'] = str(e[2])
            if r not in results:
                results.append(r)

        return results

    def get_user_profile(self, GUID):

        User = {}
        sql = '''SELECT "O_CATEGORY", "O_CLASS1", "O_DESC", "O_LOGSOURCE", "O_GUID", "O_ORIGIN" FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % int(GUID)
        r = self.cursor.execute(sql).fetchone()
        User['ROLE'] = r[0]
        User['NAME'] = r[1]
        User['GUID'] = r[4]
        User['DESC'] = str(r[2])
        User['EMAIL'] = r[3]
        User['AUTH']  = r[5]
        User['DESC'] = 'Name: %s\nRole: %s\nAuthorization: %s\nEmail: %s\n%s' % (User['NAME'], User['ROLE'], User['AUTH'], User['EMAIL'], User['DESC'])

        User['ACTIVITIES'] = []
        User['TASKS']      = []
        sql = '''
            SELECT A2."E_CATEGORY", A2."E_DESC", A2."E_DATE", A2."E_DTG", A2."E_GUID", A2."E_CLASS1", A2."E_ORIGIN", A2."E_XCOORD", A2."E_YCOORD", A2."E_TYPE"
            FROM "POLER"."RELATION" AS A1
            LEFT OUTER JOIN "POLER"."EVENT" AS A2 ON A1."TARGETGUID" = A2."E_GUID"
            WHERE A1."SOURCEGUID" = '%d'; ''' % int(GUID)
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            data = {}
            data['CATEGORY'] = e[0]
            data['DESC']   = str(e[1])
            data['DATE']   = e[2]
            data['DTG']    = e[3]
            data['GUID']   = e[4]
            data['CLASS1'] = e[5]
            data['ORIGIN'] = e[6]
            data['XCOORD'] = e[7]
            data['YCOORD'] = e[8]
            if data['CATEGORY'] == 'Task':
                if data not in User['TASKS']:
                    User['TASKS'].append(data)
            else:
                if data not in User['ACTIVITIES']:
                    User['ACTIVITIES'].append(data)

        sql = '''
            SELECT A2."E_CATEGORY", A2."E_DESC", A2."E_DATE", A2."E_DTG", A2."E_GUID", A2."E_CLASS1", A2."E_ORIGIN", A2."E_XCOORD", A2."E_YCOORD", A2."E_TYPE"
            FROM "POLER"."RELATION" AS A1
            LEFT OUTER JOIN "POLER"."EVENT" AS A2 ON A1."SOURCEGUID" = A2."E_GUID"
            WHERE A1."TARGETGUID" = '%d'; ''' % int(GUID)
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            data = {}
            data['CATEGORY'] = e[0]
            data['DESC']   = str(e[1])
            data['DATE']   = e[2]
            data['DTG']    = e[3]
            data['GUID']   = e[4]
            data['CLASS1'] = e[5]
            data['ORIGIN'] = e[6]
            data['XCOORD'] = e[7]
            data['YCOORD'] = e[8]
            if data['CATEGORY'] == 'Task':
                if data not in User['TASKS']:
                    User['TASKS'].append(data)

        User['ACTIVITIES'] = sorted(User['ACTIVITIES'], key=lambda i: i['DTG'], reverse=True)
        User['TASKS'] = sorted(User['TASKS'], key=lambda i: i['DTG'], reverse=True)

        return User

    def delete_user(self, GUID):
        # Change the password/CLASS2 into CLASS3 and set the CLASS2 to a closed code which keeps the user for records but closes access to the app
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        CLOSED_CODE = TS + "-" + str(random.randint(1000000,9999999))
        sql = '''UPDATE "POLER"."OBJECT" SET "O_CLASS3" = "O_CLASS2" WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d'
        ''' % (int(GUID))
        self.cursor.execute(sql)
        sql = '''UPDATE "POLER"."OBJECT" SET "O_CLASS2" = 'Closed-%s' WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d'
        ''' % (CLOSED_CODE, int(GUID))
        self.cursor.execute(sql)
        sql = '''SELECT "O_DESC" FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d'
            ''' % (int(GUID))
        r = self.cursor.execute(sql).fetchone()
        DELETE_DESC = "%s\nDeleted: %s" % (r[0], CLOSED_CODE)
        print(DELETE_DESC)
        sql = '''UPDATE "POLER"."OBJECT" SET "O_DESC" = '%s' WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d'
            ''' % (DELETE_DESC, int(GUID))
        self.cursor.execute(sql)

        return CLOSED_CODE

    def get_entity(self, GUID, TYPE):

        result = {'VAL' : False, 'GUID' : GUID}

        if TYPE == 'Person':
            sql = ''' SELECT "P_GEN", "P_FNAME", "P_LNAME", "P_DOB", "P_POB", "P_GUID" FROM "POLER"."PERSON" WHERE "P_GUID" = '%d' ''' % GUID
            r = self.cursor.execute(sql).fetchone()

            try:
                result = {'GEN': r[0], 'FNAME': r[1], 'LNAME': r[2], 'DOB': r[3].strftime('%d %b %Y'), 'POB': r[4], 'GUID': r[5]}
                result['NAME'] = result['FNAME'] + ' ' + result['LNAME']
                result['DESC'] = "ID: %s\nGender: %s\n%s was born on %s in %s." % (GUID, result['GEN'], result['NAME'], result['DOB'], result['POB'])
                result['POLER'] = 'Person'
                result['TYPE']     = r[0]
                result['CATEGORY'] = r[0]
                result['CLASS1']   = r[1]
                result['CLASS2']   = r[2]
                result['DATE']     = str(r[3])
                result['ORIGIN']   = r[4]
                result['VAL']      = True
            except:
                result['VAL']      = None

        elif TYPE == 'Object':
            sql = ''' SELECT "O_TYPE", "O_CATEGORY", "O_DESC", "O_CLASS1", "O_CLASS2", "O_CLASS3", "O_GUID", "O_ORIGIN" FROM "POLER"."OBJECT" WHERE "O_GUID" = '%d' ''' % GUID
            r = self.cursor.execute(sql).fetchone()
            try:
                result = {'TYPE': r[0], 'CATEGORY': r[1], 'DESC': str(r[2]), 'CLASS1': r[3], 'CLASS2': r[4], 'CLASS3': r[5], 'GUID': r[6]}
                result['NAME'] = result['TYPE'] + ' ' + result['CATEGORY']
                result['DESC'] = "ID: %s\nObject with description: %s. Classifications %s , %s, %s." % (GUID, result['DESC'], result['CLASS1'], result['CLASS2'], result['CLASS3'])
                result['POLER'] = 'Object'
                result['TYPE']     = r[0]
                result['CATEGORY'] = r[1]
                result['CLASS1']   = r[3]
                result['CLASS2']   = r[4]
                result['DATE']     = str([5])
                result['ORIGIN']   = r[6]
                result['VAL']  = True
            except:
                result['VAL']      = None

        elif TYPE == 'Location':
            sql = ''' SELECT "L_TYPE", "L_DESC", "L_XCOORD", "L_YCOORD", "L_ZCOORD", "L_CLASS1", "L_GUID", "L_ORIGIN" FROM "POLER"."LOCATION" WHERE "L_GUID" = '%d' ''' % GUID
            r = self.cursor.execute(sql).fetchone()
            try:
                result = {'TYPE': r[0], 'DESC': str(r[1]), 'XCOORD': r[2], 'YCOORD': r[3], 'ZCOORD': r[4], 'CLASS1': r[5], 'GUID': r[6]}
                result['NAME'] = result['TYPE'] + ' ' + result['DESC']
                result['DESC'] = "Location at %s, %s with data %s and %s." % (result['XCOORD'], result['YCOORD'], result['ZCOORD'], result['CLASS1'])
                result['POLER'] = 'Location'
                result['TYPE']     = r[0]
                result['CATEGORY'] = r[1]
                result['CLASS1']   = r[2]
                result['CLASS2']   = r[3]
                result['DATE']     = str(r[5])
                result['ORIGIN']   = r[7]
                result['VAL']  = True
            except:
                result['VAL']      = None

        elif TYPE == 'Event':
            sql = ''' SELECT "E_TYPE", "E_CATEGORY", "E_DESC", "E_TIME", "E_DTG", "E_DATE", "E_GUID", "E_ORIGIN" FROM "POLER"."EVENT" WHERE "E_GUID" = '%d' ''' % GUID
            r = self.cursor.execute(sql).fetchone()
            try:
                result = {'TYPE': r[0], 'CATEGORY': r[1], 'DESC': str(r[2]), 'TIME': r[3].strftime('%H:%M'), 'DTG': r[4], 'DATE': r[5].strftime('%d %b %Y'), 'GUID': r[6]}
                result['NAME'] = result['TYPE'] + ' ' + result['CATEGORY']
                result['DESC'] = "Event on %s. %s" % (result['DATE'], result['DESC'])
                result['POLER'] = 'Event'
                result['TYPE']     = r[0]
                result['CATEGORY'] = r[1]
                result['CLASS1']   = str(r[3])
                result['CLASS2']   = r[4]
                result['DATE']     = str(r[5])
                result['ORIGIN']   = r[7]
                result['VAL']  = True
            except:
                result['VAL']      = None

        if result['VAL']:
            return result
        else:
            return None

    def get_entity_relations(self, GUID):

        relations = []
        pRelCount = 0
        oRelCount = 0
        lRelCount = 0
        eRelCount = 0

        sql = ''' SELECT DISTINCT "TARGETTYPE", "TARGETGUID", "TYPE" FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%d' ''' % GUID
        run = self.cursor.execute(sql).fetchall()
        for r in run:
            result = {'TYPE': r[0], 'GUID': r[1], 'REL': r[2]}
            relations.append(result)
            if r[0] == 'Person':
                pRelCount+=1
            if r[0] == 'Object':
                oRelCount+=1
            if r[0] == 'Location':
                lRelCount+=1
            if r[0] == 'Event':
                eRelCount+=1

        sql = ''' SELECT DISTINCT "SOURCETYPE", "SOURCEGUID", "TYPE" FROM "POLER"."RELATION" WHERE "TARGETGUID" = '%d' ''' % GUID
        run = self.cursor.execute(sql).fetchall()
        for r in run:
            result = {'TYPE': r[0], 'GUID': r[1], 'REL': r[2]}
            relations.append(result)
            if r[0] == 'Person':
                pRelCount+=1
            if r[0] == 'Object':
                oRelCount+=1
            if r[0] == 'Location':
                lRelCount+=1
            if r[0] == 'Event':
                eRelCount+=1

        return relations, pRelCount, oRelCount, lRelCount, eRelCount

    def todays_recent_intel(n):
        today = datetime.now().strftime("%F")
        query = "MATCH (user:User)-[:PUBLISHED_INTEL]->(a) RETURN user.username AS username, a.GUID AS idintel, a.ORIGIN AS date, user.GUID AS iduser, a.DESC AS description ORDER BY date DESC LIMIT %d" % (n)
        qRun = self.graph.run(query)
        results = []
        for e in qRun:
            data = {}
            data['username']    = e['username']
            data['idintel']     = e['idintel']
            data['date']        = e['date']
            data['iduser']      = e['iduser']
            data['description'] = e['description']
            results.append(data)

        return results

    def get_PIR(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_PIR]: started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        results = []
        sql = ''' SELECT "O_GUID", "O_CATEGORY", "O_ORIGIN", "O_DESC", "O_CLASS2" FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'PIR' ORDER BY "O_CATEGORY"  '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            p = {}
            p['GUID'] = e[0]
            p['CATEGORY'] = e[1]
            p['ORIGIN'] = e[2]
            p['DESC'] = e[3]
            p['CLASS2'] = e[4]
            results.append(p)

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_PIR]: completed with %d PIR." % (TS, len(results)))

        return results

    def get_STR(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_STR]: started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        results = []
        sql = ''' SELECT "O_GUID", "O_CATEGORY", "O_CLASS1", "O_DESC"  FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'STRAT' ORDER BY "O_DESC"  '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            p = {}
            p['GUID'] = e[0]
            p['CATEGORY'] = e[1]
            p['CLASS1'] = e[2]
            p['DESC'] = e[3]
            results.append(p)

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_STR]: completed with %d STR." % (TS, len(results)))

        return results

    def get_locations(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_locations]: started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        results = []
        sql = '''
        SELECT * FROM (SELECT "L_GUID", "L_ORIGIN", ROW_NUMBER()
        OVER(PARTITION BY "L_ORIGIN" ORDER BY "L_ORIGIN" DESC) rn FROM "POLER"."LOCATION") a
        WHERE rn = 1 ORDER BY "L_ORIGIN"
        '''
        r = self.cursor.execute(sql).fetchall()
        for e in r:
            p = {}
            p['GUID'] = e[0]
            p['LOCNAME'] = e[1]
            results.append(p)

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-get_locations]: completed with %d locations." % (TS, len(results)))

        return results

    def ConnectToHANA(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-ConnectToHANA]: process started on \n\tHost %s \n\tPort %s \n\tUser %s." % (TS, self.host, self.port, self.user))

        if self.connected == False:
            try:
                self.cursor    =  pyhdb.connect(self.host, self.port, self.user, self.password, self.autocommit).cursor()
                self.connected = True

            except Exception as e:
                if self.Verbose == True:
                    print('[!] Connection Error: %s' % str(e))

            try:
                self.curCondis =  pyhdb.connect(self.host, self.port, self.userCondis, self.pswdCondis, self.autocommit).cursor()
                if self.Verbose == True:
                    print('[*] Connecting to ConDis on \n\tHost %s \n\tPort %s \n\tUser %s' % (self.host, self.port, self.userCondis))

            except Exception as e:
                if self.Verbose == True:
                    print('[!] Connection Error: %s' % str(e))

    def ucdp_menu(self):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-ucdp_menu]: process started." % (TS))
        UCDP = pd.read_excel(self.BaseBook, sheetname="UCDP")
        results = []
        for index, row in UCDP.iterrows():
            results.append({'GUID' : row['code'], 'NAME' : row['country']})
        return results


    def preLoadLocationsThread(self):
        #TODO bubble up values for progress meters
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-preLoadLocationsThread]: process started." % (TS))

        Locations = pd.read_excel(self.BaseBook, sheetname= "Locations")
        entity = {'TYPE' : 'Location'}
        L_TYPE = 'City'
        for index, row in Locations.iterrows():
            L_DESC = row['city'].replace("'", " ")
            L_XCOORD = row['lat']
            L_YCOORD = row['lng']
            L_ZCOORD = 0
            L_CLASS1 = row['pop']
            L_ORIGIN = ('%s, %s' % (row['city'], row['country'])).replace("'", " ")
            L_ORIGINREF = '%s%s' % (row['lat'], row['lng'])
            L_LOGSOURCE = 'A1'

            self.insertLocation(L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)

    def preLoadPIRandSTRAT(self):
        ORIGIN = 'preLoadPIRandSTRAT'
        LOGSOURCE = 'A1'
        S1guid = self.insertObject('STRAT', 'Domestic Security', 'The Vulnerable People strategy is focussed on determining factors that put children and elderly citizens at risk.' , 'High', '0', '0', ORIGIN, "STRATTheVulnerablePeoplestrategyisfocussedondeterminingfactorsthatputchildrenandelderlycitizensatrisk%s" % ORIGIN, LOGSOURCE)
        S1P1guid = self.insertObject('PIR', 'Anti Social Behavior', 'What is the effect of online antisocial behavior in creating vulnerable people in this area?' , 'Medium', 'VP', '0', ORIGIN, "PIRWhatistheeffectofonlineantisocialbehaviorincreatingvulnerablepeopleinthisarea?%s" % ORIGIN, LOGSOURCE)
        S1P2guid = self.insertObject('PIR', 'Domestic Abuse', 'What are the common traits among residential areas that have the highest count of domestic abuse cases?', 'Medium', 'VP', '0', ORIGIN, 'PIRWhatarethecommontraitsamongresidentialareasthathavethehighestcountofdomesticabusecases?%s' % ORIGIN, LOGSOURCE)
        S1P3guid = self.insertObject('PIR', 'Violent Crime', 'What patterns exist between violent crime and the high count domestic abuse areas?', 'Medium', 'VP', '0', ORIGIN, 'PIRWhatpatternsexistbetweenviolentcrimeandthehighcountdomesticabuseareas?%s' % ORIGIN, LOGSOURCE)

        S2guid = self.insertObject('STRAT', 'National Security', 'The Ground Truth strategy is focussed on identifying propaganda and misinformation purposely distributed to disrupt social processes by fomenting conflict.' , 'High', '0', '0', ORIGIN, "STRATTheGroundTruthstrategyisfocussedonidentifyingpropagandaandmisinformationpurposelydistributedtodisruptsocialprocessesbyfomentingconflict%s" % ORIGIN, LOGSOURCE)
        S2P1guid = self.insertObject('PIR', 'Cyber', 'What are the main sources of the largest online media outlets and their stories?' , 'Medium', 'GT', '0', ORIGIN, "PIRWhatarethemainsourcesofthelargestonlinemediaoutletsandtheirstories?%s" % ORIGIN, LOGSOURCE)

        self.insertRelation(int(S1P1guid), 'Object', 'AnalysisToSupport', int(S1guid), 'Object')
        self.insertRelation(int(S1P2guid), 'Object', 'AnalysisToSupport', int(S1guid), 'Object')
        self.insertRelation(int(S1P3guid), 'Object', 'AnalysisToSupport', int(S1P2guid), 'Object')
        self.insertRelation(int(S2P1guid), 'Object', 'AnalysisToSupport', int(S2guid), 'Object')


    def preLoadLocations(self):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:

            print("[%s_HDB-preLoadLocations]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        if self.Verbose == True:
            print("[*] Checking Locations inventory...")
        t1 = Thread(target=self.preLoadLocationsThread, )
        t1.start()
        print("[%s_HDB-preLoadLocations]: thread started." % (TS))

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        return "Loading locations into model that don't already exist. Started at %s" % TS


    def getSocialData(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-getSocialData]: process started." % (TS))
        try:
            view = pd.read_csv(self.SocialPath, encoding='latin-1')
        except:
            view = pd.read_csv(self.SocialPath, encoding='utf_8')
        return view

    def preLoadPeopleThread(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-preLoadPeopleThread]: process started." % (TS))
        A1 = 'A1'
        A2 = 'A2'
        A3 = 'A3'
        B1 = 'B1'
        C1 = 'C1'
        AUTHS = [A1, B1, C1]
        People = pd.read_excel(self.BaseBook, sheetname= "People")
        entity = {'TYPE' : 'Person'}
        L_TYPE = 'Person'
        for index, row in People.iterrows():
            if '-' not in str(row['DOB']):
                DOB = '%s-%s-%s' % (str(row['DOB'])[:4], str(row['DOB'])[4:6], str(row['DOB'])[-2:])
            else:
                DOB = row['DOB']
            P_GEN      = row['Gender']
            P_FNAME    = row['FirstName']
            P_LNAME    = row['LastName']
            P_DOB      = DOB
            P_POB      = row['PlaceofBirth']
            XCOORD     = row['Lat']
            YCOORD     = row['Lon']
            P_ORIGIN   = row['Country']
            P_ORIGINREF = row['FullName']
            P_LOGSOURCE = random.choice(AUTHS)

            P_GUID = self.insertPerson(P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, P_LOGSOURCE)
            E_GUID = self.insertEvent('Birth', 'Human', '%s %s born on %s' % (P_FNAME, P_LNAME, P_DOB), 'eng', TS,
                                     '12:00:00', P_DOB, '%s120000' % P_DOB, XCOORD, YCOORD,  P_ORIGIN, P_ORIGINREF,  P_LOGSOURCE)
            L_GUID = self.insertLocation('City', P_POB, XCOORD, YCOORD, '0', TS, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE)
            self.insertRelation(P_GUID, 'Person', 'BORN_ON', E_GUID, 'Event')
            self.insertRelation(E_GUID, 'Event', 'OccurredAt', L_GUID, 'Location')
            self.insertRelation(P_GUID, 'Person', 'BORN_IN', L_GUID, 'Location')

        print("People complete")

    def preLoadPeople(self):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:

            print("[%s_HDB-preLoadPeople]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        if self.Verbose == True:
            print("[*] Checking public personalities...")
        t1 = Thread(target=self.preLoadPeopleThread, )
        t1.start()
        print("[%s_HDB-preLoadPeople]: thread started." % (TS))

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        return "Loading people into model that don't already exist. Started at %s" % TS

    def preLoadVPScene1(self):

        OR = ORIGIN = 'preLoadVPScene1' # OriginRef
        A1 = 'A1'
        A2 = 'A2'
        A3 = 'A3'
        B1 = 'B1'
        C1 = 'C1'
        DESC = 'Loaded from VPScene1'
        Person = 'Person'
        Event = 'Event'
        ChargedWith = 'ChargedWith'
        Family = 'Family'
        Knows = 'Knows'
        RecordedBy = 'RecordedBy'
        SubjectofContact = 'SubjectofContact'

        # Create the people
        People = ([
            {'P_FNAME' : 'Tim', 'P_LNAME' : 'Soshel'},
            {'P_FNAME' : 'Johnny', 'P_LNAME' : 'Rice'},
            {'P_FNAME' : 'Janney', 'P_LNAME' : 'Wheat'},
            {'P_FNAME' : 'Jimmy', 'P_LNAME' : 'Grain'},
            {'P_FNAME' : 'Chris', 'P_LNAME' : 'Sneeke'},
            {'P_FNAME' : 'Connie', 'P_LNAME' : 'Reeke'},
            {'P_FNAME' : 'Patty', 'P_LNAME' : 'Pooles'},
            {'P_FNAME' : 'June', 'P_LNAME' : 'Rice'},
            {'P_FNAME' : 'Eric', 'P_LNAME' : 'Spoon'},
            {'P_FNAME' : 'Ethel', 'P_LNAME' : 'Spoon'},
            {'P_FNAME' : 'Sid', 'P_LNAME' : 'Spoon'},
            {'P_FNAME' : 'Hakim', 'P_LNAME' : 'Abdul'}
        ])

        for p in People:

            sql = '''SELECT "P_GUID" FROM "POLER"."PERSON" WHERE "P_FNAME" = '%s' AND "P_LNAME" = '%s' ''' % (p['P_FNAME'], p['P_LNAME'])
            P = self.cursor.execute(sql).fetchone()
            if p['P_FNAME'] == 'Johnny':
                Johnny = P[0]
            elif p['P_FNAME'] == 'Janney':
                Janney = P[0]
            elif p['P_FNAME'] == 'Jimmy':
                Jimmy = P[0]
            elif p['P_FNAME'] == 'Chris':
                Chris = P[0]
            elif p['P_FNAME'] == 'Connie':
                Connie = P[0]
            elif p['P_FNAME'] == 'Tim':
                Tim = P[0]
            elif p['P_FNAME'] == 'Patty':
                Patty = P[0]
            elif p['P_FNAME'] == 'June':
                June = P[0]
            elif p['P_FNAME'] == 'Eric':
                Eric = P[0]
            elif p['P_FNAME'] == 'Ethel':
                Ethel = P[0]
            elif p['P_FNAME'] == 'Sid':
                Sid = P[0]
            elif p['P_FNAME'] == 'Hakim':
                Hakim = P[0]
        RawEvents = [
        ('Crime', 'Domestic Violence', 'Chris was charged with the crime by Patty.', C1),
        ('Crime', 'Drug Trafficking', 'June was charged with the crime by Patty.', C1),
        ('Crime', 'Child Abuse', 'Connie was charged with the crime.', C1),
        ('Social Services', 'Assessment', 'Tim conducted an assessment on Johnny.', B1),
        ('Crime', 'Child Abuse', 'Connie was charged with the crime.', C1),
        ('Crime', 'Drug Trafficking', 'June was charged with the crime by Patty.', C1),
        ('Social Services', 'Assessment', 'Tim conducted an assessment on Jimmy.', B1),
        ('Social Services', 'Assessment', 'Tim conducted an assessment on Eric.', B1),
        ('Health', 'Emergency call', 'Hakim treated Jimmy for bruises.', B1),
        ('Health', 'Emergency call', 'Hakim treated Eric for dementia induced injuries.', B1),
        ]
        xBox = [535133, 535857]
        yBox = [-15918, -13174]
        Events = []
        T = 0
        TS = datetime.fromtimestamp(time.time() + T).strftime('%Y-%m-%d %H:%M')
        for e in RawEvents:

            x = random.randint(xBox[0], xBox[1])/10000
            y = random.randint(yBox[0], yBox[1])/10000
            d = {'E_TYPE' : '%s' % e[0],
                 'E_CATEGORY' : '%s' % e[1],
                 'E_DESC' : '%s' % e[2],
                 'E_LANG' : 'en',
                 'E_CLASS1' : '0',
                 'E_TIME' : '%s' % TS[-5:],
                 'E_DATE' : '%s' % TS[:10],
                 'E_DTG' : '%s' % TS.replace(":", "").replace("-", "").replace(" ", ""),
                 'E_XCOORD' : '%d' % int(x),
                 'E_YCOORD' : '%d' % int(y),
                 'E_ORIGIN' : '%s' % ORIGIN,
                 'E_ORIGINREF' : ('%s%s%s' % (OR, TS, e[2])).replace(":", "").replace("-", "").replace(" ", ""),
                 'E_LOGSOURCE' : '%s' % e[3]}
            Events.append(d)
            T+=  random.randint(50000, 500000)
            TS = datetime.fromtimestamp(time.time()+T).strftime('%Y-%m-%d %H:%M')

        for e in Events:
            e['GUID'] = self.insertEvent(e['E_TYPE'], e['E_CATEGORY'], e['E_DESC'], e['E_LANG'], e['E_CLASS1'], e['E_TIME'], e['E_DATE'], e['E_DTG'], e['E_XCOORD'], e['E_YCOORD'], e['E_ORIGIN'], e['E_ORIGINREF'], e['E_LOGSOURCE'])
            if 'Connie' in e['E_DESC']:
                self.insertRelation(Connie, Person, ChargedWith, e['GUID'], Event)
            if 'Chris' in e['E_DESC']:
                self.insertRelation(Chris, Person, ChargedWith, e['GUID'], Event)
            if 'June' in e['E_DESC']:
                self.insertRelation(June, Person, ChargedWith, e['GUID'], Event)
            if 'Jimmy' in e['E_DESC']:
                self.insertRelation(Jimmy, Person, SubjectofContact, e['GUID'], Event)
            if 'Tim' in e['E_DESC']:
                self.insertRelation(Tim, Person, RecordedBy, e['GUID'], Event)
            if 'Johnny' in e['E_DESC']:
                self.insertRelation(Johnny, Person, SubjectofContact, e['GUID'], Event)
            if 'Patty' in e['E_DESC']:
                self.insertRelation(Patty, Person, RecordedBy, e['GUID'], Event)
            if 'Eric' in e['E_DESC']:
                self.insertRelation(Eric, Person, SubjectofContact, e['GUID'], Event)
            if 'Hakim' in e['E_DESC']:
                self.insertRelation(Hakim, Person, SubjectofContact, e['GUID'], Event)

        self.insertRelation(June, Person, Family, Johnny, Person)
        self.insertRelation(Tim, Person, Knows, June, Person)
        self.insertRelation(Patty, Person, Knows, Tim, Person)
        self.insertRelation(Janney, Person, Family, Tim, Person)
        self.insertRelation(Jimmy, Person, Knows, Tim, Person)
        self.insertRelation(Sid, Person, Family, Eric, Person)
        self.insertRelation(Sid, Person, Family, Ethel, Person)
        self.insertRelation(Ethel, Person, Family, Eric, Person)

    def preLoadObjects(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-preLoadObjects]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        if self.Verbose == True:
            print("[*] Checking preset objects...")
        Objects = pd.read_excel(self.BaseBook, sheetname= "seedObjects")

        for index, row in Objects.iterrows():
            O_TYPE      = row['TYPE']
            O_CATEGORY  = row['CATEGORY']
            O_DESC      = row['DESC']
            O_CLASS1    = row['CLASS1']
            O_CLASS2    = row['CLASS2']
            O_CLASS3    = row['CLASS3']
            O_ORIGIN    = row['ORIGIN']
            O_ORIGINREF = row['ORIGINREF']
            O_LOGSOURCE = self.ConDisSrc

            O_GUID = self.insertObject(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

    def findUser(self, username):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-findUser]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        statement = '''SELECT "O_GUID", "O_LOGSOURCE", "O_CLASS2", "O_CATEGORY", "O_ORIGIN" FROM "POLER"."OBJECT" WHERE "O_CLASS1" = '%s' ''' % username
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-findUser]: %s." % (TS, statement))

        r = self.cursor.execute(statement).fetchall()
        if len(r) == 0:
            user = None
        else:
            user = {}
            user['GUID'] = r[0][0]
            user['email'] = r[0][1]
            user['tel'] = 0
            user['location'] = r[0][4]
            user['password'] = r[0][2]
            user['utype'] = r[0][3]

        return user

    def tileStats(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-tileStats]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        tile_stats = {}
        DIR = {}
        CATEGORY = 'PIR'
        sql = '''SELECT COUNT(*) FROM "POLER"."OBJECT" WHERE "O_TYPE" = '%s' ''' % CATEGORY
        DIR['PIRcount'] = self.cursor.execute(sql).fetchall()[0][0]
        sql = '''SELECT COUNT(*) FROM "POLER"."OBJECT" WHERE "O_TYPE" = '%s' AND "O_CLASS2" = 'Imminent' ''' % CATEGORY
        DIR['Critical'] = self.cursor.execute(sql).fetchall()[0][0]
        sql = '''SELECT COUNT(*) FROM "POLER"."OBJECT" WHERE "O_TYPE" = '%s' AND "O_CLASS2" = 'High' ''' % CATEGORY
        DIR['High'] = self.cursor.execute(sql).fetchall()[0][0]
        sql = '''SELECT COUNT(*) FROM "POLER"."OBJECT" WHERE "O_TYPE" = '%s' AND "O_CLASS2" = 'Medium' ''' % CATEGORY
        DIR['Medium'] = self.cursor.execute(sql).fetchall()[0][0]
        DIR['Low'] = DIR['PIRcount'] - DIR['Critical'] - DIR['High'] - DIR['Medium']
        tile_stats['DIR'] = DIR

        DIT = {}
        CATEGORY = 'Task'
        sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            DIT['Taskcount'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            DIT['Taskcount'] = 0
        DIT['Outstanding'] = DIT['Taskcount'] / 3
        sql = '''SELECT "E_DTG" FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ORDER BY "E_DTG" DESC ''' % CATEGORY
        try:
            DIT['Last'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            DIT['Taskcount'] = 0
        tile_stats['DIT'] = DIT

        CO = {}
        CATEGORY = 'OSINTSearch'

        sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            CO['Searches'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            CO['Searches'] = 0
        CO['Channels'] = 3
        sql = '''SELECT "E_DTG" FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ORDER BY "E_DTG" DESC ''' % CATEGORY
        try:
            CO['Lastsearch'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            CO['Lastsearch'] = 0
        tile_stats['CO'] = CO

        CF = {}
        CATEGORY = 'PROCESSED_INTEL'
        sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            CF['Files'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            CF['Files'] = 0
        CF['Templates'] = 7
        sql = '''SELECT "E_DTG" FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ORDER BY "E_DTG" DESC ''' % CATEGORY
        try:
            CF['Lastupload'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            CF['Lastupload'] = 0
        tile_stats['CF'] = CF

        DET = {}
        sql = ''' SELECT COUNT(*) FROM "POLER"."EVENT" WHERE CONTAINS (("E_DESC"), 'EXTRACTION_CORE_VOICEOFCUSTOMER') '''
        try:
            DET['Sentiment'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            DET['Sentiment'] = 0
        sql = ''' SELECT COUNT(*) FROM "POLER"."EVENT" WHERE CONTAINS (("E_DESC"), 'EXTRACTION_CORE_PUBLIC_SECTOR') '''
        try:
            DET['POLE'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            DET['POLE'] = 0
        sql = ''' SELECT COUNT(*) FROM "POLER"."EVENT" WHERE CONTAINS (("E_DESC"), 'LINGANALYSIS_FULL') '''
        try:
            DET['Linguistic'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            DET['Linguistic'] = 0
        tile_stats['DET'] = DET

        AE = {}
        CATEGORY = 'PUBLISHED_INTEL'
        sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            AE['New'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            AE['New'] = 0
        sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            AE['Merged'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            sql = '''SELECT COUNT(*) FROM "POLER"."EVENT" WHERE "E_CATEGORY" = '%s' ''' % CATEGORY
        try:
            AE['Relations'] = self.cursor.execute(sql).fetchall()[0][0]
        except:
            AE['Relations'] = 0
        tile_stats['AE'] = AE
        VP = {}
        VP['Children'] = len(self.Graph_VP_CHILDREN(1,5))
        VP['Adults']   = len(self.Graph_VP_ADULTS())
        VP['Total']    = VP['Children'] + VP['Adults']
        tile_stats['VP'] = VP

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-tileStats]: %s." % (TS, tile_stats))

        if self.cursor == None:
            self.ConnectToHANA()

        return tile_stats


    def get_users(self):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-getUsers]: process started." % (TS))

        sql = '''SELECT "O_CLASS1", "O_GUID", "O_CATEGORY" FROM "POLER"."OBJECT" WHERE "O_TYPE" = 'User' '''
        r = self.cursor.execute(sql).fetchall()
        results = []
        for e in r:
            data = {}
            data['NAME']  = e[0]
            data['GUID']  = e[1]
            data['ROLE']  = e[2]
            results.append(data)

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-getUsers]: process completed with %d users." % (TS, len(results)))

        return results


    def EntityResolve(self, entity):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-EntityResolve]: process started with %s." % (TS, entity))

        entity['LOOKUP'] = bytes(entity['LOOKUP'], 'utf-8').decode('utf-8', 'ignore')
        entity['LOOKUP'] = entity['LOOKUP'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')

        if entity['TYPE'] == 'Event':
            sql = '''SELECT * FROM "POLER"."EVENT" WHERE CONTAINS ("E_ORIGINREF", '%s') ''' % entity['LOOKUP'] #EVENT120182873981EVENt39483014780MERGEDEVENT
            if self.Verbose == True:
                print("[%s_HDB-EntityResolve]: Event SQL\n %s." % (TS, sql))
            r = self.cursor.execute(sql).fetchall()
            if len(r) == 0:
                # No matches so get the last GUID of the event
                GUID = int(str('4' + str(time.time()).replace(".", "")))
                exists = 0
            else:
                GUID = int(r[0][0])
                exists = 1

        elif entity['TYPE'] == 'Location':
            sql = '''SELECT * FROM "POLER"."LOCATION" WHERE CONTAINS ("L_ORIGINREF", '%s') ''' % entity['LOOKUP']
            if self.Verbose == True:
                print("[%s_HDB-EntityResolve]: Location SQL\n %s." % (TS, sql))
            r = self.cursor.execute(sql).fetchall()
            if len(r) == 0:
                # No matches so get the last GUID of the event
                GUID = int(str('3' + str(time.time()).replace(".", "")))
                exists = 0
            else:
                GUID = int(r[0][0])
                exists = 1

        elif entity['TYPE'] == 'Person':
            sql = '''SELECT * FROM "POLER"."PERSON" WHERE CONTAINS ("P_ORIGINREF", '%s') ''' % entity['LOOKUP']
            if self.Verbose == True:
                print("[%s_HDB-EntityResolve]: Person SQL\n %s." % (TS, sql))
            r = self.cursor.execute(sql).fetchall()
            if len(r) == 0:
                # No matches so get the last GUID of the event
                GUID = int(str('1' + str(time.time()).replace(".", "")))
                exists = 0
            else:
                GUID = int(r[0][0])
                exists = 1

        elif entity['TYPE'] == 'Object':
            sql = '''SELECT * FROM "POLER"."OBJECT" WHERE CONTAINS ("O_ORIGINREF", '%s') ''' % entity['LOOKUP']
            if self.Verbose == True:
                print("[%s_HDB-EntityResolve]: Object SQL\n %s." % (TS, sql))
            r = self.cursor.execute(sql).fetchall()
            if len(r) == 0:
                # No matches so get the last GUID of the event
                GUID = int(str('2' + str(time.time()).replace(".", "")))
                exists = 0
            else:
                GUID = int(r[0][0])
                exists = 1
        else:
            GUID = int(str('7' + str(time.time()).replace(".", "")))
            exists = 0

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-EntityResolve]: Exists [%d] GUID-%s from sql %s." % (TS, exists, GUID, sql))

        return GUID, exists

    def check_date(self, E_DATE):

        datePatterns = ['%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y', '%Y-%d-%m', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%Y/%d/%m' ]
        for p in datePatterns:
            try:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                checkedE_DATE = datetime.strftime((datetime.strptime(E_DATE, p)), datePatterns[0])
                if self.Verbose == True:
                    print("[%s_HDB-check_date]: received pattern %s with %s and returned %s." % (TS, p, E_DATE, checkedE_DATE))
                return checkedE_DATE
            except:
                pass
        return datetime.strptime('2000-01-01', '%Y-%m-%d')

    def insertEvent(self, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE):

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-insertEvent]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        if len(E_LOGSOURCE) > 199:
            E_LOGSOURCE = E_LOGSOURCE[:200]
        E_DATE = self.check_date(E_DATE)
        if ':' not in str(E_TIME):
            E_TIME = '12:00'
        E_DESC = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
        E_DESC = '%s' % E_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        E_DESC = E_DESC[:5000]
        TA_ID = int(str('9' + str(time.time()).replace(".", "")))
        sql = '''INSERT INTO "POLER"."MASTER_TA" VALUES('%s', '%s') ''' % (TA_ID, E_DESC)
        self.cursor.execute(sql)

        if E_ORIGIN == None:
            E_ORIGIN = E_LOGSOURCE
        else:
            E_ORIGIN = '%s' % E_ORIGIN.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if E_CLASS1 != None and isinstance(E_CLASS1, str) == True:
            E_CLASS1 = (E_CLASS1.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', ''))[:200]

        E_ORIGINREF = ('%s%s%s%s%s%s' % (E_TYPE, E_CATEGORY, E_DESC, E_DTG, E_CLASS1, E_ORIGIN)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        E_ORIGINREF = (E_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        E_ORIGIN = E_ORIGIN[:200]

        if isinstance(E_XCOORD, int) == False or isinstance(E_XCOORD, float) == False:
            E_XCOORD = 0
        if isinstance(E_YCOORD, int) == False or isinstance(E_YCOORD, float) == False:
            E_YCOORD = 0

        E_GUID, exists = self.EntityResolve({'TYPE' : 'Event', 'LOOKUP' : '%s' % E_ORIGINREF})
        if exists == 0:
            statement = '''INSERT INTO "POLER"."EVENT" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s', 'EVENT') ''' % (E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'EVENT', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (E_GUID, E_TYPE, E_CATEGORY, E_DATE, str(E_DESC)[:2000], E_CLASS1, E_XCOORD, E_YCOORD)
            if self.Verbose == True:
                print('[*] Insert with:\n%s\n%s ' % (statement, statementB))

            self.cursor.execute(statement)
            self.cursor.execute(statementB)
            if isinstance(E_CLASS1, int) == False or isinstance(E_CLASS1, float) == False:
                E_CLASS1 = 0
            self.CondisIncident(E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

        return E_GUID

    def insertLocation(self, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE):

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        L_LOGSOURCE = str(L_LOGSOURCE)
        L_DESC = str(L_DESC).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if self.cursor == None:
            self.ConnectToHANA()
        if isinstance(L_ORIGIN, str):
            L_ORIGIN = L_ORIGIN.replace("'", '')
        try:
            L_XCOORD = float(L_XCOORD)
        except:
            L_XCOORD = 0.000
        try:
            L_YCOORD = float(L_YCOORD)
        except:
            L_YCOORD = 0.000
        if len(L_LOGSOURCE) > 199:
            L_LOGSOURCE = L_LOGSOURCE[:200]

        L_ORIGINREF = ('%s%s%s%s%s%s' % (L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1))
        L_ORIGINREF = (L_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        L_CLASS1 = 0

        L_GUID, exists = self.EntityResolve({'TYPE' : 'Location', 'LOOKUP' : '%s' % L_ORIGINREF })
        if exists == 0:
            statement = '''INSERT INTO "POLER"."LOCATION" VALUES('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', 'LOCATION') ''' % (L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
            statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'LOCATION', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (L_GUID, L_TYPE, L_TYPE, TS, str(L_DESC)[:2000], L_XCOORD, L_YCOORD, L_ZCOORD)
            if self.Verbose == True:
                print("[%s_HDB-insertLocation]: Insertion with\n%s\n%s" % (TS, statement, statementB))

            self.cursor.execute(statement)
            self.cursor.execute(statementB)
            self.CondisLocation(L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)

        return L_GUID

    def insertObject(self, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE):
        '''
            An Object is any virtual or physical item that can be described and associated with a person, location or event.
            The Type could be HairColor, Religion, SocialMediaAccount, CommunicationDevice, Weapon...
            Coorespondng category Brown, Atheist, Twitter, MobilePhone, Hand-Gun
            Cooresponding desc N/A, Doesn't believe in God, Username: Tweeter1 established on 5.05.05 associated with..., Phone Number: ...., SN/ 444 registered to on ...
            Cooresponding Class1 N/A, N/A, Tweeter1, 555-5555, Glock9
            Cooresponding Class2 N/A, N/A, ID-394949, SN-393910, SN-444
        '''
        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-insertObject]: process started." % (TS))
        O_CLASS1 = (str(O_CLASS1).replace("'", ""))[:200]
        O_CLASS2 = (str(O_CLASS2).replace("'", ""))[:200]
        O_CLASS3 = (str(O_CLASS3).replace("'", ""))[:200]

        if self.cursor == None:
            self.ConnectToHANA()

        if len(O_LOGSOURCE) > 199:
            O_LOGSOURCE = O_LOGSOURCE[:200]

        if O_CATEGORY != None:
            O_CATEGORY = (O_CATEGORY[:60]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        else:
            O_CATEGORY = 'Unknown'
        O_ORIGINREF = ('%s%s%s%s%s%s' % (O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        O_ORIGINREF = (O_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')

        if O_DESC != None:
            O_DESC = ('%s' % O_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', ''))[:5000]
        else:
            O_DESC = O_LOGSOURCE
        O_GUID, exists = self.EntityResolve({'TYPE' : 'Object', 'LOOKUP' : '%s' % O_ORIGINREF})
        if exists == 0:
            statement = '''INSERT INTO "POLER"."OBJECT" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 'OBJECT') ''' % (O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'OBJECT', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (O_GUID, O_TYPE, O_CATEGORY[:40], TS, str(O_DESC)[:2000], O_CLASS1, O_CLASS2, O_CLASS3)
            print('[*] Insert Object:\n%s' % statement)

            self.cursor.execute(statement)
            self.cursor.execute(statementB)
            self.CondisObject(O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

        return O_GUID

    def insertODBPerson(self, P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.cursor == None:
            self.ConnectToHANA()

        statement = '''INSERT INTO "POLER"."PERSON" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 'PERSON') ''' % (P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE)
        statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'PERSON', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (P_GUID, P_GEN, P_POB, P_DOB, P_ORIGINREF, P_FNAME, P_LNAME, P_ORIGIN)
        try:
            self.cursor.execute(statement)
        except Exception as e:
            print("[%s_HDB-insertODBPerson]: ERROR: %s\n%s." % (TS, e, statement))
        try:
            self.cursor.execute(statementB)
        except Exception as e:
            print("[%s_HDB-insertODBPerson]: ERROR: %s\n%s." % (TS, e, statementB))
        self.CondisPerson(P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE)

    def insertODBObject(self, O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE):

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        statement = '''INSERT INTO "POLER"."OBJECT" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 'OBJECT') ''' % (O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
        statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'OBJECT', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (O_GUID, O_TYPE, O_CATEGORY[:40], TS, str(O_DESC)[:2000], O_CLASS1, O_CLASS2, O_CLASS3)

        try:
            self.cursor.execute(statement)
        except Exception as e:
            print("[%s_HDB-insertODBObject]: ERROR: %s\n%s." % (TS, e, statement))
        try:
            self.cursor.execute(statementB)
        except Exception as e:
            print("[%s_HDB-insertODBObject]: ERROR: %s\n%s." % (TS, e, statementB))
        self.CondisObject(O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)


    def insertODBEvent(self, E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE):

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        E_DESC = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
        E_DESC = '%s' % E_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        E_DESC = E_DESC[:5000]
        TA_ID = int(str('9' + str(time.time()).replace(".", "")))
        sql = '''INSERT INTO "POLER"."MASTER_TA" VALUES('%s', '%s') ''' % (TA_ID, E_DESC)
        self.cursor.execute(sql)

        statement = '''INSERT INTO "POLER"."EVENT" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s', 'EVENT') ''' % (E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
        statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'EVENT', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (E_GUID, E_TYPE, E_CATEGORY, E_DATE, str(E_DESC)[:2000], E_CLASS1, E_XCOORD, E_YCOORD)

        try:
            self.cursor.execute(statement)
        except Exception as e:
            print("[%s_HDB-insertODBEvent]: ERROR: %s\n%s." % (TS, e, statement))

        try:
            self.cursor.execute(statementB)
        except Exception as e:
            print("[%s_HDB-insertODBEvent]: ERROR: %s\n%s." % (TS, e, statementB))


        if isinstance(E_CLASS1, int) == False or isinstance(E_CLASS1, float) == False:
            E_CLASS1 = 0
        self.CondisIncident(E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

    def insertODBLocation(self, L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE):

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        statement = '''INSERT INTO "POLER"."LOCATION" VALUES('%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s', '%s', 'LOCATION') ''' % (L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
        statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'LOCATION', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (L_GUID, L_TYPE, L_TYPE, TS, str(L_DESC)[:2000], L_XCOORD, L_YCOORD, L_ZCOORD)

        self.cursor.execute(statement)
        self.cursor.execute(statementB)
        self.CondisLocation(L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)

    def LoadSimData(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-LoadSimData]: started." % (TS))

        bb = load_workbook(filename=self.BaseBook, read_only=False)
        self.simLocations  = bb['Locs']
        self.simNames      = bb['Names']
        self.simAttributes = bb['Attributes']
        self.simObjects    = bb['Objects']
        self.simTelco      = bb['Telco']

        # Establish on the fly needs
        Any = ['Albania', 'France', 'Italy', 'Afghanistan']
        Nord = ['Norway', 'Estonia', 'Finland', 'Latvia', 'Lithuania', 'Sweden', 'Denmark', 'Netherlands']
        Eeuro = ['Albania', 'Bosnia']
        Weuro = ['France', 'Germany', 'Italy', 'Belgium', 'Spain']
        Eslavic = ['Poland', 'Russia', 'Hungary']
        Wslavic = ['Serbia', 'Croatia']
        Arabic = ['Afghanistan']
        Easia = ['China']
        self.simOrigins = [Any, Arabic, Easia, Eeuro, Eslavic, Nord, Weuro, Wslavic]

        self.simdata = True

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-LoadSimData]: complete." % (TS))

    def namegen(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-namegen]: starting." % (TS))

        if self.simdata == False:
            self.LoadSimData()
        SurNames = []
        GiveNames = []
        Country = 'France'
        gen = random.choice(['M','F'])

        for origin in self.simOrigins:  # Match the country to the cooresponding orign region
            if Country in origin:
                i = random.randint(0,7)
                if i == 0:
                    origIndex = "Any"
                if i == 1:
                    origIndex = "Arabic"
                if i == 2:
                    origIndex = "Easia"
                if i == 3:
                    origIndex = "Eeuro"
                if i == 4:
                    origIndex = "Eslavic"
                if i == 5:
                    origIndex = "Nord"
                if i == 6:
                    origIndex = "Weuro"
                if i == 7:
                    origIndex = "Wslavic"

        i=1 # Filter out a list of names appropriate to the gender and origin
        for name in self.simNames['C:C']:

            if self.simNames['C%s' % str(i)].value == origIndex or self.simNames['C%s' % str(i)].value == "Any":
                if self.simNames['B%s' % str(i)].value == gen:
                    GiveNames.append(self.simNames['A%s' % str(i)].value)
                if self.simNames['B%s' % str(i)].value == 'L':
                    SurNames.append(self.simNames['A%s' % str(i)].value)
            i+=1

        # Catch in case no names chosen
        if len(GiveNames) < 1 or len(SurNames) < 1:
            GiveNames.append("John")
            SurNames.append("Doe")

        FNAME = random.choice(GiveNames)
        LNAME = random.choice(SurNames)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_OG-namegen]: returning %s %s." % (TS, FNAME, LNAME))

        return FNAME, LNAME

    def insertPerson(self, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-insertPerson]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        if len(DESC) == 0 or DESC == None:
            DESC = 'Record created on %s' % TS
        if P_FNAME == 'Unk' or len(P_FNAME) < 2:
            P_FNAME = lname = "Unknown"
        if P_LNAME == 'Unk' or len(P_LNAME) < 2:
            fname = P_LNAME = "Unknown"
        P_DOB = str(self.check_date(P_DOB))[:10]

        P_FNAME = (P_FNAME.replace("'", ""))[:60]
        P_LNAME = (P_LNAME.replace("'", ""))[:60]
        P_GEN = P_GEN.strip()
        P_ORIGINREF = ("%s%s%s%s%s" % (P_FNAME, P_LNAME, P_GEN, P_DOB, P_POB)).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace("-", "")[:2000]
        P_GUID, exists = self.EntityResolve({'TYPE' : 'Person', 'LOOKUP' : '%s' % P_ORIGINREF})
        if exists == 0:
            statement = '''INSERT INTO "POLER"."PERSON" VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', 'PERSON') ''' % (P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE)
            statementB = '''INSERT INTO "POLER"."MASTER_NODE" VALUES('%s', 'PERSON', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (P_GUID, P_GEN, P_POB, P_DOB, P_ORIGINREF, P_FNAME, P_LNAME, P_ORIGIN)
            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("[%s_HDB-insertPerson]: Person insert statement:\n%s\n%s." % (TS, statement, statementB))
            self.cursor.execute(statement)
            self.cursor.execute(statementB)

            self.CondisPerson(P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE)

        return P_GUID

    def insertRelation(self, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print('''[%s_HDB-insertRelation]: process started with
                    SOURCEGUID:%s, SOURCETYPE:%s, TYPE:%s, TARGETGUID:%s, TARGETTYPE:%s.'''
                  % (TS, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE))

        if self.cursor == None:
            self.ConnectToHANA()

        if SOURCEGUID == None or TARGETGUID == None:
            if self.Verbose == True:
                print("[%s_HDB-insertRelation]: completed due to None type in Source or Target GUID." % (TS))
            return False

        SOURCEGUID = int(SOURCEGUID)
        TARGETGUID = int(TARGETGUID)
        sql = '''
        SELECT * FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%s' AND "TARGETGUID" = '%s' AND "TYPE" = '%s';
        ''' % (SOURCEGUID, TARGETGUID, TYPE)
        if len(self.cursor.execute(sql).fetchall()) < 1:
            R_GUID = int(str('8' + str(time.time()).replace(".", "")))

            statement = '''INSERT INTO "POLER"."RELATION" VALUES('%s', '%s', '%s', '%s', '%s', '%s')''' % (R_GUID, TYPE, SOURCETYPE, TARGETTYPE, SOURCEGUID,  TARGETGUID)
            if self.Verbose == True:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print('[%s_HDB-insertRelation] Insert Relation:\n%s' % (TS, statement))

            self.cursor.execute(statement)
            self.CondisRelation(R_GUID, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE)

    def insertUser(self, username, password, email, tel, location, image, utype):

        User = 'User'
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-insertUser]: started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        O_DESC = 'Username %s of type %s created on %s can be reached at %s' % (username, utype, TS, email)
        O_ORIGINREF = username + email
        O_GUID = self.insertObject(User, utype, O_DESC, username, password, tel, location, O_ORIGINREF, email)
        PGUID = self.insertPerson('U', User, username, TS, location, O_GUID, O_ORIGINREF, 'A1', O_DESC)
        self.insertRelation(PGUID, 'Person', 'AccountCreation', O_GUID, 'Object')
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-insertUser]: Created Associated HANA GUID: Person %s." % (TS, O_GUID))

        return O_GUID

    def insertODBUser(self, O_GUID, PGUID, username, password, email, tel, location, image, utype):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        User = 'User'
        O_ORIGINREF = None
        O_DESC = 'Username %s of type %s created on %s can be reached at %s' % (username, utype, TS, email)
        self.insertODBObject(O_GUID, User, utype, O_DESC, username, password, tel, location, O_ORIGINREF, email)
        self.insertODBPerson(PGUID, 'U', User, username, TS, location, O_GUID, O_ORIGINREF, 'A1', O_DESC)
        self.insertRelation(PGUID, 'Person', 'AccountCreation', O_GUID, 'Object')

        return O_GUID

    def merge_ORGREF_BlockChain(self, TYPE, GUID1, GUID2, aORIGINREF, bORIGINREF, aTable, bTable, aguid, bguid):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-merge_ORGREF_BlockChain]: process started with %s %s %s %s %s %s %s %s %s" % (TS, TYPE, GUID1, GUID2, aORIGINREF, bORIGINREF, aTable, bTable, aguid, bguid))
        # Get the ORIGINREF of the disolving entity and append to the end of the absorbing entity
        statement = ''' SELECT "%s" FROM "POLER"."%s" WHERE "%s" = '%s' ''' % (bORIGINREF, bTable, bguid, GUID2)
        bORIGINREFval = self.cursor.execute(statement).fetchone()[0]
        print("%s %s" % (bORIGINREFval, statement))
        statement = ''' SELECT "%s" FROM "POLER"."%s" WHERE "%s" = '%s' ''' % (aORIGINREF, aTable, aguid, GUID1)
        aORIGINREFval = self.cursor.execute(statement).fetchone()[0]
        print("%s %s" % (aORIGINREFval, statement))
        aORIGINREFval = "%s-%s" % (aORIGINREFval, bORIGINREFval)
        statement = ''' UPDATE "POLER"."%s" SET "%s" = '%s' WHERE "%s" = '%s' ''' % (aTable, aORIGINREF, aORIGINREFval, aguid, GUID1)
        print('[%s_HDB-merge_ORGREF_BlockChain] %s' % (TS, statement))
        self.cursor.execute(statement)
        print("%s" % (statement))
        return "[%s_HDB-merge_ORGREF_BlockChain] %s" % (TS, aORIGINREFval)

    def merge_entities(self, TYPE, GUID1, GUID2):

        guids = [GUID1, GUID2]
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-merge_entities]: process started." % (TS))

        if str(GUID2)[0] == '1':
            GUID2type  = 'Person'
            bTable     = 'PERSON'
            bguid      = 'P_GUID'
            bORIGINREF = 'P_ORIGINREF'
        elif str(GUID2)[0] == '2':
            GUID2type  = 'Object'
            bTable     = 'OBJECT'
            bguid      = 'O_GUID'
            bORIGINREF = 'O_ORIGINREF'
        elif str(GUID2)[0] == '3':
            GUID2type  = 'Location'
            bTable     = 'LOCATION'
            bguid      = 'L_GUID'
            bORIGINREF = 'L_ORIGINREF'
        elif str(GUID2)[0] == '4':
            GUID2type  = 'Event'
            bTable     = 'EVENT'
            bguid      = 'E_GUID'
            bORIGINREF = 'E_ORIGINREF'

        if str(GUID1)[0] == '1':
            GUID1type  = 'Person'
            aTable     = 'PERSON'
            aguid      = 'P_GUID'
            aORIGINREF = 'P_ORIGINREF'
        elif str(GUID1)[0] == '2':
            GUID1type  = 'Object'
            aTable     = 'OBJECT'
            aguid      = 'O_GUID'
            aORIGINREF = 'O_ORIGINREF'
        elif str(GUID1)[0] == '3':
            GUID1type  = 'Location'
            aTable     = 'LOCATION'
            aguid      = 'L_GUID'
            aORIGINREF = 'L_ORIGINREF'
        elif str(GUID1)[0] == '4':
            GUID1type  = 'Event'
            aTable     = 'EVENT'
            aguid      = 'E_GUID'
            aORIGINREF = 'E_ORIGINREF'

        self.merge_ORGREF_BlockChain(TYPE, GUID1, GUID2, aORIGINREF, bORIGINREF, aTable, bTable, aguid, bguid)

        if self.curCondis == None:
            self.ConnectToHANA()

        # Define the tables for ease of referencing
        MR = '"CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION"'
        MN = '"CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_NODES"'
        ET = '"CONDIS_SCHEMA"."com.sap.condis::content.%s"' % GUID2type.upper()
        PR = '"POLER"."RELATION"'
        PM = '"POLER"."MASTER_NODE"'
        PE = '"POLER"."%s"' % TYPE.upper()

        statement = ''' SELECT "TYPE", "TARGETGUID", "TARGETTYPE" FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%s'  ''' % GUID2
        entities = self.cursor.execute(statement).fetchall()
        relcount = 0
        if len(entities) < 1:
            entities = self.cursor.execute(statement).fetchall()
        for e in entities:
            self.insertRelation(GUID1, GUID1type, e[0], e[1], e[2])
            relcount+=1
        statement = ''' SELECT "TYPE", "SOURCEGUID", "SOURCETYPE" FROM "POLER"."RELATION" WHERE "TARGETGUID" = '%s'  ''' % GUID2
        entites = self.cursor.execute(statement).fetchall()
        if len(entities) < 1:
            entities = self.cursor.execute(statement).fetchall()
        for e in entities:
            self.insertRelation(e[1], e[2], e[0], GUID1, GUID1type)
            relcount+=1

        statement = "DELETE FROM %s WHERE SOURCE_ENTITY_GUID = '%s'" % (MR, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.curCondis.execute(statement)
        statement = "DELETE FROM %s WHERE TARGET_ENTITY_GUID = '%s'" % (MR, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.curCondis.execute(statement)
        statement = "DELETE FROM %s WHERE ENTITY_GUID = '%s'" % (MN, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.curCondis.execute(statement)
        statement = "DELETE FROM %s WHERE ENTITY_GUID = '%s'" % (ET, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.curCondis.execute(statement)

        statement = "DELETE FROM %s WHERE 'SOURCEGUID' = '%s'" % (PR, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.cursor.execute(statement)
        statement = "DELETE FROM %s WHERE 'TARGETGUID' = '%s'" % (PR, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.cursor.execute(statement)
        statement = '''DELETE FROM %s WHERE "%s" = '%s' ''' % (PE, bguid, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.cursor.execute(statement)
        statement = '''DELETE FROM %s WHERE "ENTITY_GUID" = '%s' ''' % (PM, GUID2)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print('[%s_HDB-merge_entities] %s' % (TS, statement))
        self.cursor.execute(statement)

        return '%s merged into %s with %d new relationships.' % (GUID2, GUID1, relcount)

    def TextAnalytics(self, TA_CONFIG, text, TA_RUN, ODB):
        '''
        TA_CONFIG : VOC
        0 : ID, 1 : TA_RULE, 2 : TA_COUNTER, 3 : TA_TOKEN, 4 : TA_LANGUAGE, 5 : TA_TYPE,
        6 : TA_TYPE_EXPANDED, 7 : TA_NORMALIZED, 8 : TA_STEM, 9 : TA_PARAGRAPH, 10 : TA_SENTENCE
        11 : TA_CREATED_AT, 12 : TA_OFFSET, 13 : TA_PARENT

        Return a list of dictionaries (simple table) where each dictionary is a row from a HANA TA_INDEX
        '''
        # Set up the Objects for terms to be extracted
        CATEGORY = TA_CONFIG
        CLASS1 = 0
        CLASS2 = 0
        CLASS3 = 0
        ORIGIN = 'HANA_POLER_INDEX'
        ORIGINREF = 'COIN'
        LOGSOURCE = 'B1'
        # Default Person values
        GEN = 'U'
        LNAME = 'Doe'
        DOB = '2000-01-01'
        POB = 'Unknown'
        DESC = 'Person extracted from TA'
        NA = 'NA'

        if self.cursor == None:
            self.ConnectToHANA()

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-TextAnalytics]: process started." % (TS))
        TA_ID = int(str('9' + str(time.time()).replace(".", "")))

        text = text.replace("'", "").replace('"', '')
        statement = '''INSERT INTO "POLER"."MASTER_TA" VALUES('%s', '%s') ''' % (TA_ID, text)
        try:
            self.cursor.execute(statement)
        except:
            TA_ID = int(str('9' + str(time.time()).replace(".", "")))
            statement = '''INSERT INTO "POLER"."MASTER_TA" VALUES('%s', '%s') ''' % (TA_ID, text)
            self.cursor.execute(statement)

        print("[%s_HDB-TextAnalytics]: pausing for text processing." % (TS))
        time.sleep(3)
        statement = '''SELECT * FROM "POLER"."$TA_RTT" WHERE "ENTITY_GUID" = '%d' ''' % (TA_ID)
        TA_TABLE = self.cursor.execute(statement).fetchall()
        if len(TA_TABLE) < 1:
            TA_TABLE = self.cursor.execute(statement).fetchall()
        View = []
        for t in TA_TABLE:
            Node = {}
            Node['ID']           = t[0]
            Node['TA_RULE']      = t[1]
            Node['TA_COUNTER']   = t[2]
            Node['TA_TOKEN']     = t[3]
            Node['TA_LANGUAGE']  = t[4]
            Node['TA_TYPE']      = t[5]
            Node['TA_PARAGRAPH'] = t[9]
            Node['TA_SENTENCE']  = t[10]
            Node['TA_OFFSET']    = t[12]
            Node['GUID']         = 0
            View.append(Node)

        # set up iterators to determine where TA results should have relations
        curSent = 1
        curPara = 1
        curCount = 1
        prePerson = preTopic = preLocation = curPerson = curTopic = curLocation = 0
        newSent = newPara =  False

        print("[%s_HDB-TextAnalytics]: TA complete with %d rows." % (TS, len(View)))
        for Node in View:
            ORIGINREF = '%s%s%s' % (Node['TA_TYPE'], Node['TA_TOKEN'], "TA_COIN")
            if Node['TA_SENTENCE'] != curSent:
                curSent+=1
                prePerson = preTopic = preLocation = curPerson = curTopic = curLocation = 0
                newSent = True
            else:
                newSent = False

            if Node['TA_PARAGRAPH'] != curPara:
                curPara+=1
                prePerson = preTopic = preLocation = curPerson = curTopic = curLocation = 0
                newPara = True
            else:
                newPara = False
            if (Node['TA_TYPE'] == 'Topic' or
                Node['TA_TYPE'] == 'NOUN_GROUP' or
                Node['TA_TYPE'] == 'Occupation' or
                Node['TA_TYPE'] == 'Artifact'):

                if ODB != None:
                    curTopic = Node['GUID'] = ODB.insertObject(Node['TA_TYPE'], CATEGORY, '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, None, LOGSOURCE)
                else:
                    curTopic = Node['GUID'] = self.insertObject(Node['TA_TYPE'], CATEGORY, '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)


                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                    curTopic = Node['GUID']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != Node['GUID']:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')

                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                preTopic = Node['TA_COUNTER']
                TA_RUN['Object'].append({'GUID' : str(Node['GUID']), 'DESC' : Node['TA_TOKEN'], 'CATEGORY' : Node['TA_TYPE'], 'TYPE' : 'Object', 'NAME' : '%s %s' % (Node['TA_TYPE'], Node['TA_TOKEN'])})

                if ODB != None:
                    ODB.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')
                else:
                    self.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')

            elif Node['TA_TYPE'] == 'PERSON':
                if ODB != None:
                    Node['GUID'] = ODB.insertPerson(GEN, Node['TA_TOKEN'], LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE, DESC)
                else:
                    Node['GUID'] = self.insertPerson(GEN, Node['TA_TOKEN'], LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE, DESC)
                if prePerson == 0:
                    prePerson = Node['TA_COUNTER']
                    curPerson = Node['GUID']
                elif Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curPerson != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Person', 'TA_REFERENCE_SAME_SENTENCE', curPerson, 'Person')
                    else:
                        self.insertRelation(Node['GUID'], 'Person', 'TA_REFERENCE_SAME_SENTENCE', curPerson, 'Person')
                if preTopic != 0:
                    if Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Person', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Person', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')

                prePerson = Node['TA_COUNTER']
                TA_RUN['Person'].append({'GUID' : str(Node['GUID']), 'DESC' : Node['TA_TOKEN'], 'CATEGORY' : Node['TA_TYPE'], 'TYPE' : 'Person', 'NAME' : Node['TA_TOKEN']})
                if ODB != None:
                    ODB.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Person')
                else:
                    self.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Person')

            elif Node['TA_TYPE'] == 'LOCALITY' or Node['TA_TYPE'] == 'COUNTRY' or Node['TA_TYPE'] == 'CONTINENT' or Node['TA_TYPE'] == 'GEO_AREA':
                if ODB != None:
                    Node['GUID'] = ODB.insertLocation(Node['TA_TYPE'], Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertLocation(Node['TA_TYPE'], Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
                if preLocation == 0:
                    preLocation = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preLocation < 5 and newSent == False and curPerson != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Location', 'TA_REFERENCE_SAME_SENTENCE', curPerson, 'Person')
                    else:
                        self.insertRelation(Node['GUID'], 'Location', 'TA_REFERENCE_SAME_SENTENCE', curPerson, 'Person')
                if preTopic != 0:
                    if Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Location', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Location', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                preLocation = Node['TA_COUNTER']
                TA_RUN['Location'].append({'GUID' : str(Node['GUID']), 'DESC' : Node['TA_TOKEN'], 'CATEGORY' : Node['TA_TYPE'], 'TYPE' : 'Location', 'NAME' : '%s %s' % (Node['TA_TYPE'], Node['TA_TOKEN'])})
                if ODB != None:
                    ODB.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Location')
                else:
                    self.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Location')

            elif 'COMMON_WEAPON/' in Node['TA_TYPE']:
                if ODB != None:
                    Node['GUID'] = ODB.insertObject('Weapon', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertObject('Weapon', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                preTopic = Node['TA_COUNTER']
                TA_RUN['Object'].append({'GUID' : str(Node['GUID']), 'DESC' : Node['TA_TOKEN'], 'CATEGORY' : Node['TA_TYPE'], 'TYPE' : 'Object', 'NAME' : '%s %s' % (Node['TA_TYPE'], Node['TA_TOKEN'])})
                if ODB != None:
                    ODB.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')
                else:
                    self.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')

            elif 'VEHICLE/' in Node['TA_TYPE']:
                if ODB != None:
                    Node['GUID'] = ODB.insertObject('Vehicle', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertObject('Vehicle', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')

            elif 'ORGANIZATION/COMMERCIAL' in Node['TA_TYPE']:
                if ODB != None:
                    Node['GUID'] = ODB.insertObject('Commercial Organization', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertObject('Commercial Organization', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')

            elif 'ORGANIZATION/MEDIA' in Node['TA_TYPE']:
                if ODB != None:
                    Node['GUID'] = ODB.insertObject('Media Organization', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertObject('Media Organization', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')


            elif 'FACILITY/BUILDINGGROUNDS' in Node['TA_TYPE']:
                if ODB != None:
                    Node['GUID'] = ODB.insertObject('Facility', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertObject('Facility', '%s' % Node['TA_TYPE'], '%s' % Node['TA_TOKEN'], CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                if preTopic == 0:
                    preTopic = Node['TA_COUNTER']
                elif Node['TA_COUNTER'] - preTopic < 5 and newSent == False and curTopic != 0:
                    if ODB != None:
                        ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                    else:
                        self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                if prePerson != 0:
                    if Node['TA_COUNTER'] - prePerson < 5 and newSent == False and curTopic != 0:
                        if ODB != None:
                            ODB.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')
                        else:
                            self.insertRelation(Node['GUID'], 'Object', 'TA_REFERENCE_SAME_SENTENCE', curTopic, 'Object')



                preTopic = Node['TA_COUNTER']
                TA_RUN['Object'].append({'GUID' : str(Node['GUID']), 'DESC' : Node['TA_TOKEN'], 'CATEGORY' : Node['TA_TYPE'], 'TYPE' : 'Object', 'NAME' : '%s %s' % (Node['TA_TYPE'], Node['TA_TOKEN'])})
                if ODB != None:
                    ODB.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')
                else:
                    self.insertRelation(TA_RUN['GUID'], 'Event', 'TEXT_ANALYTICS', Node['GUID'], 'Object')

            if 'Action_' in Node['TA_TYPE']:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                DTG = TS.replace(":", "").replace("-", "").replace(" ", "")
                E_TIME = '12:00'
                E_DATE = '1900-01-01'
                E_CLASS1 = 'NA'
                E_ORIGINREF = 'TAREF%s%s' % (Node['TA_TYPE'], Node['TA_TOKEN'])
                if ODB != None:
                    Node['GUID'] = ODB.insertEvent('Event', 'From_TA', Node['TA_TOKEN'], Node['TA_LANGUAGE'], E_CLASS1, E_TIME, E_DATE, DTG, 0.0, 0.0, ORIGIN, E_ORIGINREF, LOGSOURCE)
                else:
                    Node['GUID'] = self.insertEvent('Event', 'From_TA', Node['TA_TOKEN'], Node['TA_LANGUAGE'], E_CLASS1, E_TIME, E_DATE, DTG, 0.0, 0.0, ORIGIN, E_ORIGINREF, LOGSOURCE)

            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-TextAnalytics]: Row %d complete." % (TS, curCount))
            curCount+=1

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-TextAnalytics]: Extraction complete." % (TS))

        return View, TA_RUN


    def ConDisRelTypeCode(self, TYPE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-ConDisRelTypeCode]: Mapping %s:" % (TS, TYPE))
        rmap = None
        if TYPE == 'AccountCreated' or TYPE == 'AccountCreation':
            rmap = 'AC1'
        elif TYPE == 'AnalysisToSupport':
            rmap = 'AN1'
        elif TYPE == 'BORN_ON':
            rmap = 'BO1'
        elif TYPE == 'BORN_IN':
            rmap = 'BO2'
        elif TYPE == 'ChargedWith':
            rmap = 'CH1'
        elif TYPE == 'CollectionToSupport':
            rmap = 'CO1'
        elif TYPE == 'CommittedCrime':
            rmap = 'CO2'
        elif TYPE == 'CreatedAt':
            rmap = 'CR1'
        elif TYPE == 'CreatedBy':
            rmap = 'CR2'
        elif TYPE == 'CreatedOn':
            rmap = 'CR3'
        elif TYPE == 'DocumentIn':
            rmap = 'DO1'
        elif TYPE == 'DocumentMentioning':
            rmap = 'DO2'
        elif TYPE == 'DocumentedBy':
            rmap = 'DO3'
        elif TYPE == 'Family':
            rmap = 'FM1'
        elif TYPE == 'Follows':
            rmap = 'FO1'
        elif TYPE == 'FOUND':
            rmap = 'FO2'
        elif TYPE == 'FROM_FILE':
            rmap = 'FF1'
        elif TYPE == 'HasAttribute':
            rmap = 'HA1'
        elif TYPE == 'HasStatus':
            rmap = 'HA2'
        elif TYPE == 'INCLUDES_TAG':
            rmap = 'IN1'
        elif TYPE == 'INVOLVES' or TYPE == 'Involved' or TYPE == 'InvolvedIn':
            rmap = 'IN2'
        elif TYPE == 'KNOWS' or TYPE == 'Knows':
            rmap = 'KN1'
        elif TYPE == 'LivesAt':
            rmap = 'LI1'
        elif TYPE == 'LocatedAt':
            rmap = 'LO1'
        elif TYPE == 'ModifiedBy':
            rmap = 'MO1'
        elif TYPE == 'ModifiedOn':
            rmap = 'MO2'
        elif TYPE == 'OfType':
            rmap = 'OF1'
        elif TYPE == 'ON':
            rmap = 'ON1'
        elif TYPE == 'OccurredAt':
            rmap = 'OC1'
        elif TYPE == 'Owns':
            rmap = 'OW1'
        elif TYPE == 'PartOf':
            rmap = 'PA1'
        elif TYPE == 'PROCESSED_INTEL':
            rmap = 'PR1'
        elif TYPE == 'PUBLISHED':
            rmap = 'PU1'
        elif TYPE == 'PUBLISHED_INTEL':
            rmap = 'PU2'
        elif TYPE == 'PUBLISHED_TASK':
            rmap = 'PU3'
        elif TYPE == 'REPORTED_AT':
            rmap = 'RE1'
        elif TYPE == 'RegisteredOn':
            rmap = 'RE2'
        elif TYPE == 'ReferenceLink':
            rmap = 'RE3'
        elif TYPE == 'RecordedBy':
            rmap = 'RE4'
        elif TYPE == 'SEARCHED':
            rmap = 'SE1'
        elif TYPE == 'SubjectofContact':
            rmap = 'SU1'
        elif TYPE == 'Supporting':
            rmap = 'SU2'
        elif TYPE == 'TAGGED':
            rmap = 'TA1'
        elif TYPE == 'TASKED_TO':
            rmap = 'TA2'
        elif TYPE == 'TA_REFERENCE':
            rmap = 'TA3'
        elif TYPE == 'TEXT_ANALYTICS':
            rmap = 'TA4'
        elif TYPE == 'TA_REFERENCE_SAME_SENTENCE':
            rmap = 'TA5'
        elif TYPE == 'TweetLocation':
            rmap = 'TW1'
        elif TYPE == 'Tweeted':
            rmap = 'TW2'
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-ConDisRelTypeCode]: Mapped %s:" % (TS, rmap))
        return rmap


    def CondisRelation(self, R_GUID, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-CondisRelation]: process started." % (TS))
        if self.curCondis == None:
            self.ConnectToHANA()

        e = {}
        table = '"CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION"'
        e['REL_GUID']              = R_GUID
        e['SOURCE_ENTITY_GUID']    = SOURCEGUID
        e['TARGET_ENTITY_GUID']    = TARGETGUID
        e['LOGICAL_SOURCE_SYSTEM'] = self.ConDisSrc
        e['ORIGIN']                = self.ConDisSrc
        e['RELATIONSHIP_ID']       = R_GUID
        e['RELATIONSHIP_DESC']     = TYPE
        e['RELATIONSHIP_TYPECODE'] = self.ConDisRelTypeCode(TYPE)

        statement = '''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (table, R_GUID,
                                                                                                    e['SOURCE_ENTITY_GUID'],
                                                                                                    e['TARGET_ENTITY_GUID'],
                                                                                                    e['RELATIONSHIP_TYPECODE'],
                                                                                                    e['LOGICAL_SOURCE_SYSTEM'],
                                                                                                    e['ORIGIN'],
                                                                                                    e['RELATIONSHIP_DESC'])

        self.curCondis.execute(statement)
        if self.Verbose == True:
            print("[%s_HDB-CondisRelation]: Insert ConDis relations: %s" % (TS, statement))


    def CondisPerson(self, P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-CondisPerson]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()

        if len(str(P_DOB)) == 8:
            dob = str(P_DOB)
            now = datetime.strftime(datetime.now(), self.timeformat)
            Age = round(((datetime.strptime(now, self.timeformat) - datetime.strptime(dob, self.timeformat)).days / 365),2)

        else:
            Age = 0.00

        e = {}
        table = '"CONDIS_SCHEMA"."com.sap.condis::content.PERSON"'
        e['Entity_GUID']      = P_GUID
        e['FirstName']        = P_FNAME[:40]
        e['LastName']         = P_LNAME[:40]
        e['DOB']              = str((P_DOB).replace("-", "").replace(" ", "").replace(":", ""))[:8]
        e['Age']              = Age
        e['complexion']       = 'OTH'
        e['ethnicity']        = 'OTH'
        e['height']           = 0.00
        e['Gender']           = P_GEN.strip()
        e['CountryOfbirth']   = 'CountryOfbirth'
        e['Country']          = 'CTY'
        e['FamilyName']       = 'FamilyName'
        e['FullName']         = P_ORIGINREF
        e['BirthName']        = 'BirthName'
        e['Othernames']       = 'Othernames'
        e['PlaceofBirth']     = P_POB
        e['Nationality']      = 'NAT'
        e['SecondNationality'] = 'NAT'
        e['AdditionalRemarks'] = 'AdditionalRemarks'
        e['ENTITY_ID']             = P_GUID
        e['ENTITY_TYPE']           = 'Person'
        e['ENTITY_LATITUDE']       = 0.00
        e['ENTITY_LONGITUDE']      = 0.00
        e['ENTITY_TIME']           = '1900-01-01 12:00:00'
        e['ORIGIN']                = self.ConDisSrc
        e['EXTERNAL_REFERENCE_ID'] = self.ConDisSrc
        e['LOGICAL_SOURCE_SYSTEM'] = self.ConDisSrc

        statement = ('''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')'''
                     % (table, e['Entity_GUID'], e['FirstName'], e['LastName'], e['DOB'], e['Age'], e['complexion'], e['ethnicity'], e['height'],  e['Gender'], e['CountryOfbirth'], e['Country'],
                        e['FamilyName'], e['FullName'], e['BirthName'], e['Othernames'], e['PlaceofBirth'], e['Nationality'], e['SecondNationality'], e['AdditionalRemarks']
                        ))

        if self.Verbose == True:
            print('[*] Insert ConDis person:\n%s' % statement)
        self.CondisNode(P_GUID, P_GUID, e['ENTITY_TYPE'], P_FNAME, e['ENTITY_LATITUDE'] , e['ENTITY_LONGITUDE'] , e['ENTITY_TIME'], e['ORIGIN'], e['EXTERNAL_REFERENCE_ID'], e['LOGICAL_SOURCE_SYSTEM'], P_LOGSOURCE)
        try:
            self.curCondis.execute(statement)
        except Exception as e:
            print("[%s_HDB-CondisPerson]: ERROR: %s." % (TS, e))

    def CondisObject(self, O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-CondisObject]: process started." % (TS))

        if self.curCondis == None:
            self.ConnectToHANA()

        e = {}
        table               = '"CONDIS_SCHEMA"."com.sap.condis::content.OBJECT"'
        e['Entity_GUID']         = O_GUID
        e['Objectdescription']   = O_DESC[:12].replace("'", "").replace('"', '')
        e['Objectcategory']      = O_CATEGORY[:12]
        e['ObjectFamily']        = O_TYPE[:4]
        e['MainCategory']        = ' '
        e['EmployeeResponsible'] = ' '
        e['Missing']             = ' '


        e['ENTITY_ID']           = O_GUID
        e['ENTITY_TYPE']         = 'Object'
        e['ENTITY_TIME']          = '2017-01-01 12:00:00'
        e['ORIGIN']           = self.ConDisSrc
        e['EXTERNAL_REFERENCE_ID'] = self.ConDisSrc
        e['LOGICAL_SOURCE_SYSTEM'] = self.ConDisSrc
        e['ENTITY_LATITUDE']  = 0.00
        e['ENTITY_LONGITUDE'] = 0.00

        statement = '''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (table, e['Entity_GUID'], e['Objectdescription'], e['Objectcategory'],
                                                                                            e['ObjectFamily'], e['MainCategory'], e['EmployeeResponsible'], e['Missing'])
        if self.Verbose == True:
            print('[*] Insert ConDis object:\n%s' % statement)
        try:
            self.curCondis.execute(statement)
        except Exception as error:
            print("[%s_HDB-CondisPerson]: ERROR: %s." % (TS, error))
        self.CondisNode(O_GUID, O_GUID, e['ENTITY_TYPE'],  e['Objectdescription'], e['ENTITY_LATITUDE'], e['ENTITY_LONGITUDE'], e['ENTITY_TIME'], e['ORIGIN'] , O_ORIGINREF, e['LOGICAL_SOURCE_SYSTEM'], O_LOGSOURCE)

    def CondisLocation(self, L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_HDB-CondisLocation]: process started." % (TS))

        if self.curCondis == None:
            self.ConnectToHANA()

        e = {}
        table = '"CONDIS_SCHEMA"."com.sap.condis::content.LOCATION"'
        e['Entity_GUID']          = L_GUID
        e['Longitude']            = L_YCOORD
        e['Latitude']             = L_XCOORD
        e['LocationCategory']     = L_TYPE[:12]
        e['Locationdesc']         = L_DESC[:12].replace("'", "").replace('"', '')
        e['House_Number']         = 'HouseNo'
        e['Street']               = 'Street'
        e['City']                 = 'City'
        e['PostalCode']           = 'Postal'
        e['Country']              = ' ' #3 letter
        e['Region']               = ' '
        e['LocationType']         = ' '

        e['ENTITY_ID']            = L_GUID
        e['ENTITY_TYPE']          = 'Location'
        e['ENTITY_TIME']          = '2017-01-01 12:00:00'
        e['ORIGIN']           = self.ConDisSrc
        e['EXTERNAL_REFERENCE_ID'] = self.ConDisSrc
        e['LOGICAL_SOURCE_SYSTEM'] = self.ConDisSrc

        statement = '''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (table, e['Entity_GUID'],  e['Longitude'], e['Latitude'], e['LocationCategory'],
                                                                                                                            e['Locationdesc'], e['House_Number'], e['Street'], e['City'], e['PostalCode'],
                                                                                                                            e['Country'], e['Region'], e['LocationType'])

        if self.Verbose == True:
            print('[*] Insert ConDis location:\n%s' % statement)

        self.curCondis.execute(statement)
        self.CondisNode(L_GUID, L_GUID, e['ENTITY_TYPE'], e['Locationdesc'] , L_XCOORD, L_YCOORD, e['ENTITY_TIME'], L_ORIGIN, L_ORIGINREF, e['LOGICAL_SOURCE_SYSTEM'], L_LOGSOURCE)

    def CondisIncident(self, E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE):

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-CondisIncident]: process started." % (TS))

        if self.curCondis == None:
            self.ConnectToHANA()

        if isinstance(E_TIME, str) == False:
            E_TIME = str(E_TIME.isoformat())
        if isinstance(E_DATE, str) == False:
            E_DATE = str(E_DATE.isoformat()[:10])
        if isinstance(E_DTG, str) == False:
            E_DTG = str(E_DTG)

        e = {}
        table = '"CONDIS_SCHEMA"."com.sap.condis::content.INCIDENT"'
        e['Entity_GUID']          = E_GUID
        e['Incidentdesc']         = E_DESC[:40]
        e['IncidentType']         = E_CATEGORY[:4]
        e['Startdate']            = E_DTG[:8]
        e['Enddate']              = E_DTG[:8]
        e['ReportedBy']           = E_ORIGIN
        e['Location']             = 'Location'
        e['IncidentStartTime']    = E_TIME.replace(":", "")[:6]
        e['IncidentEndTime']      = E_TIME.replace(":", "")[:6]

        e['ENTITY_ID']            = E_GUID
        e['ENTITY_TYPE']          = 'Incident'
        e['ENTITY_TIME']          = '%s %s' % (E_DATE, E_TIME)
        e['ORIGIN']               = self.ConDisSrc
        e['EXTERNAL_REFERENCE_ID'] = self.ConDisSrc
        e['LOGICAL_SOURCE_SYSTEM'] = self.ConDisSrc
        e['Incidentdesc'].replace("'", "")

        statement = '''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') ''' % (table, e['Entity_GUID'], e['Incidentdesc'], e['IncidentType'],
                                                                                                        e['Startdate'], e['Enddate'], e['ReportedBy'], e['Location'],
                                                                                                        e['IncidentStartTime'],  e['IncidentEndTime'])
        if self.Verbose == True:
            print('[*] Insert ConDis incident:\n%s' % statement)

        self.curCondis.execute(statement)
        self.CondisNode(E_GUID, E_GUID, e['ENTITY_TYPE'], e['Incidentdesc'], E_XCOORD, E_YCOORD, e['ENTITY_TIME'], E_ORIGIN, E_ORIGINREF, e['LOGICAL_SOURCE_SYSTEM'], E_LOGSOURCE)

    def CondisNode(self, ENTITY_GUID, ENTITY_ID, ENTITY_TYPE, ENTITY_DESC, ENTITY_LATITUDE, ENTITY_LONGITUDE, ENTITY_TIME, ORIGIN, EXTERNAL_REFERENCE_ID, LOGICAL_SOURCE_SYSTEM, LOGLABEL):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-CondisNode]: process started." % (TS))

        if self.curCondis == None:
            self.ConnectToHANA()

        if isinstance(ENTITY_LATITUDE, float) == False:
            ENTITY_LATITUDE = 0.0
        if isinstance(ENTITY_LONGITUDE, float) == False:
            ENTITY_LONGITUDE = 0.0

        ENTITY_DESC = ENTITY_DESC.replace("'", "").replace('"', '')
        table = '"CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_NODES"'
        statement = '''INSERT INTO %s VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')''' % (table, ENTITY_GUID, ENTITY_ID, ENTITY_TYPE, ENTITY_DESC,
                                                                                                                ENTITY_LATITUDE, ENTITY_LONGITUDE, ENTITY_TIME,
                                                                                                                self.ConDisSrc, self.ConDisSrc, self.ConDisSrc)
        print(statement)
        try:
            self.curCondis.execute(statement)
        except Exception as e:
            print("[%s_HDB-CondisNode]: ERROR: %s." % (TS, e))


        if self.Verbose == True:
            print('[*] Insert ConDis master node:\n%s' % statement)


    def Graph_ShortestPath(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-Graph_ShortestPath]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        # Clear the calculation table if it exists
        try:
            sql = '''DROP CALCULATION SCENARIO "SPathOneToOne" CASCADE;'''
            self.cursor.execute(sql)
        except:
            pass

        sql = '''
        CREATE CALCULATION SCENARIO "SPathOneToOne" USING '
         <?xml version="1.0"?>
         <cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
          <calculationScenario name="SPathOneToOne">
           <calculationViews>
                <graph name="shortest_path_node" defaultViewFlag="true" schema="POLER" workspace="GRAPH" action="GET_SHORTEST_PATH_ONE_TO_ONE">
                        <expression>
                                <![CDATA[{
                                        "parameters": {
                                                "startVertex": "%d",
                                                "targetVertex": "%d"
                                        }
                                }]]>
                        </expression>
                        <viewAttributes>
                                <viewAttribute name="ORDERING" datatype="Fixed" length="18" sqltype="BIGINT"/>
                                <viewAttribute name="SOURCEGUID" datatype="string"/>
                                <viewAttribute name="TARGETGUID" datatype="string"/>
                        </viewAttributes>
                </graph>
           </calculationViews>
          </calculationScenario>
         </cubeSchema>
         '
         WITH PARAMETERS ('EXPOSE_NODE'=('shortest_path_node','SPathOneToOne'));

        SELECT * FROM "SPathOneToOne" ORDER BY "ORDERING";
        ''' % (startNode, endNode)

        return results


    def Graph_VP_ADULTS(self):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-Graph_VP_ADULTS]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        # Clear the calculation table if it exists
        try:
            sql = '''DROP CALCULATION SCENARIO "MATCH_VP_ADULTS" CASCADE;'''
            self.cursor.execute(sql)
        except Exception as e:
            print('ERROR IN CREATION OF WORKSPACE: %s' % str(e))

        # Create the Graph Calculation Scenario based on the user input
        sql = '''
        CREATE CALCULATION SCENARIO "MATCH_VP_ADULTS" USING '
        <?xml version="1.0"?>
        <cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
         <calculationScenario name="MATCH_VP_ADULTS">
          <calculationViews>
               <graph name="match_subgraphs_node" defaultViewFlag="true" schema="POLER" workspace="GRAPH" action="MATCH_SUBGRAPHS">
                       <expression>
                               <![CDATA[
                                       MATCH p = (A)-[*1..2]-(B)
                                       WHERE A.POLER_CLASS = ''PERSON''
                                       AND B.POLER_CLASS = ''OBJECT''
                                       AND B.ENTITY_TYPE = ''RiskScore''
                                       RETURN A.CLASS_1 AS FNAME, A.CLASS_2 AS LNAME, A.ENTITY_GUID AS P_GUID, A.ENTITY_DATE AS DOB,
                                       B.DESCRIPTION AS RiskScore, B.ENTITY_GUID AS O_GUID
                               ]]>
                       </expression>
                       <viewAttributes>
                               <viewAttribute name="FNAME" datatype="string"/>
                               <viewAttribute name="LNAME" datatype="string"/>
                               <viewAttribute name="DOB" datatype="date"/>
                               <viewAttribute name="P_GUID" datatype="string"/>
                               <viewAttribute name="RiskScore" datatype="string"/>
                               <viewAttribute name="O_GUID" datatype="string"/>
                       </viewAttributes>
               </graph>
          </calculationViews>
         </calculationScenario>
        </cubeSchema>
        '
        WITH PARAMETERS ('EXPOSE_NODE'=('match_subgraphs_node','MATCH_VP_ADULTS'));
        '''
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(str(e))
        try:
            sql = ''' SELECT DISTINCT * FROM "MATCH_VP_ADULTS" ORDER BY "P_GUID"; '''
            Q = self.cursor.execute(sql).fetchall()

        except Exception as e:
            e = str(e)
            print('ERROR IN CREATION OF WORKSPACE: %s' % e)
            if 'Inconsistent data: Found [' in e:
                # Full message example: Inconsistent data: Found [0] as source or target key, which are no valid vertex keys.
                self.Graph_Correct(e)
            try:
                self.cursor.execute(sql)
            except Exception as e:
                print(str(e))
                return None

        sql = ''' SELECT DISTINCT * FROM "MATCH_VP_ADULTS" ORDER BY "P_GUID"; '''
        Q = self.cursor.execute(sql).fetchall()
        results = []
        firstrn = True
        for e in Q:
            # Compare the age of the person to today and if less than 18 years, consider them a Child
            r = {}
            r['GUID']     = e[3]
            if firstrn == True:
                r['FNAME']    = e[0]
                r['LNAME']    = e[1]
                r['NAME']     = "%s %s" % (e[0], e[1])
                r['DOB']      = e[2]
                r['RISKS']    = []
                r['VPSCORE']  = int(e[4])
                R = {}
                R['DESC']     = "Risk score %s" % e[4]
                R['O_GUID']   = e[5]
                if R not in r['RISKS']:
                    r['RISKS'].append(R)

                results.append(r)
                firstrn = False
            # Check if the person exists
            else:
                found = False
                i = 0
                for d in results:
                    if d['GUID'] == r['GUID']:
                        found = True
                        R = {}
                        R['DESC']     = "Risk score %s" % e[4]
                        R['O_GUID']   = e[5]
                        results[i]['RISKS'].append(R)
                        results[i]['VPSCORE'] = results[i]['VPSCORE'] + int(e[4])
                        break
                    else:
                        i+=1

                if found == False:

                    r['FNAME']    = e[0]
                    r['LNAME']    = e[1]
                    r['NAME']     = "%s %s" % (e[0], e[1])
                    r['RISKS']    = []
                    R = {}
                    R['DESC']     = e[3]
                    R['O_GUID']   = e[5]
                    r['RISKS'].append(R)
                    r['VPSCORE']  = int(e[4])
                    results.append(r)

        results = sorted(results, key=lambda i: i['VPSCORE'], reverse=False)
        return results

    def Graph_Correct(self, e):

        GUIDlistStart = e.find('Found [')+7
        GUIDlistEnd = e.find(']', GUIDlistStart)
        GUIDs = e[GUIDlistStart:GUIDlistEnd]
        if ',' in GUIDs:
            sGUIDs = GUIDs.split(',') # create a list from the sting of comma separated values
            print("For each , get the number and use it to delete those GUIDs from Relationship tables in POLER and ConDis")
            for GUID in sGUIDs:
                GUID = int(GUID.strip())
                nsql = ''' DELETE FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%d'; ''' % GUID
                self.cursor.execute(nsql)
                nsql = ''' DELETE FROM "POLER"."RELATION" WHERE "TARGETGUID" = '%d'; ''' % GUID
                self.cursor.execute(nsql)
                nsql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION" WHERE "SOURCE_ENTITY_GUID" = '%d'; ''' % GUID
                self.curCondis.execute(nsql)
                nsql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION" WHERE "TARGET_ENTITY_GUID" = '%d'; ''' % GUID
                self.curCondis.execute(nsql)
        else:
            GUID = int(GUIDs.strip())
            nsql = ''' DELETE FROM "POLER"."RELATION" WHERE "SOURCEGUID" = '%d'; ''' % GUID
            self.cursor.execute(nsql)
            nsql = ''' DELETE FROM "POLER"."RELATION" WHERE "TARGETGUID" = '%d'; ''' % GUID
            self.cursor.execute(nsql)
            nsql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION" WHERE "SOURCE_ENTITY_GUID" = '%d'; ''' % GUID
            self.curCondis.execute(nsql)
            nsql = ''' DELETE FROM "CONDIS_SCHEMA"."com.sap.condis::condis.MASTER_RELATION" WHERE "TARGET_ENTITY_GUID" = '%d'; ''' % GUID
            self.curCondis.execute(nsql)

    def Graph_VP_CHILDREN(self, staPath, endPath):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-Graph_VP_CHILDREN]: process started." % (TS))

        if self.cursor == None:
            self.ConnectToHANA()
        # Clear the calculation table if it exists
        try:
            sql = '''DROP CALCULATION SCENARIO "MATCH_VP_CHILDREN" CASCADE;'''
            self.cursor.execute(sql)
        except Exception as e:
            print('ERROR IN CREATION OF WORKSPACE: %s' % str(e))

        # Create the Graph Calculation Scenario based on the user input
        sql = '''
        CREATE CALCULATION SCENARIO "MATCH_VP_CHILDREN" USING '
        <?xml version="1.0"?>
        <cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
         <calculationScenario name="MATCH_VP_CHILDREN">
          <calculationViews>
               <graph name="match_subgraphs_node" defaultViewFlag="true" schema="POLER" workspace="GRAPH" action="MATCH_SUBGRAPHS">
                       <expression>
                               <![CDATA[
                                       MATCH p = (A)-[*%d..%d]-(B)
                                       WHERE A.POLER_CLASS = ''PERSON''
                                       AND B.POLER_CLASS = ''EVENT''
                                       AND B.ENTITY_TYPE = ''Crime''
                                       RETURN A.CLASS_1 AS FNAME, A.CLASS_2 AS LNAME, A.ENTITY_GUID AS P_GUID, A.ENTITY_DATE AS DOB,
                                       B.DESCRIPTION AS EVENT, B.ENTITY_CATEGORY AS CATEGORY, B.ENTITY_TYPE AS TYPE,
                                       B.ENTITY_DATE AS DATE, B.ENTITY_GUID AS E_GUID
                               ]]>
                       </expression>
                       <viewAttributes>
                               <viewAttribute name="FNAME" datatype="string"/>
                               <viewAttribute name="LNAME" datatype="string"/>
                               <viewAttribute name="DOB" datatype="date"/>
                               <viewAttribute name="P_GUID" datatype="string"/>
                               <viewAttribute name="EVENT" datatype="string"/>
                               <viewAttribute name="TYPE" datatype="string"/>
                               <viewAttribute name="CATEGORY" datatype="string"/>
                               <viewAttribute name="DATE" datatype="date"/>
                               <viewAttribute name="E_GUID" datatype="string"/>
                       </viewAttributes>
               </graph>
          </calculationViews>
         </calculationScenario>
        </cubeSchema>
        '
        WITH PARAMETERS ('EXPOSE_NODE'=('match_subgraphs_node','MATCH_VP_CHILDREN'));
        ''' % (staPath, endPath)
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(str(e))
        try:
            sql = ''' SELECT DISTINCT * FROM "MATCH_VP_CHILDREN" ORDER BY "P_GUID"; '''
            Q = self.cursor.execute(sql).fetchall()

        except Exception as e:
            e = str(e)
            print('ERROR IN CREATION OF WORKSPACE: %s' % e)
            if 'Inconsistent data: Found [' in e:
                # Full message example: Inconsistent data: Found [0] as source or target key, which are no valid vertex keys.
                self.Graph_Correct(e)
            try:
                self.cursor.execute(sql)
            except Exception as e:
                print(str(e))
                return None

        sql = ''' SELECT DISTINCT * FROM "MATCH_VP_CHILDREN" ORDER BY "P_GUID"; '''
        Q = self.cursor.execute(sql).fetchall()
        results = []
        firstrn = True
        for e in Q:
            # Compare the age of the person to today and if less than 18 years, consider them a Child
            if (datetime.today().date() - e[2]).days/365  < 18:
                r = {}
                r['GUID']     = e[3]
                if firstrn == True:
                    r['FNAME']    = e[0]
                    r['LNAME']    = e[1]
                    r['NAME']     = "%s %s" % (e[0], e[1])
                    r['DOB']      = e[2]
                    r['RISKS']    = []
                    R = {}
                    R['DESC']     = str(e[4])
                    R['TYPE']     = e[5]
                    R['CATEGORY'] = e[6]
                    R['DATE']     = e[7]
                    R['E_GUID']   = e[8]
                    if R not in r['RISKS']:
                        r['RISKS'].append(R)
                    r['VPSCORE']  = len(r['RISKS'])
                    results.append(r)
                    firstrn = False
                # Check if the person exists
                else:
                    found = False
                    i = 0
                    for d in results:
                        if d['GUID'] == r['GUID']:
                            found = True
                            R = {}
                            R['DESC']     = e[3]
                            R['TYPE']     = e[4]
                            R['CATEGORY'] = e[5]
                            R['DATE']     = e[6]
                            R['E_GUID']   = e[7]
                            results[i]['RISKS'].append(R)
                            results[i]['VPSCORE'] = len(results[i]['RISKS'])
                            break
                        else:
                            i+=1

                    if found == False:

                        r['FNAME']    = e[0]
                        r['LNAME']    = e[1]
                        r['NAME']     = "%s %s" % (e[0], e[1])
                        r['RISKS']    = []
                        R = {}
                        R['DESC']     = e[3]
                        R['TYPE']     = e[4]
                        R['CATEGORY'] = e[5]
                        R['DATE']     = e[6]
                        R['E_GUID']   = e[7]
                        r['RISKS'].append(R)
                        r['VPSCORE']  = len(r['RISKS'])
                        results.append(r)

        results = sorted(results, key=lambda i: i['VPSCORE'], reverse=False)
        return results

    def Graph_VP_Risks(self, staPath, endPath, GUID, Profile):

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_HDB-Graph_VP_Risks]: process started:\n\t%s %s %s %s" % (TS, staPath, endPath, GUID, Profile ))

        if self.cursor == None:
            self.ConnectToHANA()
        # Clear the calculation table if it exists
        try:
            sql = '''DROP CALCULATION SCENARIO "MATCH_VP_PROFILE" CASCADE;'''
            self.cursor.execute(sql)
        except Exception as e:
            print('Drop failed because no existing table: %s' % str(e))

        # Create the Graph Calculation Scenario based on the user input
        sql = '''
        CREATE CALCULATION SCENARIO "MATCH_VP_PROFILE" USING '
        <?xml version="1.0"?>
        <cubeSchema version="2" operation="createCalculationScenario" defaultLanguage="en">
         <calculationScenario name="MATCH_VP_PROFILE">
          <calculationViews>
               <graph name="match_subgraphs_node" defaultViewFlag="true" schema="POLER" workspace="GRAPH" action="MATCH_SUBGRAPHS">
                       <expression>
                               <![CDATA[
                                       MATCH p = (A)-[*%d..%d]-(B)
                                       WHERE A.POLER_CLASS = ''PERSON'' AND A.ENTITY_GUID = ''%d'' AND B.POLER_CLASS = ''EVENT''
                                       RETURN A.CLASS_1 AS FNAME, A.CLASS_2 AS LNAME, A.ENTITY_GUID AS P_GUID,
                                       B.DESCRIPTION AS EVENT, B.ENTITY_CATEGORY AS CATEGORY, B.ENTITY_TYPE AS TYPE,
                                       B.ENTITY_DATE AS DATE, B.ENTITY_GUID AS E_GUID
                               ]]>
                       </expression>
                       <viewAttributes>
                               <viewAttribute name="EVENT" datatype="string"/>
                               <viewAttribute name="TYPE" datatype="string"/>
                               <viewAttribute name="CATEGORY" datatype="string"/>
                               <viewAttribute name="DATE" datatype="date"/>
                               <viewAttribute name="E_GUID" datatype="string"/>
                       </viewAttributes>
               </graph>
          </calculationViews>
         </calculationScenario>
        </cubeSchema>
        '
        WITH PARAMETERS ('EXPOSE_NODE'=('match_subgraphs_node','MATCH_VP_PROFILE'));
        ''' % (staPath, endPath, int(GUID))
        try:
            self.cursor.execute(sql)
        except Exception as e:
            print(str(e))
        try:
            sql = ''' SELECT DISTINCT * FROM "MATCH_VP_PROFILE"; '''
            Q = self.cursor.execute(sql).fetchall()

        except Exception as e:
            e = str(e)
            print('ERROR IN CREATION OF WORKSPACE: %s' % e)
            if 'Inconsistent data: Found [' in e:
                # Full message example: Inconsistent data: Found [0] as source or target key, which are no valid vertex keys.
                self.Graph_Correct(e)
            try:
                self.cursor.execute(sql)
            except Exception as e:
                print(str(e))
                return None

        sql = ''' SELECT DISTINCT * FROM "MATCH_VP_PROFILE"; '''
        Q = self.cursor.execute(sql).fetchall()
        Profile['RISKS'] = []
        for e in Q:
            R = {}
            R['DESC']     = str(e[0])
            R['TYPE']     = e[1]
            R['CATEGORY'] = e[2]
            R['DATE']     = e[3]
            R['GUID']     = e[4]
            Profile['RISKS'].append(R)

        return Profile

    def SPF_Run_Full(self):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        t1 = Thread(target=self.SPF_Run_Full_Thread, )
        t1.start()
        return "Started full extraction of systems at %s" % TS

    def SPF_Run_Full_Thread(self):

        self.SPF_CRIMES2_C_OFFENCE_CODE()
        self.SPF_CRIMES2_TB_PERSON_BIO()
        self.SPF_FOCUS_TB_IR_PERSON_INVOLVED()
        self.SPF_FOCUS_TB_IR_VEHICLE()
        self.SPF_PDS_TB_NRIC_INFO()
        self.SPF_PDS_TB_PERSON_VIEW()
        self.SPF_OTTER_PHONE()
        self.SPF_OTTER_VEHICLE_INFO()
        self.SPF_CRIMES2_TEST_TB_CASE()
        self.SPF_CRIMES2_TB_CASE_PERSON_RESULT()
        self.SPF_CRIMES2_TB_OFFENCE()
        self.SPF_CRIMES2_TB_CONNECTED_REPORT()

    def SPF_Get_Person(self, search):

        search = str(search).replace("'", "")
        Unknown        = 'Unknown'
        SPF_Get_Person = 'SPF_Get_Person'
        DESC = 'Unknown person created from reference %s.' % search
        DOB = '1800-01-01'

        sql = '''  SELECT "P_GUID" FROM "POLER"."PERSON" WHERE CONTAINS (("P_ORIGINREF"), '%s') ''' % search
        results = self.cursor.execute(sql).fetchone()
        if results:
            P_GUID = results[0]
        else:
            P_GUID = self.insertPerson('U', Unknown, Unknown, DOB, Unknown, SPF_Get_Person, search, SPF_Get_Person+' '+Unknown, DESC)
        return P_GUID


    def SPF_Clean_Date(self, date, dtype):

        if date == '' or date == None:

            return '1900-01-01', '12:00:00', '19000101120000'

        if dtype == 'SPF_CRIMES2_C_OFFENCE_CODE' or dtype == 'SPF_CRIMES2_TB_OFFENCE' or dtype == 'SPF_CRIMES2_TB_CASE_PERSON_RESULT' or dtype == 'SPF_TEST_TB_CASE':

            E_DATE = datetime.strptime(date[:10], "%d/%m/%Y").strftime("%Y-%m-%d")
            E_TIME = datetime.strptime(date[10:][:date[10:].find(".")].strip(), "%H:%M:%S")
            if 'PM' in date and E_TIME.hour != 12:
                E_TIME = (E_TIME + timedelta(hours=12)).strftime("%H:%M:%S")
            else:
                E_TIME = E_TIME.strftime("%H:%M:%S")

        elif dtype == 'SPF_CRIMES2_TB_CONNECTED_REPORT' or dtype == 'SPF_CRIMES2_TB_PERSON_BIO':
            E_DATE = datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d")
            E_TIME = '12:00:00'

        elif dtype == 'SPF_CRIMES2_TB_PERSON_BIO':
            E_DATE = datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d")
            return E_DATE

        elif dtype == 'SPF_OTTER_VEHICLE_INFO':
            E_DATE = datetime.strptime(date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            E_TIME = '12:00:00'

        elif dtype == 'SPF_PDS_TB_NRIC_INFO':
            if date != None:
                E_DATE = datetime.strptime(date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                return E_DATE
            else:
                return "1900-01-01"

        elif dtype == 'SPF_PDS_TB_NRIC_INFOB' or dtype == 'SPF_FOCUS_TB_IR_VEHICLE' or dtype == 'SPF_FOCUS_TB_IR_INCIDENT' or dtype == 'SPF_FOCUS_TB_IR_PERSON_INVOLVED':
            if date != None:
                E_DATE = datetime.strptime(date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
                E_TIME = datetime.strptime(date[10:16].strip(), "%H:%M").strftime("%H:%M")
            else:
                E_DATE = '1900-01-01'
                E_TIME = '12:00'

        elif dtype == 'SPF_CRIMES2_TB_PERSON_BIO' or dtype == 'SPF_FOCUS_TB_IR_PERSON_INVOLVEDb' or dtype == 'SPF_PDS_TB_PERSON_VIEW':
            E_DATE = datetime.strptime(date[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
            return E_DATE

        E_DTG = (E_DATE + E_TIME).replace("-", "").replace(":", "")

        return E_DATE, E_TIME, E_DTG

    def SPF_Clean_Name(self, name, dtype):

        if dtype == 'SPF_CRIMES2_TB_PERSON_BIO' or dtype == 'SPF_PDS_TB_NRIC_INFO' or dtype == 'SPF_FOCUS_TB_IR_PERSON_INVOLVED' or dtype == 'SPF_PDS_TB_PERSON_VIEW' and name != None:
            if ',' in name:
                split = name.find(",")
            else:
                split = name.find(" ")
            stlen = len(name)
            FNAME = name[:split]
            if ',' in name:
                LNAME = name[-(stlen-split-2):]
            else:
                LNAME = name[-(stlen-split-1):]
        else:
            FNAME = 'Unknown'
            LNAME = 'Unknown'

        return FNAME, LNAME


    def SPF_Extract_CRIMES2_TB_CASE_PERSON_RESULT(self, results):

        dtype = 'SPF_CRIMES2_TB_CASE_PERSON_RESULT'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = P_LOGSOURCE = 'C1'
        E_XCOORD = L_XCOORD = L_YCOORD = L_ZCOORD = 0.0
        E_YCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        OffenceCode    = 'OffenceCode'
        OffenceCat     = 'UnknownCode'
        Offence        = 'Offence'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        OccurredAt     = 'OccurredAt'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Person         = 'Person'
        Object         = 'Object'
        Location       = 'Location'
        DocumentIn     = 'DocumentIn'
        OfType         = 'OfType'
        CaseResult     = 'CaseResult'
        Case           = 'Case'
        PartOf         = 'PartOf'
        Involves       = 'Involves'

        for r in results:
            CasePersonID = r[0]
            PersonID     = r[1]
            CaseType     = r[2]
            CaseDate     = r[3]
            CreatedBy    = r[4]
            CreatedTS    = r[5]
            ModifiedBy   = r[6]
            ModifiedTS   = r[7]
            CaseNoID     = r[8]
            OffenceID    = r[9]
            Result       = r[10]
            TotalSent    = r[11]
            TotalFine    = r[12]
            TotalStrks   = r[13]
            TotalRest    = r[14]
            TotalLoss    = r[15]

            DESC = 'Case for %s on %s has resulted in a total sentence of %s days and total loss of %s ' % (PersonID, CreatedTS, TotalSent, TotalLoss)
            O_ORIGINREF = "SPFCASERESULT%s%s%s" % (CaseNoID, OffenceID, PersonID)
            resultGUID = self.insertObject(CaseResult, CaseType, DESC, TotalSent, TotalFine, TotalStrks, TotalRest, O_ORIGINREF, O_LOGSOURCE)

            P_ORIGINREF = 'SPFPerson%s' % PersonID
            personGUID, exists = self.EntityResolve({'TYPE' : 'Object', 'LOOKUP' : P_ORIGINREF})
            if exists == 0:
                personGUID = self.insertPerson('U', 'UnknownFirstName', 'UnknownLastName', '1900-01-01', NA, dtype, P_ORIGINREF, P_LOGSOURCE, DESC)

            O_ORIGINREF = "SPFCase%s%s%s" % (CaseNoID, OffenceID, PersonID)
            caseGUID = self.insertObject(Case, CaseType, DESC, TotalSent, TotalFine, TotalStrks, TotalRest, O_ORIGINREF, O_LOGSOURCE)

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(CreatedTS, dtype)
            E_CLASS1      = CaseType
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFCase%s%s%s%s" % (OffenceID, CreatedBy, dtype, E_DTG)
            Creation_GUID = self.insertEvent(RecordCreation, Offence, DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            mE_DATE, mE_TIME, mE_DTG = self.SPF_Clean_Date(ModifiedTS, dtype)
            E_DESC        = DESC.replace("created", "modified")
            E_CLASS1      = OffenceID
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFCase%s%s%s%s" % (OffenceID, ModifiedBy, dtype, E_DTG)
            Modified_GUID = self.insertEvent(RecordModified, Offence, DESC, E_LANG, E_CLASS1, mE_TIME, mE_DATE, mE_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            OffenceGUID, exists = self.EntityResolve({'TYPE' : 'Event', 'LOOKUP' : 'SPFOffence%s' % (OffenceID)})
            if exists == 0:
                E_TYPE = RecordCreation
                E_CATEGORY = Offence
                E_DESC = "Offence created automatically based on no record found with Offence ID %s" % OffenceID
                E_CLASS1 = NA
                E_ORIGINREF = "SPFOffence%s%s%s%s" % (OffenceID, CreatedBy, dtype, E_DTG)
                OffenceGUID  = self.insertEvent(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, CreatedBy, dtype)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, CreatedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Modification System Entity
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, ModifiedBy, dtype)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, ModifiedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(OffenceGUID, Event, PartOf, caseGUID, Object)
            self.insertRelation(caseGUID, Object, Involves, personGUID, Person)

    def SPF_CRIMES2_TB_CASE_PERSON_RESULT(self):

        sql = '''SELECT "CASE_PERSON_RESULT_ID",
                    "PERSON_ID",
                    "CASE_RESULT_TYPE_CD",
                    "CASE_RESULT_DT",
                    "CREATED_BY",
                    "CREATED_TS",
                    "LAST_MODIFIED_BY",
                    "LAST_MODIFIED_TS",
                    "CASE_NO_ID",
                    "OFFENCE_ID",
                    "RESULT",
                    "TOTAL_SENTENCE_PERIOD",
                    "TOTAL_FINE",
                    "TOTAL_NUMBER_OF_STROKES",
                    "TOTAL_RESTITUTION_AMT",
                    "TOTAL_LOSS_AMT"
                    FROM "CRIMES2_TEST"."TB_CASE_PERSON_RESULT"'''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_TB_CASE_PERSON_RESULT]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_TB_CASE_PERSON_RESULT]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_TB_CASE_PERSON_RESULT(results)
        t = Thread(target=self.SPF_Extract_CRIMES2_TB_CASE_PERSON_RESULT, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "SPF_TB_CASE_PERSON_RESULT extraction of %d records started at %s" % (len(results), TS)


    def SPF_Extract_CRIMES2_TB_OFFENCE(self, results):

        dtype = 'SPF_CRIMES2_TB_OFFENCE'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = P_LOGSOURCE = 'C1'
        E_XCOORD = L_XCOORD = L_YCOORD = L_ZCOORD = 0.0
        E_YCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        OffenceCode    = 'OffenceCode'
        OffenceCat     = 'UnknownCode'
        Offence        = 'Offence'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        OccurredAt     = 'OccurredAt'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        Location       = 'Location'
        DocumentIn     = 'DocumentIn'
        OfType         = 'OfType'

        for r in results:
            OffenceID      = r[0]
            OffenceCode    = r[1]
            OffenceFromDT  = r[2]
            OffenceToDT    = r[3]
            PlaceDesc      = r[4]
            AddressID      = r[5]
            CreatedBy      = r[6]
            CreatedTS      = r[7]
            LastModified   = r[8]
            LastModifiedTS = r[9]
            MO             = r[10]
            ValueInvolved  = r[11]
            PlaceDescCD    = r[12]

            # Lookup the OFFENCE_CODE_ID AS SPF_CRIMES2_C_OFFENCE_CODE%s ID
            O_ORIGINREF = 'SPF_CRIMES2_C_OFFENCE_CODE%s' % OffenceCode
            OffenceDesc = 'Offence with code %s created on %s with MO %s.' % (OffenceCode, CreatedTS, MO)
            OffenceCodeGUID, exists = self.EntityResolve({'TYPE' : 'Object', 'LOOKUP' : '%s' % O_ORIGINREF})
            if exists == 0:
                OffenceCodeGUID  = self.insertObject(OffenceCode, OffenceCat, OffenceDesc, OffenceCode, OffenceCat, OffenceCat, dtype, O_ORIGINREF, O_LOGSOURCE)

            L_ORIGINREF = "%s%s%s%s" % (PlaceDesc, PlaceDescCD, AddressID, dtype)
            LocationGUID, exists = self.EntityResolve({'TYPE' : 'Location', 'LOOKUP' : '%s' % L_ORIGINREF})
            if exists == 0:
                L_TYPE = 'Offence'
                L_CLASS1 = NA
                LocationGUID = self.insertLocation(L_TYPE, OffenceDesc, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, dtype, L_ORIGINREF, L_LOGSOURCE)

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(CreatedTS, dtype)
            E_CLASS1      = MO
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFOffence%s%s%s%s" % (OffenceID, CreatedBy, dtype, E_DTG)
            Creation_GUID = self.insertEvent(RecordCreation, Offence, OffenceDesc, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(LastModifiedTS, dtype)
            E_DESC        = OffenceDesc.replace("created", "modified")
            E_CLASS1      = OffenceCode
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFOffence%s%s%s%s" % (OffenceID, ModifiedBy, dtype, E_DTG)
            Modified_GUID = self.insertEvent(RecordModified, Offence, OffenceDesc, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, CreatedBy, dtype)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, CreatedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Modification System Entity
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, ModifiedBy, dtype)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, ModifiedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(Creation_GUID, Event, OfType, OffenceCodeGUID, Object)
            self.insertRelation(Modified_GUID, Event, OfType, OffenceCodeGUID, Object)
            self.insertRelation(Creation_GUID, Event, OccurredAt, LocationGUID, Location)


    def SPF_CRIMES2_TB_OFFENCE(self):

        sql = '''SELECT "OFFENCE_ID",
                    "OFFENCE_CODE_ID",
                    "OFFENCE_FROM_DT",
                    "OFFENCE_TO_DT",
                    "PLACE_DESC",
                    "ADDR_ID",
                    "CREATED_BY",
                    "CREATED_TS",
                    "LAST_MODIFIED_BY",
                    "LAST_MODIFIED_TS",
                    "MO",
                    "VALUE_AMT_INVOLVED",
                    "PLACE_DESC_CD"
                    FROM "CRIMES2_TEST"."TB_OFFENCE"'''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_CRIMES2_TB_OFFENCE]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_CRIMES2_TB_OFFENCE]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_TB_OFFENCE(results)
        t = Thread(target=self.SPF_Extract_CRIMES2_TB_OFFENCE, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "CRIMES2_TB_OFFENCE extraction of %d records started at %s" % (len(results), TS)

    def SPF_Extract_CRIMES2_TEST_TB_CASE(self, results):

        dtype = 'SPF_TEST_TB_CASE'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = P_LOGSOURCE = 'C1'
        E_XCOORD = L_XCOORD = L_YCOORD = L_ZCOORD = 0.0
        E_YCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        OffenceCode    = 'OffenceCode'
        OffenceCat     = 'UnknownCode'
        Case           = 'Offence'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        OccurredAt     = 'OccurredAt'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        Location       = 'Location'
        DocumentIn     = 'DocumentIn'
        OfType         = 'OfType'
        Organization   = 'Organization'
        Police         = 'Police'
        Owns           = 'Owns'

        for r in results:
            CaseNoID       = r[0]
            Department     = str(r[1].getvalue())
            CaseFileNo     = r[2]
            CaseType       = r[3]
            CaseName       = r[4]
            CaseStatus     = r[5]
            CreatedBy      = r[6]
            CreatedTS      = r[7]
            LastModifiedBy = r[8]
            LastModifiedTS = r[9]
            SecurityClass  = r[10]
            BranchCode     = r[11]
            DivisionCode   = r[12]

            CaseDESC = "Case %s owned by department %s %s %s with current status of %s and classification %s." % (CaseNoID, Department, BranchCode, DivisionCode, CaseStatus, SecurityClass)
            O_ORIGINREF = 'SPFCase%s' % (CaseNoID)
            CaseGUID = self.insertObject(Case, CaseType, CaseDESC, CaseStatus, CaseFileNo, CaseName, dtype, O_ORIGINREF, O_LOGSOURCE)

            DeptDESC = "SPF deparment %s %s %s" % (Department, BranchCode, DivisionCode)
            # Division
            O_ORIGINREF = 'SPFDept%s%s%s' % (Department, BranchCode, DivisionCode)
            DeptGUID = self.insertObject(Organization, Police, DeptDESC, Department, BranchCode, DivisionCode, dtype, O_ORIGINREF, O_LOGSOURCE)

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(CreatedTS, dtype)
            E_CLASS1      = CaseStatus
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFCase%s%s%s%s" % (CaseNoID, CreatedBy, dtype, E_DTG)
            Creation_GUID = self.insertEvent(RecordCreation, Case, CaseDESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(LastModifiedTS, dtype)
            E_DESC        = CaseDESC
            E_CLASS1      = OffenceCode
            E_ORIGIN      = dtype
            E_ORIGINREF   = "SPFCasee%s%s%s%s" % (CaseNoID, ModifiedBy, dtype, E_DTG)
            Modified_GUID = self.insertEvent(RecordModified, Case, CaseDESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, CreatedBy, dtype)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, CreatedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Modification System Entity
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, ModifiedBy, dtype)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, ModifiedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(CaseGUID, Object, CreatedOn, Creation_GUID, Event)
            self.insertRelation(DeptGUID, Object, Owns, CaseGUID, Object)


    def SPF_CRIMES2_TEST_TB_CASE(self):

        sql = '''SELECT "CASE_NO_ID",
                        "DEPT_DIV_CD",
                        "CASE_FILE_NO",
                        "CASE_TYPE_CD",
                        "CASE_NAME",
                        "CASE_STATUS_CD",
                        "CREATED_BY",
                        "CREATED_TS",
                        "LAST_MODIFIED_BY",
                        "LAST_MODIFIED_TS",
                        "SECURITY_CLASSIFICATION",
                        "BRANCH_CD",
                        "SUBDIVISION_CD"
                        FROM "CRIMES2_TEST"."TEST_TB_CASE"'''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_CRIMES2_TEST_TB_CASE]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_CRIMES2_TEST_TB_CASE]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_TEST_TB_CASE(results)
        t = Thread(target=self.SPF_Extract_CRIMES2_TEST_TB_CASE, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "CRIMES2_TB_OFFENCE extraction of %d records started at %s" % (len(results), TS)


    def SPF_Extract_CRIMES2_C_OFFENCE_CODE(self, results):

        dtype = 'SPF_CRIMES2_C_OFFENCE_CODE'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = 'C1'
        E_XCOORD = 0.0
        E_YCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        OffenceCode    = 'OffenceCode'
        OffenceChapter = 'OffenceChapter'
        OffenceAct     = 'OffenceAct'
        Offence        = 'Offence'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        DocumentIn     = 'DocumentIn'

        for r in results:

            RecordCreationDTG = r[0]
            RecordModDTG      = r[1]
            ActCLASS1         = r[2]
            ActCLASS2         = r[3]
            ActCLASS3         = r[4]
            ChapterCATEGORY   = r[5]
            ChapterCLASS1     = r[6]
            ChapterCLASS2     = r[7]
            ChapterCLASS3     = r[8]
            OffenceDESC       = r[9].upper()
            OffenceCATEGORY   = r[10]
            OffenceCLASS1     = r[11]
            OffenceCLASS2     = r[12]
            SourceSystem1DESC = r[13]
            SourceSystem2DESC = r[14]

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordCreationDTG, dtype)
            E_DESC        = "%s %s %s created on %s." % (OffenceCode, ChapterCLASS3, OffenceDESC, E_DATE)
            E_CLASS1      = ChapterCLASS1
            E_ORIGIN      = dtype
            E_ORIGINREF   = "%s%s%s%s" % (SourceSystem1DESC, dtype, RecordCreationDTG, RecordModDTG)
            Creation_GUID = self.insertEvent(RecordCreation, OffenceCode, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordModDTG, dtype)
            E_DESC        = "%s %s %s modified on %s." % (OffenceCode, ChapterCLASS3, OffenceDESC, E_DATE)
            E_CLASS1      = ChapterCLASS1
            E_ORIGIN      = dtype
            E_ORIGINREF   = "%s%s%s%s" % (SourceSystem2DESC, dtype, RecordCreationDTG, RecordModDTG)
            Modified_GUID = self.insertEvent(RecordModified, OffenceCode, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Offence Object
            O_CLASS3      = ChapterCLASS1
            O_ORIGIN      = SourceSystem1DESC
            O_ORIGINREF   = "SPF_CRIMES2_C_OFFENCE_CODE%s%s%s%s" % (OffenceCode, OffenceCLASS1, OffenceCATEGORY, OffenceDESC)
            Offence_GUID  = self.insertObject(OffenceCode, OffenceCATEGORY, OffenceDESC, OffenceCLASS1, OffenceCLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Offence Chapter Object
            O_ORIGIN      = SourceSystem1DESC
            O_ORIGINREF   = "%s%s%s%s" % (OffenceChapter, ChapterCATEGORY, OffenceDESC, ChapterCLASS1)
            Chapter_GUID  = self.insertObject(OffenceChapter, ChapterCATEGORY, OffenceDESC, ChapterCLASS1, ChapterCLASS2, ChapterCLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Offence Act Object
            O_ORIGIN      = SourceSystem1DESC
            O_ORIGINREF   = "%s%s%s%s" % (Offence, OffenceAct, NA, ActCLASS1)
            Act_GUID      = self.insertObject(Offence, OffenceAct, NA, ActCLASS1, ActCLASS2, ActCLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, SourceSystem1DESC, dtype)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem1DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Modification System Entity
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, SourceSystem2DESC, dtype)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem2DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Create the relations for events
            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(Offence_GUID, Object, CreatedOn, Creation_GUID, Event)
            self.insertRelation(Offence_GUID, Object, ModifiedOn, Modified_GUID, Event,)

            # Create the relations that form the offence
            self.insertRelation(Offence_GUID, Object, DocumentIn, Chapter_GUID, Object)
            self.insertRelation(Chapter_GUID, Object, DocumentIn, Act_GUID, Object)

    def SPF_CRIMES2_C_OFFENCE_CODE(self):

        sql = '''SELECT "CREATED_TS",
                    "LAST_MODIFIED_TS",
                    "SUBLEG_TITLE",
                    "SUB_SECTION1",
                    "SUB_SECTION2",
                    "TITLE_OF_ACT",
                    "CHAPTER_PART1",
                    "CHAPTER_PART2",
                    "SECTION_CHAPTER",
                    "OFFENCE_DESC",
                    "OFFENCE_CATEGORY",
                    "OFFENCE_CODE_ID",
                    "OFFENCE_CODE",
                    "CREATED_BY",
                    "LAST_MODIFIED_BY"
                    FROM "CRIMES2_TEST"."C_OFFENCE_CODE"'''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_C_OFFENCE_CODE]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_C_OFFENCE_CODE]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_C_OFFENCE_CODE(results)
        t = Thread(target=self.SPF_Extract_CRIMES2_C_OFFENCE_CODE, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "SPF_CRIMES2_C_OFFENCE_CODE extraction of %d records started at %s" % (len(results), TS)

    def SPF_Extract_CRIMES2_TB_CONNECTED_REPORT(self, results):

        dtype = 'SPF_CRIMES2_TB_CONNECTED_REPORT'
        Case = 'Case'
        Report = 'Report'
        Document = 'Document'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = 'C1'
        E_ORIGIN = O_ORIGIN = dtype
        E_XCOORD = 0.0
        E_YCOORD = 0.0
        RecordModified = 'RecordModified'
        SourceSystem   = 'SourceSystem'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        DocumentIn     = 'DocumentIn'
        O_CLASS1       = NA
        O_CLASS2       = NA
        O_CLASS3       = NA

        for r in results:
            CaseORIGINREF     = r[0]
            ReportORIGINREF   = r[1]
            CaseDESC          = r[2]
            SourceSystemDESC  = r[3]
            RecordModDate     = r[4]

            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordModDate, dtype)
            E_GUID = self.insertEvent(RecordModified, Case, CaseDESC, E_LANG, ReportORIGINREF, E_TIME, E_DATE, E_DTG,  E_XCOORD, E_YCOORD, E_ORIGIN, CaseORIGINREF, E_LOGSOURCE)

            Case_GUID   = self.insertObject(Document, Case, CaseDESC, CaseORIGINREF, SourceSystemDESC, O_CLASS3, O_ORIGIN, Case+CaseORIGINREF, O_LOGSOURCE)
            Report_GUID = self.insertObject(Document, Report, CaseDESC, ReportORIGINREF, SourceSystemDESC, O_CLASS3, O_ORIGIN, Report+ReportORIGINREF, O_LOGSOURCE)
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystemDESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, SourceSystemDESC + ' ' + dtype, O_LOGSOURCE)

            self.insertRelation(Case_GUID, Object, ModifiedBy, Source_GUID, Object)
            self.insertRelation(Case_GUID, Object, ModifiedOn, E_GUID, Event)
            self.insertRelation(Report_GUID, Object, DocumentIn, Case_GUID, Object)

    def SPF_CRIMES2_TB_CONNECTED_REPORT(self):

        sql = '''SELECT "CASE_NO_ID",
                    "REPORT_NO",
                    "STATUS",
                    "USER_ID",
                    "CHANGED_DT"
                    FROM "CRIMES2_TEST"."TB_CONNECTED_REPORT"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_TB_CONNECTED_REPORT]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_TB_CONNECTED_REPORT]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_TB_CONNECTED_REPORT(results)
        t = Thread(target=self.SPF_Extract_CRIMES2_TB_CONNECTED_REPORT, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "SPF_Extract_SPF_CRIMES2_TB_CONNECTED_REPORT extraction of %d records started at %s" % (len(results), TS)


    def SPF_Extract_CRIMES2_TB_PERSON_BIO(self, results):

        dtype = 'SPF_CRIMES2_TB_PERSON_BIO'
        ORIGIN = dtype
        LOGSOURCE = 'C1'
        Case = 'Case'
        Report = 'Report'
        Document = 'Document'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = 'C1'
        E_ORIGIN = O_ORIGIN = dtype
        E_XCOORD = 0.0
        E_YCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        Person         = 'Person'
        DocumentIn     = 'DocumentIn'
        BirthRecord    = 'BirthRecord'

        Occupation     = 'Occupation'
        Race           = 'Race'
        PersonAttribute = 'PersonAttribute'
        Language       = 'Language'
        HasAttribute   = 'HasAttribute'
        Owns           = 'Owns'

        O_CLASS1       = NA
        O_CLASS2       = NA
        O_CLASS3       = NA

        for r in results:
            RecordCreationDTG = r[0]
            CountryDESC       = r[1]
            BirthDocCLASS1    = r[2]
            BirthDocCLASS2    = r[3]
            BirthDocCLASS3    = r[4]
            LanguageDESC      = r[5]
            OccupationCLASS1  = r[6]
            OccupationCLASS2  = r[7]
            RaceDescription   = r[8]
            SourceSystem1DESC = r[9]
            SourceSystem2DESC = r[10]
            PersonDOB         = r[11]
            PersonFullName    = r[12]
            PersonGender      = r[13]
            PersonORIGINREF   = r[14]
            RecordModDTG      = r[15]

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordCreationDTG, 'SPF_CRIMES2_C_OFFENCE_CODE')
            E_DESC        = "%s created on %s." % (PersonORIGINREF, E_DATE)
            E_CLASS1      = NA
            E_ORIGIN      = dtype
            E_ORIGINREF   = SourceSystem1DESC + ' ' + dtype + ' ' + RecordCreationDTG + ' ' + RecordModDTG
            Creation_GUID = self.insertEvent(RecordCreation, PersonORIGINREF, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordModDTG, 'SPF_CRIMES2_C_OFFENCE_CODE')
            E_DESC        = "%s modified on %s." % (PersonORIGINREF, E_DATE)
            E_CLASS1      = NA
            E_ORIGIN      = dtype
            E_ORIGINREF   = SourceSystem2DESC + ' ' + dtype + ' ' + RecordCreationDTG + ' ' + RecordModDTG
            Modified_GUID = self.insertEvent(RecordModified, PersonORIGINREF, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Set up the record authors
            O_ORIGINREF   = SourceSystem1DESC + ' ' + dtype
            Source_GUID    = self.insertObject(SourceSystem, SourceSystem, SourceSystem1DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            O_ORIGINREF   = SourceSystem2DESC + ' ' + dtype
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem2DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Set up the Person
            if PersonFullName != None:
                FNAME, LNAME = self.SPF_Clean_Name(PersonFullName, dtype)
            else:
                FNAME = LNAME = 'Unknown'

            if PersonGender == None:
                PersonGender = 'U'

            if PersonDOB != None:
                PersonDOB = self.SPF_Clean_Date(PersonDOB, dtype)

            P_ORIGINREF = "%s%s%sSPFPerson%s" % (FNAME, LNAME, PersonDOB, PersonORIGINREF)
            DESC = 'Extracted from Crimes 2 with entity id %s' % PersonORIGINREF
            POB = 'Unknown'
            P_GUID = self.insertPerson(PersonGender, FNAME, LNAME, PersonDOB, POB, ORIGIN, P_ORIGINREF, LOGSOURCE, DESC)

            # Set up the high level objects
            O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Race, RaceDescription)
            Race_GUID       = self.insertObject(PersonAttribute, Race, RaceDescription, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Language, LanguageDESC)
            Language_GUID   = self.insertObject(PersonAttribute, Language, LanguageDESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            OccupationDESC  = "%s%s" % (OccupationCLASS1, OccupationCLASS2)
            O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Occupation, OccupationDESC)
            Occupation_GUID = self.insertObject(PersonAttribute, Occupation, OccupationDESC, OccupationCLASS1, OccupationCLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Birth Document Object
            BirthDesc     = "Document of birth for %s on %s." % (PersonFullName, PersonDOB)
            O_ORIGINREF   = "%s %s" % (BirthDesc, BirthDocCLASS1)
            BirthDoc_GUID = self.insertObject(Document, BirthRecord, BirthDesc, BirthDocCLASS1, BirthDocCLASS2, BirthDocCLASS3, O_ORIGIN, NA, O_LOGSOURCE)

            # Create the relations for the person
            self.insertRelation(P_GUID, Person, HasAttribute, Race_GUID, Object)
            self.insertRelation(P_GUID, Person, HasAttribute, Language_GUID, Object)
            self.insertRelation(P_GUID, Person, HasAttribute, Occupation_GUID, Object)
            self.insertRelation(P_GUID, Person, Owns, BirthDoc_GUID, Object)

            # Create the relations for events
            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(P_GUID, Person, CreatedOn, Creation_GUID, Event)
            self.insertRelation(P_GUID, Person, ModifiedOn, Modified_GUID, Event)

    def SPF_CRIMES2_TB_PERSON_BIO(self):

        sql = '''SELECT "CREATED_TS",
                    "COUNTRY_OF_BIRTH_CD",
                    "NATIONALITY_CD",
                    "BIRTH_CERT_NUMBER",
                    "CITIZENSHIP_NO",
                    "MOTHER_TONGUE_CD",
                    "OCCUPATION_CD",
                    "OCCUPATION_DESC",
                    "RACE_CD",
                    "CREATED_BY",
                    "LAST_MODIFIED_BY",
                    "DOB",
                    "PERSON_NAME",
                    "GENDER_CD",
                    "ENTITY_ID",
                    "LAST_MODIFIED_TS"
                    FROM "CRIMES2_TEST"."TB_PERSON_BIO"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_TB_PERSON_BIO]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_CRIMES2_TB_PERSON_BIO]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_CRIMES2_TB_PERSON_BIO(results)

        t = Thread(target=self.SPF_Extract_CRIMES2_TB_PERSON_BIO, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "SPF_CRIMES2_TB_PERSON_BIO extraction of %d records started at %s" % (len(results), TS)

    def SPF_Extract_OTTER_VEHICLE_INFO(self, results):

        dtype = 'SPF_OTTER_VEHICLE_INFO'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = 'C1'
        O_ORIGIN = E_ORIGIN = dtype
        E_XCOORD = 0.0
        E_YCOORD = 0.0
        HasAttribute   = 'HasAttribute'
        HasStatus      = 'HasStatus'
        NA             = 'NA'
        Unk            = 'Unk'
        Event          = 'Event'
        Object         = 'Object'
        DocumentIn     = 'DocumentIn'
        Vehicle        = 'Vehicle'
        Registration   = 'Registration'
        RegisteredOn   = 'RegisteredOn'
        Owns           = 'Owns'
        Color          = 'Color'
        Status         = 'Status'
        Attribute      = 'Attribute'

        for r in results:
            VehicleORIGINREF  = "%s%s" % (r[0], Vehicle)
            VehicleDESC       = "VRN:%s Chassis:%s" % (r[1], r[8])
            VehicleCLASS1     = r[2].upper()  # Make
            VehicleCLASS2     = r[3].upper()  # Model
            COLOR             = r[4].upper()
            VehicleCATEGORY   = r[5]
            VehicleCLASS3     = r[6]  # Year
            PersonORIGINREF   = ("%s" % r[7]).replace("'", "")
            VehicleChassis    = r[8]
            RegistrationDate  = r[9]
            COLOR2            = r[10]
            STATUS            = r[11].upper()

            if r[2]:
                VehicleCLASS1     = r[2].upper() # Make
            else:
                VehicleCLASS1 = ''
            if r[3]:
                VehicleCLASS2     = r[3].upper() # Model
            else:
                VehicleCLASS2 = ''
            if r[4]:
                COLOR             = r[4].upper().strip()
            else:
                COLOR = ''
            if r[0] == None:
                VehicleORIGINREF = VehicleDESC

            Vehicle_GUID = self.insertObject(Vehicle, VehicleCATEGORY, VehicleDESC, VehicleCLASS1, VehicleCLASS2, VehicleCLASS3, O_ORIGIN, VehicleORIGINREF, O_LOGSOURCE)
            if COLOR2:
                COLOR = COLOR + ' ' + COLOR2
            DESC = '%s %s %s' % (COLOR, Attribute, Color)
            ColorGUID = self.insertObject(Attribute, Color, DESC, COLOR, NA, NA, NA, DESC, NA)
            DESC = '%s %s %s' % (STATUS, Attribute, Status)
            StatusGUID = self.insertObject(Attribute, Status, DESC, STATUS, NA, NA, NA, DESC, NA)

            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RegistrationDate, dtype)
            E_DESC = 'Vehicle registration on %s by %s.' % (RegistrationDate, PersonORIGINREF)
            E_CLASS1 = NA
            E_GUID = self.insertEvent(Registration, Vehicle, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, PersonORIGINREF, E_LOGSOURCE)

            # Get the potential person related to the vehicle
            P_GUID = self.SPF_Get_Person(PersonORIGINREF)

            self.insertRelation(Vehicle_GUID, Object, RegisteredOn, E_GUID, Event)
            self.insertRelation(P_GUID, Object, Owns, Vehicle_GUID, Object)
            self.insertRelation(Vehicle_GUID, Object, HasAttribute, ColorGUID, Object)
            self.insertRelation(Vehicle_GUID, Object, HasStatus, StatusGUID, Object)

    def SPF_OTTER_VEHICLE_INFO(self):

        sql = '''SELECT "VEHICLE_UID",
                    "VRN",
                    "MAKE_CODE",
                    "MODEL",
                    "PRI_COLOR",
                    "BODY_CODE",
                    "MANUFACTURE_YEAR",
                    "OWNER_UID",
                    "CHASSIS_NO",
                    "ORIGINIAL_REG_DATE",
                    "SEC_COLOR",
                    "VRN_STATUS"
                    FROM "OTTER_TEST"."VEHICLE_INFO"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_OTTER_VEHICLE_INFO]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_OTTER_VEHICLE_INFO]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_OTTER_VEHICLE_INFO(results)
        t = Thread(target=self.SPF_Extract_OTTER_VEHICLE_INFO, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "SPF_OTTER_VEHICLE_INFO extraction of %d records started at %s" % (len(results), TS)


    def SPF_Extract_OTTER_PHONE(self, results):

        dtype = 'SPF_OTTER_PHONE'
        L_LOGSOURCE = O_LOGSOURCE = 'C1'
        O_ORIGIN = L_ORIGIN = dtype
        Telephone    = 'Telephone'
        Owner        = 'Owner'
        Organization = 'Organization'
        Object       = 'Object'
        Location     = 'Location'
        Owns         = 'Owns'
        LocatedAt    = 'LocatedAt'

        for r in results:
            TelephoneNo  = r[0]
            OwnerName    = r[1]
            OwnerID      = r[2]
            StreetName   = r[3]
            BlockNo      = r[4]
            FloorNo      = r[5]
            UnitNo       = r[6]
            BuildingName = r[7]
            PostalCode   = r[8]
            PhoneType    = r[9]

            if TelephoneNo != None:
                O_ORIGINREF = '%s%s%s%s%s' % (Telephone, TelephoneNo, PhoneType, PostalCode, OwnerID)
                PhoneDesc = "%s with number %s located at %s %s %s %s. Owned by %s. " % (Telephone, TelephoneNo, StreetName, BlockNo, FloorNo, BuildingName, OwnerName)
                PhoneGUID = self.insertObject(Telephone, PhoneType, PhoneDesc, TelephoneNo, PostalCode, BuildingName, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

                if OwnerName != None:
                    O_ORIGINREF = '%s%s%s' % (Owner, OwnerName, StreetName)
                    OwnerDesc = "%s located at %s %s %s %s." % (Owner, StreetName, BlockNo, FloorNo, BuildingName)
                    OwnerGUID = self.insertObject(Organization, Owner, OwnerDesc, OwnerName, StreetName, BuildingName, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                    self.insertRelation(OwnerGUID, Object, Owns, PhoneGUID, Object)

                if StreetName != None:
                    L_ORIGINREF = '%s%s%s' % (Owner, OwnerName, StreetName)
                    L_DESC = "Telephone location at %s %s %s %s." % (StreetName, BlockNo, FloorNo, BuildingName)
                    L_TYPE = 'PhoneLocation'
                    LocationGUID = self.insertLocation(L_TYPE, L_DESC, 0.0, 0.0, 0.0, Telephone, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
                    self.insertRelation(PhoneGUID, Object, LocatedAt, LocationGUID, Location)

    def SPF_OTTER_PHONE(self):

        sql = '''SELECT "TELEPHONE_NO",
                    "OWNER_NAME",
                    "OWNER_ID",
                    "STREET_NAME",
                    "BLOCK_NO",
                    "FLOOR_NO",
                    "UNIT_NO",
                    "BUILDING_NAME",
                    "POSTAL_CODE",
                    "PHONE_TYPE"
                    FROM "OTTER_TEST"."PHONE"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_OTTER_PHONE]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_OTTER_PHONE]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_OTTER_PHONE(results)
        t = Thread(target=self.SPF_Extract_OTTER_PHONE, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        return "s_SPF_OTTER_PHONE extraction of %d records started at %s" % (len(results), TS)


    def SPF_Extract_PDS_TB_NRIC_INFO(self, results):

        dtype = 'SPF_PDS_TB_NRIC_INFO'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = P_LOGSOURCE = L_LOGSOURCE = 'C1'
        E_ORIGIN = O_ORIGIN = P_ORIGIN =  L_ORIGIN = dtype
        E_XCOORD = L_XCOORD = L_YCOORD = E_YCOORD = L_ZCOORD = 0.0
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        SourceSystem   = 'SourceSystem'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        ModifiedBy     = 'ModifiedBy'
        ModifiedOn     = 'ModifiedOn'
        NA             = 'NA'
        Unk            = 'Unk'
        U              = 'U'
        Event          = 'Event'
        Object         = 'Object'
        Person         = 'Person'
        Location       = 'Location'
        DocumentIn     = 'DocumentIn'
        BirthRecord    = 'BirthRecord'
        ContactDetail  = 'ContactDetail'
        Document       = 'Document'
        HomeOfRecord   = 'HomeOfRecord'
        LivesAt        = 'LivesAt'
        PDSRecord      = 'PDSRecord'

        Occupation     = 'Occupation'
        Race           = 'Race'
        PersonAttribute = 'PersonAttribute'
        HasAttribute   = 'HasAttribute'
        Owns           = 'Owns'

        O_CLASS1       = NA
        O_CLASS2       = NA
        O_CLASS3       = NA
        L_CLASS1       = NA

        for r in results:
            PersonORIGINREF   = r[0]
            PersonName        = r[1]
            PersonDOB         = r[2]   # YYY-MM-DD Same as C2_Person_BIO
            PersonGender      = r[3]
            RaceDescription   = r[4]
            OccupationCLASS1  = r[5]
            OccupationCLASS2  = r[6]
            SourceSystem1DESC = r[7]
            RecordCreationDTG = r[8]
            SourceSystem2DESC = r[9]
            RecordModDTG      = r[10]

            ContactHomePhone  = r[11]
            ContactMobile     = r[12]
            ContactEmail      = r[13]

            HomeBlockNumber   = r[14]
            HomeUnitNumber    = r[15]
            HomeBldgNumber    = r[16]
            HomeFloorNumber   = r[17]
            HomeStreetName    = r[18]
            HomePostCode      = r[19]

            FNAME, LNAME = self.SPF_Clean_Name(PersonName, dtype)
            if PersonDOB != "None":
                PersonDOB = self.SPF_Clean_Date(PersonDOB, dtype)
            else:
                PersonDOB = '1800-01-01'

            if PersonGender == '' or len(str(PersonGender)) > 1:
                PersonGender = U
            P_POB = 'Unknown'
            P_ORIGINREF = "%s %s %s %s" % (FNAME, LNAME, PersonDOB, PersonORIGINREF)
            DESC = 'Extracted from PDS_NRIC with entity id %s' % PersonORIGINREF
            P_GUID = self.insertPerson(PersonGender, FNAME, LNAME, PersonDOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC)
            print(PersonGender, FNAME, LNAME, PersonDOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC)

            # Set up the high level objects
            ORIGINREF = "%s %s %s" % (PersonAttribute, Race, RaceDescription)
            Race_GUID       = self.insertObject(PersonAttribute, Race, RaceDescription, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, ORIGINREF, O_LOGSOURCE)
            OccupationDESC  = "%s %s" % (OccupationCLASS1, OccupationCLASS2)
            ORIGINREF = "%s %s %s" % (PersonAttribute, Occupation, OccupationDESC)
            Occupation_GUID = self.insertObject(PersonAttribute, Occupation, OccupationDESC, OccupationCLASS1, OccupationCLASS2, O_CLASS3, O_ORIGIN, NA, O_LOGSOURCE)
            print(PersonAttribute, Occupation, OccupationDESC, OccupationCLASS1, OccupationCLASS2, O_CLASS3, O_ORIGIN, NA, O_LOGSOURCE)

            # Set up the contact details
            ContactDesc     = "Contact for %s %s with %s %s %s" % (FNAME, LNAME, ContactHomePhone, ContactMobile, ContactEmail)
            ORIGINREF = "%s %s %s" % (Document, ContactDetail, ContactDesc)
            Contact_GUID    = self.insertObject(Document, ContactDetail, ContactDesc, ContactHomePhone, ContactMobile, ContactEmail, O_ORIGIN, NA, O_LOGSOURCE)
            print(Document, ContactDetail, ContactDesc, ContactHomePhone, ContactMobile, ContactEmail, O_ORIGIN, NA, O_LOGSOURCE)
            # Set up the home as a location
            LocationDesc    = "Block:%s Unit:%s Bldg:%s Floor:%s Street:%s Post:%s Reference:%s" % (HomeBlockNumber, HomeUnitNumber, HomeBldgNumber, HomeFloorNumber, HomeStreetName, HomePostCode, PersonORIGINREF)
            Location_GUID   = self.insertLocation(HomeOfRecord, LocationDesc, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, NA, L_LOGSOURCE)
            print(HomeOfRecord, LocationDesc, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, NA, L_LOGSOURCE)

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordCreationDTG, dtype + "B")
            E_DESC        = "%s created on %s." % (PersonName, E_DATE)
            E_CLASS1      = P_ORIGINREF
            E_ORIGIN      = SourceSystem1DESC
            E_ORIGINREF   = SourceSystem2DESC
            Creation_GUID = self.insertEvent(RecordCreation, PDSRecord, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordModDTG, dtype + "B")
            E_DESC        = "%s modified on %s." % (PersonName, E_DATE)
            E_CLASS1      = P_ORIGINREF
            E_ORIGIN      = SourceSystem2DESC
            E_ORIGINREF   = SourceSystem1DESC
            Modified_GUID = self.insertEvent(RecordModified, PDSRecord, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % ()
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem1DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem2DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Create the relations for the person
            self.insertRelation(P_GUID, Person, HasAttribute, Race_GUID, Object)
            self.insertRelation(P_GUID, Person, HasAttribute, Occupation_GUID, Object)
            self.insertRelation(P_GUID, Person, HasAttribute, Contact_GUID, Object)
            self.insertRelation(P_GUID, Person, LivesAt, Location_GUID, Location)

            # Create the relations for events
            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Modified_GUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(P_GUID, Person, CreatedOn, Creation_GUID, Event)
            self.insertRelation(P_GUID, Person, ModifiedOn, Modified_GUID, Event)

    def SPF_PDS_TB_NRIC_INFO(self):

        sql = '''SELECT "NRIC_NO",
                    "NAME",
                    "DOB",
                    "GENDER_CD_DESC",
                    "RACE_CD_DESC",
                    "SSOC_OCCUPATION_CD_DESC",
                    "OCCUPATION_OTHERS",
                    "CREATED_BY",
                    "CREATED_DT",
                    "LAST_MODIFIED_BY",
                    "LAST_MODIFIED_DT",
                    "HOME_NO",
                    "MOBILE_NO",
                    "EMAIL_ADDR",
                    "BLK_HS_NO",
                    "UNIT_NO",
                    "BLDG",
                    "FLR_NO",
                    "STREET",
                    "POSTCD"
                    FROM "PDS_TEST"."TB_NRIC_INFO"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_PDS_TB_NRIC_INFO]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_PDS_TB_NRIC_INFO]: Complete with %d records." % (TS, len(results)))
        #self.SPF_Extract_PDS_TB_NRIC_INFO(results)
        t = Thread(target=self.SPF_Extract_PDS_TB_NRIC_INFO, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = "SPF_PDS_TB_NRIC_INFO extraction of %d records started at %s" % (len(results), TS)

        return message


    def SPF_Extract_PDS_TB_PERSON_VIEW(self, results):

        dtype = 'SPF_PDS_TB_PERSON_VIEW'
        E_LANG = 'en'
        E_LOGSOURCE = O_LOGSOURCE = P_LOGSOURCE = L_LOGSOURCE = 'C1'
        E_ORIGIN = O_ORIGIN = P_ORIGIN =  L_ORIGIN = dtype
        E_XCOORD = L_XCOORD = L_YCOORD = E_YCOORD = L_ZCOORD = 0.0

        Occupation      = 'Occupation'
        Race            = 'Race'
        PersonAttribute = 'PersonAttribute'
        HasAttribute    = 'HasAttribute'
        Object          = 'Object'
        Location        = 'Location'
        Person          = 'Person'
        ContactDetail   = 'ContactDetail'
        Document        = 'Document'
        U               = 'U'
        NA              = 'NA'

        for r in results:
            PersonID       = r[0]
            PersonName     = r[1]
            PersonDOB      = r[2]
            Address        = r[3]
            RaceDesc       = r[4]
            PersonGender   = r[5]
            Nationality    = r[6]
            HomeNumber     = r[7]
            MobileNumber   = r[8]
            OccupationDesc = r[9]
            Occupation     = r[10]

            FNAME, LNAME = self.SPF_Clean_Name(PersonName, dtype)
            if PersonDOB != "None" and PersonDOB != None:
                PersonDOB = self.SPF_Clean_Date(PersonDOB, dtype)
            else:
                PersonDOB = '1800-01-01'

            if PersonGender == None or len(str(PersonGender)) > 1:
                PersonGender = U
            else:
                PersonGender = PersonGender[0]

            P_ORIGINREF = "%s%s%s%s" % (FNAME, LNAME, PersonDOB, PersonID)
            P_POB       = 'Unknown'
            DESC        = 'Person created from %s with name %s' % (dtype, PersonName)
            P_GUID      = self.insertPerson(PersonGender, FNAME, LNAME, PersonDOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC)

            if Race != None:
                O_CLASS3 = 0
                O_ORIGINREF     = "%s%s%s%s" % (PersonAttribute, Race, RaceDesc, Nationality)
                RaceDesc        = '%s%s' % (RaceDesc, Nationality )
                Race_GUID       = self.insertObject(PersonAttribute, Race, RaceDesc, RaceDesc, Nationality, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Race_GUID, Object)

            if OccupationDesc != None or Occupation != None:
                O_CLASS2 = O_CLASS3 = 0
                O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Occupation, OccupationDesc)
                Occupation_GUID = self.insertObject(PersonAttribute, Occupation, OccupationDesc, OccupationDesc, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Occupation_GUID, Object)

            if MobileNumber != None or HomeNumber != None:
                O_CLASS1 = O_CLASS2 = O_CLASS3 = 0
                ContactDesc  = "Contact for %s %s with %s %s" % (FNAME, LNAME, MobileNumber, HomeNumber)
                ORIGINREF    = "%s%s%s" % (Document, ContactDetail, ContactDesc)
                Contact_GUID = self.insertObject(Document, ContactDetail, ContactDesc, MobileNumber, HomeNumber, O_CLASS3, O_ORIGIN, NA, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Contact_GUID, Object)

    def SPF_PDS_TB_PERSON_VIEW(self):

        sql = '''SELECT "ID_NO",
                    "NAME",
                    "DOB",
                    "ADDRESS",
                    "RACE",
                    "GENDER",
                    "NATIONALITY",
                    "HOME_NO",
                    "MOBILE_NO",
                    "OCCUPATION_CODE",
                    "OCCUPATION_DESC"
                    FROM "PDS_TEST"."PERSON_VIEW"'''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_PDS_TB_PERSON_VIEW]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_SPF_PDS_TB_PERSON_VIEW]: Complete with %d records." % (TS, len(results)))
        #self.SPF_Extract_PDS_TB_PERSON_VIEW(results)
        t = Thread(target=self.SPF_Extract_PDS_TB_PERSON_VIEW, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = "SPF_PDS_TB_PERSON_VIEW extraction of %d records started at %s" % (len(results), TS)

        return message

    def SPF_Extract_FOCUS_TB_IR_VEHICLE(self, results):

        dtype = 'SPF_FOCUS_TB_IR_VEHICLE'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = 'C1'
        O_ORIGIN = E_ORIGIN = L_ORIGIN = dtype
        HomeOfRecord   = 'HomeOfRecord'
        LivesAt        = 'LivesAt'
        Vehicle        = 'Vehicle'
        Owns           = 'Owns'
        VehicleCATEGORY = VehicleCLASS3 = 'Unknown' # Type and year
        E_LANG = 'en'
        O_ORIGIN = E_ORIGIN = dtype
        E_XCOORD = L_XCOORD = L_YCOORD = E_YCOORD = L_ZCOORD = 0.0
        NA = 'NA'
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        SourceSystem   = 'SourceSystem'
        HasAttribute   = 'HasAttribute'
        HasStatus      = 'HasStatus'
        Event          = 'Event'
        Object         = 'Object'
        Person         = 'Person'
        CreatedBy      = 'CreatedBy'
        CreatedOn      = 'CreatedOn'
        Color          = 'Color'
        Status         = 'Status'
        Attribute      = 'Attribute'

        for r in results:
            VehicleORIGINREF  = ("%s%s" % (Vehicle, r[0]))
            ReportNumber      = r[1]
            VehicleRegNo      = r[2]
            VehicleDESC       = ("VRN:%s Chassis:%s" % (r[2], r[3])).replace("'", "")
            SourceSystem1     = r[11] # Created by
            RecordCreationDTG = r[12]
            PersonORIGINREF   = ("%s" % r[13]).replace("'", "") # Owner ID
            PersonName        = r[14]

            HomeStreetName    = r[15]
            HomeBlockNumber   = r[16]
            HomeFloorNumber   = r[17]
            HomeUnitNumber    = r[18]
            HomePostCode      = r[19]
            if r[6] != None:
                VehicleCLASS1     = r[6].upper() # Make
            else:
                VehicleCLASS1 = 'NA'
            if r[7] != None:
                VehicleCLASS2     = r[7].upper() # Model
            else:
                VehicleCLASS2 = 'NA'
            if r[8] != None:
                COLOR             = r[8].upper().strip()
            else:
                COLOR = 'NA'
            if r[9] != None:
                COLOR2            = r[9].upper().strip()
            else:
                COLOR2 = 'NA'
            if r[10] != None:
                STATUS            = r[10].upper()
            else:
                STATUS = 'NA'
            if r[0] == None:
                VehicleORIGINREF = VehicleDESC

            # Creation Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(RecordCreationDTG, dtype)
            E_DESC        = "%s %s %s recorded on %s." % (Vehicle, PersonORIGINREF, VehicleORIGINREF, E_DATE)
            E_CLASS1      = VehicleORIGINREF
            E_ORIGIN      = SourceSystem1
            E_ORIGINREF   = "%s %s" % (PersonORIGINREF, VehicleORIGINREF)
            Creation_GUID = self.insertEvent(RecordCreation, Vehicle, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Source System Entity
            O_ORIGIN      = NA
            O_ORIGINREF   = dtype + ' ' + str(SourceSystem1)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID  = self.insertObject(SourceSystem, SourceSystem, SourceSystem1, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Set up the high level objects
            if COLOR2 != '':
                COLOR = COLOR + ' ' + COLOR2
            DESC = '%s %s %s' % (COLOR, Attribute, Color)
            ColorGUID = self.insertObject(Attribute, Color, DESC, COLOR, NA, NA, NA, DESC, NA)
            DESC = '%s %s %s' % (STATUS, Attribute, Status)
            StatusGUID = self.insertObject(Attribute, Status, DESC, STATUS, NA, NA, NA, DESC, NA)

            # Set up the home location
            L_ORIGINREF     = 'SPFLocation%s%s%s%s%s' % (HomeBlockNumber, HomeFloorNumber, HomeOfRecord, HomePostCode, HomeStreetName)
            LocationDesc    = "Block:%s Unit:%s Floor:%s Street:%s Post:%s Reference:%s" % (HomeBlockNumber, HomeUnitNumber, HomeFloorNumber, HomeStreetName, HomePostCode, PersonORIGINREF)
            Location_GUID   = self.insertLocation(HomeOfRecord, LocationDesc, L_XCOORD, L_YCOORD, L_ZCOORD, NA, L_ORIGIN, NA, L_LOGSOURCE)

            Vehicle_GUID = self.insertObject(Vehicle, VehicleCATEGORY, VehicleDESC, VehicleCLASS1, VehicleCLASS2, VehicleCLASS3, O_ORIGIN, VehicleORIGINREF, O_LOGSOURCE)
            P_GUID = self.SPF_Get_Person("%s %s" % (PersonORIGINREF, PersonName))

            self.insertRelation(Vehicle_GUID, Object, HasAttribute, ColorGUID, Object)
            self.insertRelation(Vehicle_GUID, Object, HasStatus, StatusGUID, Object)
            self.insertRelation(P_GUID, Object, Owns, Vehicle_GUID, Object)
            self.insertRelation(Creation_GUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(Vehicle_GUID, Object, CreatedOn, Creation_GUID, Event)


    def SPF_FOCUS_TB_IR_VEHICLE(self):


        sql = '''
        SELECT "VEH_SYSTEM_UID_NO",
                    "REPORT_NO",
                    "VEH_REG_NO",
                    "CHASIS_NO",
                    "CREATED_BY",
                    "CREATED_DT",
                    "MAKE_CD_DESC",
                    "MODEL",
                    "PRI_COLOR_CD_DESC",
                    "SEC_COLOR_CD_DESC",
                    "VEHICLE_STATUS_DESC",
                    "CREATED_BY",
                    "CREATED_DT",
                    "OWNER_IDNO",
                    "OWNER_NAME",
                    "OWNER_STREET",
                    "OWNER_BLK_HSE_NO",
                    "OWNER_FLR_NO",
                    "OWNER_UNIT_NO",
                    "OWNER_POSTCD"
                    FROM "FOCUS_TEST"."TB_IR_VEHICLE"
        '''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_VEHICLE]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_VEHICLE]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_FOCUS_TB_IR_VEHICLE(results)
        t = Thread(target=self.SPF_Extract_FOCUS_TB_IR_VEHICLE, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = "SPF_FOCUS_TB_IR_VEHICLE extraction of %d records started at %s" % (len(results), TS)

        return message

    def SPF_Extract_FOCUS_TB_IR_INCIDENT(self, results):

        dtype = 'SPF_FOCUS_TB_IR_INCIDENT'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = 'C1'
        O_ORIGIN = E_ORIGIN = L_ORIGIN = dtype
        IncidentLocation = 'IncidentLocation'
        E_LANG = 'en'
        NA = 'NA'
        RecordCreation = 'RecordCreation'
        RecordModified = 'RecordModified'
        SourceSystem   = 'SourceSystem'
        CreatedOn      = 'CreatedOn'
        HasAttribute   = 'HasAttribute'
        HasStatus      = 'HasStatus'
        Event          = 'Event'
        FocusIncident  = 'FocusIncident'
        OccurredAt     = 'OccurredAt'
        Event          = 'Event'
        Object         = 'Object'
        Location       = 'Location'

        for r in results:

            SerialNo   = r[0]
            ReportNo   = r[1]
            IncType    = r[2]
            IncDate    = r[3]
            CreatedBy  = r[4]
            CreatedDT  = r[5]
            ModifiedBy = r[6]
            ModifiedDT = r[7]
            XCOORD     = r[8]
            YCOORD     = r[9]
            LocRemark  = r[10]
            BlockHsNo  = r[11]
            Building   = r[12]
            Street     = r[13]
            PostCode   = r[14]

            LocationDesc = "Block:%s Building:%s Street:%s Post:%s" % (BlockHsNo, Building, Street, PostCode)
            L_ORIGINREF  = "SPFLocation%s%s%s%s" % (XCOORD, YCOORD, LocRemark, PostCode)
            LocationGUID = self.insertLocation(IncidentLocation, LocationDesc, XCOORD, YCOORD, PostCode, BlockHsNo, dtype, NA, L_LOGSOURCE)

            E_TIME, E_DATE, E_DTG = self.SPF_Clean_Date(IncDate, dtype)
            E_TYPE = 'Incident'
            IncidentDesc = "%s at %s" % (IncType, LocationDesc)
            E_ORIGINREF = "%s%s%s" % (SerialNo, ReportNo, CreatedDT)
            IncidentGUID  = self.insertEvent(E_TYPE, IncType, IncidentDesc, E_LANG, SerialNo, E_TIME, E_DATE, E_DTG, XCOORD, YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            E_TIME, E_DATE, E_DTG = self.SPF_Clean_Date(CreatedDT, dtype)
            E_ORIGINREF = "%s%s%s%s" % (RecordCreation, dtype, SerialNo, CreatedDT)
            CreationGUID = self.insertEvent(RecordCreation, FocusIncident, IncidentDesc, E_LANG, SerialNo, E_TIME, E_DATE, E_DTG, XCOORD, YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            E_TIME, E_DATE, E_DTG = self.SPF_Clean_Date(ModifiedDT, dtype)
            E_ORIGINREF = "%s%s%s%s" % (RecordModified, dtype, SerialNo, ModifiedDT)
            ModifiedGUID = self.insertEvent(RecordModified, FocusIncident, IncidentDesc, E_LANG, SerialNo, E_TIME, E_DATE, E_DTG, XCOORD, YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            O_ORIGIN      = NA
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, CreatedBy, dtype)
            O_CLASS1      = NA
            O_CLASS2      = NA
            O_CLASS3      = NA
            Source_GUID    = self.insertObject(SourceSystem, SourceSystem, CreatedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            O_ORIGINREF   = "%s%s%s" % (SourceSystem, ModifiedBy, dtype)
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, ModifiedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            self.insertRelation(CreationGUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(ModifiedGUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(IncidentGUID, Event, CreatedOn, CreationGUID, Event)
            self.insertRelation(IncidentGUID, Event, OccurredAt, LocationGUID, Location)


    def SPF_FOCUS_TB_IR_INCIDENT(self):

        sql = '''
        SELECT "SERIAL_NO",
                "REPORT_NO",
                "FIR_INCD_TYPE",
                "INCD_FROM_DT",
                "CREATED_BY",
                "CREATED_DT",
                "LAST_MODIFIED_BY",
                "LAST_MODIFIED_DT",
                "LONGTITUDE",
                "LATITUDE",
                "INCD_LOC_REMARK",
                "INCD_BLK_HS_NO",
                "INCD_BLDG",
                "INCD_STREET",
                "POSTCD_IND_DESC"
                FROM "FOCUS_TEST"."TB_IR_INCIDENT"
        '''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_INCIDENT]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_INCIDENT]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_FOCUS_TB_IR_INCIDENT(results)
        t = Thread(target=self.SPF_Extract_FOCUS_TB_IR_INCIDENT, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = "SPF_FOCUS_TB_IR_INCIDENT extraction of %d records started at %s" % (len(results), TS)

        return message

    def SPF_Extract_FOCUS_TB_IR_PERSON_INVOLVED(self, results):

        dtype = 'SPF_FOCUS_TB_IR_PERSON_INVOLVED'
        E_LOGSOURCE = O_LOGSOURCE = L_LOGSOURCE = LOGSOURCE = 'C1'
        O_ORIGIN = E_ORIGIN = L_ORIGIN = ORIGIN = dtype
        IncidentLocation = 'IncidentLocation'
        XCOORD = YCOORD = 0.0
        E_LANG = 'en'
        NA = 'NA'
        PersonAttribute = 'PersonAttribute'
        RecordCreation  = 'RecordCreation'
        RecordModified  = 'RecordModified'
        SourceSystem    = 'SourceSystem'
        CreatedOn       = 'CreatedOn'
        HasStatus       = 'HasStatus'
        Event           = 'Event'
        FocusIncident   = 'FocusIncident'
        OccurredAt      = 'OccurredAt'
        Object          = 'Object'
        Location        = 'Location'
        IncType         = 'PersonIncident'
        Unk             = 'Unk'
        Person          = 'Person'
        DocumentIn      = 'DocumentIn'
        Document        = 'Document'
        BirthRecord     = 'BirthRecord'
        Occupation      = 'Occupation'
        Race            = 'Race'
        PersonAttribute = 'PersonAttribute'
        Language        = 'Language'
        HasAttribute    = 'HasAttribute'
        Owns            = 'Owns'
        Vehicle         = 'Vehicle'
        Color           = 'Color'
        MentalStatus    = 'MentalStatus'
        VehicleCATEGORY = 'Unknown'
        sEyeColor       = 'EyeColor'
        sHairColor      = 'HairColor'
        ContactDetail   = 'ContactDetail'
        ModifiedOn      = 'ModifiedOn'

        for r in results:
            SerialNo        = r[0]
            ReportNo        = r[1]
            PersonTag       = r[2]
            IDType          = r[3]
            IDNumber        = r[4]
            FullName        = r[5]
            PersonDOB       = r[6]
            OccupationDesc  = r[7]
            HomeNumber      = r[8]
            MobileNumber    = r[9]
            EyeColor        = r[10]
            HairColor       = r[11]
            MentalCondition = r[12]
            VehicleReg      = r[13]
            VehicleMake     = r[14]
            VehicleModel    = r[15]
            VehicleColor    = r[16]
            CreatedBy       = r[17]
            CreatedDT       = r[18]
            ModifiedBy      = r[19]
            ModifiedDT      = r[20]
            Alias           = r[21]
            Gender          = r[22]
            RaceDesc        = r[23]
            LanguageDesc    = r[24]

            if IncType == None:
                IncType = 'Unknown'
            E_TIME, E_DATE, E_DTG = self.SPF_Clean_Date(CreatedDT, dtype)
            E_TYPE = 'Incident'
            IncidentDesc = "Incident involving %s created on %s " % (FullName, CreatedDT)
            E_ORIGINREF = "%s%s%s" % (SerialNo, ReportNo, CreatedDT)
            IncidentGUID  = self.insertEvent(RecordCreation, IncType, IncidentDesc, E_LANG, SerialNo, E_TIME, E_DATE, E_DTG, XCOORD, YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Modification Record Event
            E_DATE, E_TIME, E_DTG = self.SPF_Clean_Date(ModifiedDT, dtype)
            E_DESC        = IncidentDesc.replace("created", "modified")
            E_CLASS1      = NA
            E_ORIGIN      = dtype
            E_ORIGINREF   = "%s%s%s" % (SerialNo, ReportNo, ModifiedDT)
            ModifiedGUID  = self.insertEvent(RecordModified, IncType, E_DESC, E_LANG, SerialNo, E_TIME, E_DATE, E_DTG, XCOORD, YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)

            # Set up the record authors
            if ModifiedBy == None:
                ModifiedBy = 'NoSource'
            O_CLASS1 = O_CLASS2 = O_CLASS3 = NA
            O_ORIGINREF   = CreatedBy + dtype
            Source_GUID   = self.insertObject(SourceSystem, SourceSystem, CreatedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            O_ORIGINREF   = ModifiedBy + dtype
            Modifier_GUID  = self.insertObject(SourceSystem, SourceSystem, ModifiedBy, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)

            # Set up the Person
            if FullName != None:
                FNAME, LNAME = self.SPF_Clean_Name(FullName, dtype)
            else:
                FNAME = LNAME = 'Unknown'

            if Gender == None:
                Gender = 'U'

            PersonDOB = self.SPF_Clean_Date(PersonDOB, dtype+'b')

            P_ORIGINREF = "%s%s%sSPFPerson%s" % (FNAME, LNAME, PersonDOB, SerialNo)
            DESC = 'Extracted from Focus with entity reference %s' % P_ORIGINREF
            POB = 'Unknown'
            P_GUID = self.insertPerson(Gender[0], FNAME, LNAME, PersonDOB, POB, ORIGIN, P_ORIGINREF, LOGSOURCE, DESC)

            # Set up the high level objects
            if RaceDesc != None:
                O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Race, RaceDesc)
                Race_GUID       = self.insertObject(PersonAttribute, Race, RaceDesc, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Race_GUID, Object)

            if LanguageDesc != None:
                O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Language, LanguageDesc)
                Language_GUID   = self.insertObject(PersonAttribute, Language, LanguageDesc, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Language_GUID, Object)

            if OccupationDesc != None:
                O_ORIGINREF     = "%s%s%s" % (PersonAttribute, Occupation, OccupationDesc)
                Occupation_GUID = self.insertObject(PersonAttribute, Occupation, OccupationDesc, Occupation, OccupationDesc, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Occupation_GUID, Object)

            if IDNumber != None:
                DocDesc       = "%s %s used by %s for identification." % (IDType, IDNumber, FullName)
                O_ORIGINREF   = "%s%s" % (Document, DocDesc)
                BirthDoc_GUID = self.insertObject(Document, IDType, DocDesc, IDNumber, DocDesc, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, Owns, BirthDoc_GUID, Object)

            if VehicleReg != None:
                VehicleORIGINREF = '%s%s' % (Vehicle, VehicleReg)
                VehicleDESC = 'VRN:%s %s %s %s' % (VehicleReg, VehicleMake, VehicleModel, VehicleColor)
                Vehicle_GUID = self.insertObject(Vehicle, VehicleCATEGORY, VehicleDESC, VehicleMake, VehicleModel, VehicleColor, O_ORIGIN, VehicleORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, Owns, Vehicle_GUID, Object)

            if MentalStatus != None:
                O_ORIGINREF = '%s%s' % (MentalStatus, MentalCondition)
                O_DESC = "%s mental condition" % MentalCondition
                Mental_GUID = self.insertObject(PersonAttribute, MentalStatus, O_DESC, MentalCondition, MentalStatus, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, Mental_GUID, Object)

            if EyeColor != None:
                O_ORIGINREF = '%s%s' % (sEyeColor, EyeColor)
                O_DESC = "%s eye color" % EyeColor
                EyeColor_GUID = self.insertObject(PersonAttribute, sEyeColor, O_DESC, EyeColor, sEyeColor, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, EyeColor_GUID, Object)

            if HairColor != None:
                O_ORIGINREF = '%s%s' % (sHairColor, HairColor)
                O_DESC = "%s hair color" % HairColor
                HairColor_GUID = self.insertObject(PersonAttribute, sHairColor, O_DESC, HairColor, sHairColor, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                self.insertRelation(P_GUID, Person, HasAttribute, HairColor_GUID, Object)

            if MobileNumber != None or HomeNumber != None:
                if HomeNumber == None:
                    HomeNumber = 'Unknown'
                if MobileNumber == None:
                    MobileNumber = 'Unknown'
                ContactDesc  = "Contact for %s %s with %s %s" % (FNAME, LNAME, MobileNumber, HomeNumber)
                ORIGINREF    = "%s%s%s" % (Document, ContactDetail, ContactDesc)
                Contact_GUID = self.insertObject(Document, ContactDetail, ContactDesc, MobileNumber, HomeNumber, O_CLASS3, O_ORIGIN, NA, O_LOGSOURCE)

            # Create the relations for events
            self.insertRelation(IncidentGUID, Event, CreatedBy, Source_GUID, Object)
            self.insertRelation(ModifiedGUID, Event, ModifiedBy, Modifier_GUID, Object)
            self.insertRelation(P_GUID, Person, CreatedOn, IncidentGUID, Event)
            self.insertRelation(P_GUID, Person, ModifiedOn, ModifiedGUID, Event)


    def SPF_FOCUS_TB_IR_PERSON_INVOLVED(self):

        sql = ''' SELECT "SERIAL_NO",
                    "REPORT_NO",
                    "PERSON_TAG_CD_DESC",
                    "ID_TYPE_CD_DESC",
                    "IDNO",
                    "NAME",
                    "DOB",
                    "OCCUPATION_OTHERS",
                    "HOME_NO",
                    "MOBILE_NO",
                    "EYE_COLOR_CD_DESC",
                    "HAIR_COLOR_CD_DESC",
                    "MENTAL_CONDITION_DESC",
                    "VEHICLE_REG_NO",
                    "VEHICLE_MAKE",
                    "VEHICLE_MODEL",
                    "VEHICLE_COLOR",
                    "CREATED_BY",
                    "CREATED_DT",
                    "LAST_MODIFIED_BY",
                    "LAST_MODIFIED_DT",
                    "ALIAS",
                    "GENDER_CD_DESC",
                    "RACE_CD_DESC",
                    "LANGUAGE_CD_DESC"
                    FROM "FOCUS_TEST"."TB_IR_PERSON_INVOLVED"
            '''

        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_PERSON_INVOLVED]: Querying HDB\n%s" % (TS, sql))
        #results = self.cursor.execute(sql).fetchall()
        results = self.cursor.execute(sql).fetchmany(50)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s-SPF_FOCUS_TB_IR_PERSON_INVOLVED]: Complete with %d records." % (TS, len(results)))

        #self.SPF_Extract_FOCUS_TB_IR_PERSON_INVOLVED(results)
        t = Thread(target=self.SPF_Extract_FOCUS_TB_IR_PERSON_INVOLVED, args=(results, ))
        t.start()
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = "SPF_FOCUS_TB_IR_PERSON_INVOLVED extraction of %d records started at %s" % (len(results), TS)

        return message



    def ETLACLED2Graph(self, acledData):
        '''
        data_id          int     Row identification
        gwno             int     Country code
        event_id_cnty    string  Number and Country acronym
        event_id_no_cnty string  Number identifier
        event_date       date    yyyy-mm-dd
        year             int     Year
        time_precision   int     Certainty of time of event
        event_type       string  Type of conflict
        actor1           string  Named of actor involved
        ally_actor_1     string  Name of ally or identifying actor
        inter1           int     Numeric code indicating the type of actor1
        actor2           string  Named of actor involved
        ally_actor_2     string  Name of ally or identifying actor
        inter2           int     Numeric code indicating the type of actor2
        interaction      int     Numeric code indicating interaction between actors
        country          string  Name of country event occurred in
        admin1           string  Largest sub-national region
        admin2           string  Next sub-national
        admin3           string  Next sub-national
        location         string  Location
        latitude         decimal Latitude
        longitude        decimal Longitude
        geo_precision    int     Certainty of location of event
        source           string  Reporting source
        notes            string  Description of event
        fatalities       int     Number of reported fatalities
        timestamp        intdate Unix timestamp of collection time
        '''
        LANG = 'en'
        LOGSOURCE = 'A1'
        FILEID = self.insertObject("Database", "ACLED", "Armed Conflict Location Event Database", "Open",
                                  "10",
                                  "1979",
                                  "BaseBook",
                                  "COIN-BB",
                                  "ACLED-COIN-BB")

        for index, acled in acledData.iterrows():

            if acled['event_date'] != '':
                DATE = acled['event_date']
                TYPE = "ConflictEvent"
                if acled['event_type'] != '':
                    CATEGORY = acled['event_type']
                else:
                    CATEGORY = 'ACLED'

                if acled['notes'] != '':
                    DESC = acled['notes']
                    DESC = '%s' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                else:
                    DESC = "DESC"

                if acled['fatalities'] != '':
                    CLASS1 = acled['fatalities']
                else:
                    CLASS1 = 0

                if acled['source'] != None:
                    ORIGIN = acled['source'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                else:
                    ORIGIN = 'ACLEDdata'
                if ':' not in str(DATE):
                    TIME = '12:00:00'
                else:
                    TIME = '12:00:00'
                DTG = int('%s' % (str(DATE).replace("/", "").replace("-", "").replace(" ", "").replace(":", "")))
                ORIGINREF = 'ACLED-%s' % index
                if acled['latitude'] != '':
                    XCOORD = float(acled['latitude'])
                    YCOORD = float(acled['longitude'])
                else:
                    XCOORD = 0.0
                    YCOORD = 0.0

                eGUID = self.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
                self.insertRelation(FILEID, 'Object', 'FROM_FILE', eGUID, 'Event')

                # Set up the Location
                if acled['latitude'] != '':

                    TYPE = 'ConflictLocation'
                    DESC = '%s, %s, %s :%s' % (acled['country'], acled['admin1'], acled['location'], acled['notes'])
                    DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                    ZCOORD = 0
                    CLASS1 = acled['fatalities']
                    lGUID = self.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
                    self.insertRelation(FILEID ,'Object', 'FROM_FILE', eGUID, 'Event')
                    self.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')

            # Set up the Actors
            if acled['actor1'] != '':
                TYPE = "Organization"
                CATEGORY = "ConflictActor"
                DESC = '%s is an actor identified within ACLED events.' % acled['actor1']
                DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                CLASS1 = acled['actor1']
                CLASS2 = acled['inter1']
                CLASS3 = acled['event_date']
                ORIGIN = acled['country']
                ORIGINREF = 'ACLED-%s' % DESC
                oGUID = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                newRel = self.insertRelation(FILEID ,'Object', 'FROM_FILE', oGUID, 'Object')

                self.insertRelation(oGUID, 'Object', 'InvolvedIn', eGUID, 'Event')
                self.insertRelation(oGUID, 'Object', 'OccurredAt', lGUID, 'Location')

            if acled['actor2'] != '':
                TYPE = "Organization"
                CATEGORY = "ConflictActor"
                DESC = '%s is an actor identified within ACLED events.' % acled['actor2']
                DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                CLASS1 = acled['actor2']
                CLASS2 = acled['inter2']
                CLASS3 = acled['event_date']
                ORIGIN = acled['country']
                ORIGINREF = 'ACLED-%s' % DESC
                oGUID2 = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                self.insertRelation(FILEID ,'Object', 'FROM_FILE', oGUID2, 'Object')

                self.insertRelation(oGUID2, 'Object', 'Involved', eGUID, 'Event')
                self.insertRelation(oGUID2, 'Object', 'OccurredAt', lGUID, 'Location')

    def update_user(self, iObj):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        message = {'SRC' : '%s_SCP_update_user' % TS, 'TXT' : '', 'TYPE' : True, 'TRACE' : []}
        DESC = ''
        if self.cursor == None:
            self.ConnectToHANA()

        if iObj['TYPE'][0] == 'USER_DETAIL':
            if iObj['EMAIL'][0] != '':
                sql = '''
                UPDATE "POLER"."OBJECT" SET "O_ORIGINREF" = '%s'
                WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % (iObj['EMAIL'][0], int(iObj['GUID']))
                DESC = '%s Updated email to %s.\n' % (TS, iObj['EMAIL'][0])
                self.cursor.execute(sql)
            if iObj['TEL'][0] != '':
                sql = '''
                UPDATE "POLER"."OBJECT" SET "O_CLASS3" = '%s'
                WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % (iObj['TEL'][0], int(iObj['GUID']))
                DESC = '%s%s Updated telephone to %s.\n' % (DESC, TS, iObj['TEL'][0])
                self.cursor.execute(sql)
            if iObj['PASSWORD'][0] != '':
                sql = '''
                UPDATE "POLER"."OBJECT" SET "O_CLASS2" = '%s'
                WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % (bcrypt.encrypt(iObj['PASSWORD'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated password.\n' % (DESC, TS)
                self.cursor.execute(sql)
            if iObj['ROLE'][0] != '':
                sql = '''
                UPDATE "POLER"."OBJECT" SET "O_CATEGORY" = '%s'
                WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % ((iObj['ROLE'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['ROLE'][0])
                print(sql)
                self.cursor.execute(sql)
            if iObj['AUTH'][0] != '':
                sql = '''
                UPDATE "POLER"."OBJECT" SET "O_ORIGIN" = '%s'
                WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % ((iObj['AUTH'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['AUTH'][0])
                print(sql)
                self.cursor.execute(sql)
            sql = '''
            SELECT "O_DESC" FROM "POLER"."OBJECT" WHERE "O_GUID" = '%d'
            ''' % (int(iObj['GUID']))
            oDESC = self.cursor.execute(sql).fetchone()
            print(oDESC)
            DESC = '%s\n%s' % (oDESC[0], DESC)
            sql = '''
            UPDATE "POLER"."OBJECT" SET "O_DESC" = '%s'
            WHERE "O_TYPE" = 'User' AND "O_GUID" = '%d' ''' % (DESC, int(iObj['GUID']))
            print(sql)
            self.cursor.execute(sql)

        print(message)
        return message

    def ETLGTD2Graph(self):
        '''
        ALL VARIABLES:
        General : 0-eventid, 4-approxdate, 5-extended, 6-resolution, 7-country, 9-region, 10-region_txt, 16-vicinity

        Criteria codes : 19-crit1, 20-crit2, 21-crit3, 22-doubtterr, 23-alternative, 24-alternative_txt, 25-multiple, 26-success

        Attack details: 27-suicide, 28-attacktype1, 30-attacktype2, 32-attacktype3, 64-motive, 65-guncertain1, 66-guncertain2, 67-guncertain3, 68-individual, 69-nperps, 70-nperpcap, 71-claimed, 72-claimmode
                        73-claimmode_txt, 74-claim2, 75-claimmode2, 76-claimmode2_txt, 77-claim3, 78-claimmode3, 79-claimmode3_txt, 80-compclaim, 81-weaptype1, 83-weapsubtype1, 84-weapsubtype1_txt
                        85-weaptype2, 86-weaptype2_txt, 87-weapsubtype2, 88-weapsubtype2_txt, 89-weaptype3, 90-weaptype3_txt, 91-weapsubtype3, 92-weapsubtype3_txt, 93-weaptype4, 94-weaptype4_txt
                        95-weapsubtype4, 96-weapsubtype4_txt, 97-weapdetail, 99-nkillus, 100-nkillter, 102-nwoundus, 103-nwoundte, 104-property, 105-propextent, 106-propextent_txt, 107-propvalue,

        Parties involved: 34-targtype1, 36-targsubtype1, 39-target1, 40-natlty1, 44-targsubtype2, 45-targsubtype2_txt, 46-corp2, 47-target2, 48-natlty2, 49-natlty2_txt, 52-targsubtype3,  54-corp3, 55-target3
                        56-natlty3, 57-natlty3_txt, 59-gsubname, 60-gname2, 61-gsubname2, 62-gname3, 63-gsubname3,


        Context: 108-propcomment, 109-ishostkid, 110-nhostkid, 111-nhostkidus, 112-nhours, 113-ndays, 114-divert, 115-kidhijcountry, 116-ransom, 117-ransomamt, 118-ransomamtus, 119-ransompaid, 120-ransompaidus
                 121-ransomnote, 122-hostkidoutcome, 123-hostkidoutcome_txt, 124-nreleased, 125-addnotes, 126-scite1, 127-scite2, 128-scite3, 130-INT_LOG, 131-INT_IDEO, 132-INT_MISC, 133-INT_ANY, 134-related


        '''


        '''
        [1-iyear, 2-imonth, 3-iday] Concatenate to yyyy-mm-dd
        8-country_txt, 11-provstate, 12-city,
        13-latitude
        14-longitude
        15-specificity
        18-summary + 17-location + 31-attacktype2_txt + 33-attacktype3_txt + 38-corp1 + 41-natlty1_txt,
                     42-targtype2, 43-targtype2_txt, 50-targtype3, 51-targtype3_txt, 53-targsubtype3_txt,

        29-attacktype1_txt
        35-targtype1_txt
        37-targsubtype1_txt
        58-gname
        82-weaptype1_txt
        98-nkill
        101-nwound
        129-dbsource

        '''

        # Prepare the data

        FILEID = self.insertObject("Database", "GTD", "Global Terrorism Database", "Open",
                                      "10",
                                      "1979",
                                      "BaseBook",
                                      "COIN-BB",
                                      "GTD-COIN-BB")
        gtdData = pd.read_excel(self.BaseBook, sheetname= "GTD")
        rows = int(gtdData.shape[0])
        gtdData['latitude'] = gtdData['latitude'].fillna(0.00)
        gtdData['longitude'] = gtdData['longitude'].fillna(0.00)
        LANG = 'en'
        LOGSOURCE = 'A1'

        for index, gtd in gtdData.iterrows():

            TYPE     = 'ConflictEvent'
            CATEGORY = gtd['attacktype1_txt']
            eDESC    = str(gtd['summary']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            XCOORD = float(gtd['latitude'])
            YCOORD = float(gtd['longitude'])
            DTG      = gtd['eventid']
            TIME = '%s:%s:00' % (str(DTG)[8:10], str(DTG)[-2:])
            DATE = '%s-%s-%s' % (str(DTG)[:4], str(DTG)[4:6], str(DTG)[6:8] )
            try:
                CLASS1 = gtd['nkill'] + gtd['nwound']
            except:
                CLASS1 = 0

            ORIGIN = self.ConDisSrc
            ORIGINREF = 'GTD-%s' % ORIGIN
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_HDB-ETLGTD2Graph]: Event: %s, %s-%s %s " % (TS, eDESC, XCOORD, YCOORD, DTG))
            eGUID = self.insertEvent(TYPE, CATEGORY, eDESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
            self.insertRelation(FILEID, 'Object', 'FROM_FILE', eGUID, 'Event')

            TYPE = 'Organization'
            CATEGORY = 'ConflictActor'
            CLASS1 = str(gtd['gname']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = str(gtd['gsubname']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS3 = str(gtd['gsubname2']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            DESC = '%s %s %s' % (CLASS1, CLASS2, CLASS3)
            oGUID1 = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            self.insertRelation(FILEID, 'Object', 'FROM_FILE', oGUID1, 'Object')

            CLASS1 = str(gtd['targtype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = str(gtd['targsubtype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS3 = str(gtd['target1']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            DESC = '%s %s %s' % (CLASS1, CLASS2, CLASS3)
            oGUID2 = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            self.insertRelation(FILEID, 'Object', 'FROM_FILE', oGUID2, 'Object')

            TYPE = 'Weapon'
            CATEGORY = str(gtd['weaptype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS1 = str(gtd['weapsubtype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = str(gtd['weapdetail']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS3 = 'CLASS3'
            DESC = '%s %s %s' % (CATEGORY, CLASS1, CLASS2)
            oGUID3 = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)

            TYPE = 'ConflictLocation'
            DESC = gtd['region_txt'] + ':' + eDESC
            DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            ZCOORD = 0
            CLASS1 = 0
            lGUID = self.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)

            self.insertRelation(FILEID, 'Object', 'FROM_FILE', lGUID, 'Location')
            self.insertRelation(oGUID1, 'Object', 'InvolvedIn', eGUID, 'Event')
            self.insertRelation(oGUID2, 'Object', 'InvolvedIn', eGUID, 'Event')
            self.insertRelation(oGUID3, 'Object', 'InvolvedIn', eGUID, 'Event')
            self.insertRelation(eGUID, 'Event', 'OccurredAt' , lGUID, 'Location')
            self.insertRelation(oGUID1, 'Object', 'ReportedAt' , lGUID, 'Location')
            self.insertRelation(oGUID2, 'Object', 'ReportedAt' , lGUID, 'Location')

            if rows > 1000 and int(index) > 0:
                p = float(float(index)/float(rows))*100
                print("[*] ROW %d: %f complete" % (int(index), p))


    def ETLUCDP2Graph(self, upsData, intelGUID):

        LOGSOURCE = 'A1'
        LANG = 'en'

        for ups in upsData:

            if ups['date_start'] != '':
                DATE = ups['date_start'][:10]
                TYPE = "ConflictEvent"
                CATEGORY = 'UCDP'

                if ups['source_article'] != '' and isinstance(ups['source_article'], str) == True:
                    DESC = ups['source_article']
                    DESC = '%s' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                else:
                    DESC = "DESC"

                if ups['best'] != '':
                    CLASS1 = ups['best']
                else:
                    CLASS1 = 0

                if ups['source_original'] != None and isinstance(ups['source_original'], str) == True:
                    ORIGIN = ups['source_original'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                else:
                    ORIGIN = 'UCDPdata'
                if ':' not in str(DATE):
                    TIME = '12:00:00'
                else:
                    TIME = str(DATE)[8:]
                DTG = int('%s' % (str(DATE + TIME).replace("/", "").replace("-", "").replace(" ", "").replace(":", "")))
                ORIGINREF = 'UCDP-%s' % ups['id']
                if ups['latitude'] != '':
                    XCOORD = float(ups['latitude'])
                    YCOORD = float(ups['longitude'])
                else:
                    XCOORD = 0.0
                    YCOORD = 0.0
                ORIGIN = ORIGIN[:200]
                eGUID = self.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
                self.insertRelation(intelGUID, 'Event', 'PROCESSED_INTEL', eGUID, 'Event')

                # Set up the Location
                if ups['latitude'] != '':

                    TYPE = 'ConflictLocation'
                    ZCOORD = 0
                    lGUID = self.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
                    self.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')

            # Set up the Actors
            if ups['side_b'] != '':
                TYPE = "Organization"
                CATEGORY = "ConflictActor"
                DESC = '%s is an actor identified within UCDP events.' % ups['side_b']
                DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                ORIGINREF = 'UCDP-%s-%s' % (ups['side_b'], ups['side_b_dset_id'])
                CLASS1 = ups['side_b']
                CLASS2 = 0
                CLASS3 = 0
                oGUID = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                self.insertRelation(intelGUID, 'Event', 'PROCESSED_INTEL', oGUID, 'Object')
                self.insertRelation(oGUID, 'Object', 'InvolvedIn', eGUID, 'Event')
                self.insertRelation(oGUID, 'Object', 'OccurredAt', lGUID, 'Location')

            if ups['side_a'] != '':
                TYPE = "Organization"
                CATEGORY = "ConflictActor"
                DESC = '%s is an actor identified within UCDP events.' % ups['side_a']
                DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                ORIGINREF = 'UCDP-%s-%s' % (ups['side_a'], ups['side_a_dset_id'])
                CLASS1 = ups['side_a']
                CLASS2 = 0
                CLASS3 = 0
                oGUID2 = self.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                self.insertRelation(intelGUID, 'Event', 'PROCESSED_INTEL', oGUID2, 'Object')
                self.insertRelation(oGUID2, 'Object', 'InvolvedIn', eGUID, 'Event')
                self.insertRelation(oGUID2, 'Object', 'OccurredAt', lGUID, 'Location')

# Testing
GUID = 0
SOURCEGUID = 0
SOURCETYPE = 'Object'
TYPE = 'o'
TARGETGUID = 0
TARGETTYPE = 'Event'

HDB = HANAModel()
#HDB.goLive()
#HDB.SPF_FOCUS_TB_IR_PERSON_INVOLVED()

#HDB.merge_entities('person', 115278292190426629, 115278311985821726)

#HDB.initialize_user()
#HDB.Graph_VP_CHILDREN(1,5)
#HDB.preLoadVPScene1()
#HDB.initialize_reset()

#HDB.get_user_profile(215263125273378168)
#print(HDB.check_date('8/31/2016'))
#HDB.Graph_VP_CHILDREN(1, 9)
#HDB.preLoadLocations()
#q = "l"
#HDB.getlatlong(q)
#HDB.preLoadLocations()
#HDB.preLoadPeople()
#HDB.insertRelation(SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE)
