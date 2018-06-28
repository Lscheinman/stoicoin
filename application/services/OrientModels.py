# -*- coding: utf-8 -*-
import pyorient
import pandas as pd
import time, os, json, requests, random, jsonify
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from openpyxl import load_workbook
from openpyxl import Workbook
from threading import Thread
from passlib.hash import bcrypt
debugging = False

class OrientModel():
    
    def __init__(self):
        
        self.user = "root"
        self.pswd = "admin"
        self.client = pyorient.OrientDB("localhost", 2424)
        self.session_id = self.client.connect(self.user, self.pswd) 
        self.Verbose = True
        self.entities = ['Person', 'Object', 'Location', 'Event']
        self.reltypes = ['AccountCreated', 'AnalysisToSupport', 'BornOn', 'BornIn', 'ChargedWith', 'CollectionToSupport', 
                         'CommittedCrime', 'CreatedAt', 'CreatedBy', 'CreatedOn', 'DocumentIn', 'DocumentMentioning', 'DocumentedBy',
                         'Family', 'Follows', 'Found', 'FromFile', 'HasAttribute', 'HasStatus', 'IncludesTag', 'Involves', 'Knows',
                         'LivesAt', 'LocatedAt', 'ModifiedBy', 'ModifiedOn', 'OfType', 'On', 'OccurredAt', 'Owns', 'PartOf', 
                         'ProcessedIntel', 'Published', 'PublishedIntel', 'PublishedTask', 'ReportedAt', 'RegisteredOn', 'ReferenceLink',
                         'RecordedBy', 'Searched', 'SubjectofContact', 'Supporting', 'Tagged', 'TaskedTo', 'TA_Reference', 'TextAnalytics',
                         'TweetLocation', 'Tweeted']
        self.setDemoDataPath()
        # If the POLER schema doesn't exist create it
        try:
            self.openDB('POLER')
        except:
            if debugging == False:
                self.Locations  = pd.read_excel(self.BaseBook, sheetname= "Locations")
                self.People     = pd.read_excel(self.BaseBook, sheetname= "People")                 
            self.initialize_reset()  

    def setDemoDataPath(self):
        if '\\' in os.getcwd():
            if debugging == False:
                self.BaseBook   = '%s\\application\\services\\data\\BaseBook.xlsx' % (os.getcwd())
                self.SocialPath = '%s\\application\\services\\data\\Social.csv' % (os.getcwd()) 
            else:
                self.BaseBook   = '%s\\data\\BaseBook.xlsx' % (os.getcwd()) # debugging line 
                self.SocialPath = '%s\\data\\Social.csv' % (os.getcwd()) # debugging line    
        else:
            if debugging == False:
                self.BaseBook   = '%s/application/services/data/BaseBook.xlsx' % (os.getcwd())
                self.SocialPath = '%s/application/services/data/Social.csv' % (os.getcwd())   
            else:
                self.BaseBook   = '%s/data/BaseBook.xlsx' % (os.getcwd()) # debugging line
                self.SocialPath = '%s/data/Social.csv' % (os.getcwd())                   

    def shutdown(self):
        self.client.shutdown(self.user, self.pswd)
        
    def openDB(self, DB):
        self.client.db_open(DB, self.user, self.pswd)
    
    def setToken(self):
        self.sessionToken = self.client.get_session_token()
        self.client.set_session_token(self.sessionToken)
        
    
    def createPOLER(self):
        
        try:
            self.client.db_create('POLER', pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)
        except:
            self.client.db_drop('POLER')
            self.client.db_create('POLER', pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_MEMORY)
        for e in self.entities:
            self.client.command("create class %s extends V" % e)
        for r in self.reltypes:
            self.client.command("create class %s extends E" % r)
            
    def closeDB(self):
        self.client.db_close()
    
    def check_date(self, E_DATE):
        
        datePatterns = ['%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y', '%Y-%d-%m', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%Y/%d/%m' ]
        for p in datePatterns:
            try:
                TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                checkedE_DATE = datetime.strftime((datetime.strptime(E_DATE, p)), datePatterns[0])
                if self.Verbose == True:
                    print("[%s_ODB-check_date]: received pattern %s with %s and returned %s." % (TS, p, E_DATE, checkedE_DATE))  
                return checkedE_DATE
            except:
                pass
        return datetime.strptime('2000-01-01', '%Y-%m-%d')    
      
    
    def findUser(self, username):
        
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-findUser]: process started." % (TS))   
        
        sql = '''select GUID, LOGSOURCE, O_CLASS2, CATEGORY, ORIGIN from Object where O_CLASS1 = '%s' ''' % username
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-findUser]: %s." % (TS, sql))                  
        
        r = self.client.command(sql)
        if len(r) == 0:
            user = None
        else:
            user = {}
            r = r[0].oRecordData
            user['GUID'] = r['GUID'] 
            user['email'] = r['LOGSOURCE']
            user['tel'] = 0
            user['location'] = r['ORIGIN']
            user['password'] = r['O_CLASS2']
            user['utype'] = r['CATEGORY']
        
        return user    
    
    
    def delete_user(self, GUID):
        # Change the password/CLASS2 into CLASS3 and set the CLASS2 to a closed code which keeps the user for records but closes access to the app
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        CLOSED_CODE = TS + "-" + str(random.randint(1000000,9999999))
        sql = '''update Object set O_CLASS3 = O_CLASS2 where TYPE = 'User' and GUID = %d
        ''' % (int(GUID))
        self.client.command(sql)
        sql = '''update Object set O_CLASS2 = 'Closed-%s' where TYPE = 'User' and GUID = %d
        ''' % (CLOSED_CODE, int(GUID))
        self.client.command(sql)
        sql = '''select O_DESC from Object where TYPE = 'User' and GUID = %d
            ''' % (int(GUID))
        r = self.client.command(sql)[0].oRecordData['O_DESC']
        DELETE_DESC = "%s\nDeleted: %s" % (r, CLOSED_CODE)
        sql = '''update Object set O_DESC = '%s' where O_TYPE = 'User' and GUID = %d
            ''' % (DELETE_DESC, int(GUID))
        self.client.command(sql) 
        
        return CLOSED_CODE 
    
    def get_user_profile(self, GUID):
        
        User = {}
        sql = '''select CATEGORY, O_CLASS1, O_DESC, LOGSOURCE, GUID, ORIGIN from Object where TYPE = 'User' and GUID = %d ''' % int(GUID)
        r = self.client.command(sql)[0].oRecordData
        User['ROLE'] = r['CATEGORY']
        User['NAME'] = r['O_CLASS1']
        User['GUID'] = r['GUID']
        User['DESC'] = str(r['O_DESC'])
        User['EMAIL'] = r['LOGSOURCE']
        User['AUTH']  = r['ORIGIN']
        User['DESC'] = 'Name: %s\nRole: %s\nAuthorization: %s\nEmail: %s\n%s' % (User['NAME'], User['ROLE'], User['AUTH'], User['EMAIL'], User['DESC'])
        
        User['ACTIVITIES'] = []
        User['TASKS']      = []
        sql = '''
        match {class: Object, as: u, where: (TYPE = 'User' and GUID = %d)}.both() {class: Event, as: e, where: (TYPE = 'UserAction')} 
        return e.CATEGORY, e.E_DESC, e.DATE, e.DTG, e.GUID, e.CLASS1, e.ORIGIN, e.XCOORD, e.YCOORD
        ''' % int(GUID)
        r = self.client.command(sql)
        for e in r:
            e = e.oRecordData
            data = {}
            data['CATEGORY'] = e['e_CATEGORY']
            data['DESC']   = str(e['e_E_DESC'])
            data['DATE']   = e['e_DATE']
            data['DTG']    = e['e_DTG']
            data['GUID']   = e['e_GUID']
            data['CLASS1'] = e['e_CLASS1']
            data['ORIGIN'] = e['e_ORIGIN']
            data['XCOORD'] = e['e_XCOORD']
            data['YCOORD'] = e['e_YCOORD']
            if data['CATEGORY'] == 'Task':
                if data not in User['TASKS']:
                    User['TASKS'].append(data)
            else:
                if data not in User['ACTIVITIES']:
                    User['ACTIVITIES'].append(data)

        User['ACTIVITIES'] = sorted(User['ACTIVITIES'], key=lambda i: i['DTG'], reverse=True)
        User['TASKS'] = sorted(User['TASKS'], key=lambda i: i['DTG'], reverse=True)
            
        return User      
    
    def get_ta_runs(self):
        sql = '''
        select GUID, CLASS1, E_DESC from Event where CATEGORY = 'TEXT_ANALYTICS'
        '''
        Q = self.client.command(sql)
        results = []
        for e in Q:
            e = e.oRecordData
            r['GUID'] = e['GUID'] 
            r['CATEGORY'] = e['CLASS1']
            r['DESC'] = str(e['E_DESC'])
            if r not in results:
                results.append(r)
        
        return results              
    
    def get_task(self, GUID):
        
        if GUID != 'None':
            sql = '''select GUID, CATEGORY, E_DESC, CLASS1, DATE, DTG, ORIGIN, XCOORD, YCOORD from Event where GUID = '%d'  ''' % GUID
        else:
            sql = '''select GUID, CATEGORY, E_DESC, CLASS1, DATE, DTG, ORIGIN, XCOORD, YCOORD from Event where CATEGORY = 'Task'  '''
          
        r = self.client.command(sql)[0].oRecordData
        task = {'GUID'   : r['GUID'],
                'NAME'   : r['CATEGORY'],
                'DESC'   : str(r['E_DESC']),
                'CLASS1' : r['CLASS1'],
                'DATE'   : r['DATE'],
                'DTG'    : int(r['DTG']),
                'STATUS' : r['ORIGIN']
                }
        if GUID == 'None': 
            GUID = int(task['GUID'])
        
        task['FROM'] = self.client.command("select O_CLASS1 from Object where GUID = %d " % r['XCOORD'])[0].oRecordData['O_CLASS1']
        task['TO']   = self.client.command("select O_CLASS1 from Object where GUID = %d " % r['YCOORD'])[0].oRecordData['O_CLASS1']

        return task    
    
    def get_entity(self, GUID, TYPE):
        
        result = {'VAL' : False, 'GUID' : GUID}
        
        sql = ''' select *, OUT().GUID, IN().GUID from %s where GUID = %s ''' % (TYPE, GUID)
        r = self.client.command(sql)[0].oRecordData
        
        if TYPE == 'Person':

            result['NAME']     = r['FNAME'] + ' ' + r['LNAME']
            result['DESC']     = "ID: %s\nGender: %s\n%s was born on %s in %s." % (GUID, r['GEN'], result['NAME'], r['DOB'], r['POB'])
            result['POLER']    = 'Person'
            result['TYPE']     = r['GEN']
            result['CATEGORY'] = r['GEN']
            result['CLASS1']   = r['FNAME']
            result['CLASS2']   = r['LNAME']
            result['DATE']     = str(r['DOB'])   
            result['ORIGIN']   = r['ORIGIN']  
            result['VAL']      = True
     
        elif TYPE == 'Object':
            
            result['NAME']     = r['TYPE'] + ' ' + r['CATEGORY']
            result['DESC']     = "ID: %s\nObject with description: %s. Classifications %s , %s, %s." % (GUID, r['O_DESC'], r['CLASS1'], r['CLASS2'], r['CLASS3'])            
            result['POLER']    = 'Object'
            result['TYPE']     = r['TYPE']
            result['CATEGORY'] = r['CATEGORY']
            result['CLASS1']   = r['CLASS1'] 
            result['CLASS2']   = r['CLASS2'] 
            result['DATE']     = str(r['DATE'] )   
            result['ORIGIN']   = r['ORIGIN']               
            result['VAL']      = True        
  
        elif TYPE == 'Location':

            result['NAME']     = r['TYPE'] + ' ' + r['L_DESC']
            result['DESC']     = "Location at %s, %s with data %s and %s." % (r['XCOORD'], r['YCOORD'], r['ZCOORD'], r['CLASS1'])            
            result['POLER']    = 'Location'
            result['TYPE']     = r['TYPE']
            result['CATEGORY'] = r['CATEGORY'] 
            result['CLASS1']   = r['XCOORD']
            result['CLASS2']   = r['YCOORD']
            result['DATE']     = result['CLASS1']  
            result['ORIGIN']   = r['ORIGIN']                 
            result['VAL']      = True      
                
        elif TYPE == 'Event':
            
            result['NAME']     = result['TYPE'] + ' ' + result['CATEGORY']
            result['DESC']     = "Event on %s. %s" % (result['DATE'], result['DESC'])  
            result['POLER']    = 'Event'
            result['TYPE']     = r['TYPE'] 
            result['CATEGORY'] = r['CATEGORY'] 
            result['CLASS1']   = str(r['E_TIME'])
            result['CLASS2']   = r['E_DTG']
            result['DATE']     = str(r['E_DATE'])   
            result['ORIGIN']   = r['E_ORIGIN']                 
            result['VAL']  = True      
        else:
            return None
        
        result['Relations'] = []
        pRelCount = 0
        oRelCount = 0
        lRelCount = 0
        eRelCount = 0        
        
        for o in r['OUT']:
            if str(o)[0] == '1':
                result['Relations'].append({'TYPE' : 'Person', 'GUID' : o, 'REL' : rel})
                pRelCount+=1 
            if str(o)[0] == '2':
                result['Relations'].append({'TYPE' : 'Object', 'GUID' : o, 'REL' : rel})
                oRelCount+=1             
                
        
        return result    
    
    
    def get_entity_relations(self, GUID):
        
        relations = []
        pRelCount = 0
        oRelCount = 0
        lRelCount = 0
        eRelCount = 0
        
        sql = ''' select OUT().GUID, IN().GUID from %s where GUID = %s ''' % (TYPE, GUID)
        r = self.client.command(sql)[0].oRecordData        
        
        for e in r:
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
    
    
    def get_users(self):
        
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-getUsers]: process started." % (TS)) 
        
        sql = ''' select O_CLASS1, GUID, CATEGORY from Object where TYPE = 'User' '''
        r = self.client.command(sql)
        results = []
        for e in r:
            e = e.oRecordData
            data = {}
            data['NAME']  = e['O_CLASS1']
            data['GUID']  = e['GUID']
            data['ROLE']  = e['CATEGORY']
            results.append(data)            

        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-getUsers]: process completed with %d users." % (TS, len(results)))  
        
        return results    
    
    def EntityResolve(self, entity):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-EntityResolve]: process started with %s." % (TS, entity))         
        
        entity['LOOKUP'] = bytes(entity['LOOKUP'], 'utf-8').decode('utf-8', 'ignore')
        entity['LOOKUP'] = entity['LOOKUP'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if entity['TYPE'] == 'Person':
            lead = '1'
        elif entity['TYPE'] == 'Object':
            lead = '2'
        elif entity['TYPE'] == 'Location':
            lead = '3'      
        elif entity['TYPE'] == 'Event':
            lead = '4'   
            
        sql = '''select from %s where ORIGINREF contains '%s' ''' % (entity['TYPE'], entity['LOOKUP'])
        if self.Verbose == True:
            print("[%s_ODB-EntityResolve]: %s SQL\n %s." % (TS, entity['TYPE'], sql))                
        r = self.client.command(sql)
        
        if len(r) == 0:
            # No matches so get the last GUID of the event
            GUID = int(str(lead + str(time.time()).replace(".", ""))) 
            exists = 0
        else:
            GUID = int(r[0][0])
            exists = 1        
        
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-EntityResolve]: Exists [%d] GUID-%s from sql %s." % (TS, exists, GUID, sql))            
                    
        return GUID, exists     
       
    def insertUser(self, username, password, email, tel, location, image, utype):
        
        User = 'User'
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertUser]: started." % (TS))    
        
        O_DESC = 'Username %s of type %s created on %s can be reached at %s' % (username, utype, TS, email)  
        O_ORIGINREF = username + email
        O_GUID = self.insertObject(User, utype, O_DESC, username, password, tel, location, O_ORIGINREF, email) 
        PGUID = self.insertPerson('U', User, username, TS, location, O_GUID, O_ORIGINREF, 'A1', O_DESC)
        self.insertRelation(PGUID, 'Person', 'AccountCreation', O_GUID, 'Object')
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-insertUser]: Created Associated HANA GUID: Person %s." % (TS, O_GUID))       
                
        return O_GUID                  
    
    def insertPerson(self, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE, DESC):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertPerson]: process started." % (TS))   
            
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
            sql = '''create vertex Person set GUID = '%s', GEN = '%s', FNAME = '%s', LNAME = '%s', 
            DOB = '%s', POB = '%s', ORIGIN = '%s', ORIGINREF = '%s', 
            LOGSOURCE = '%s' ''' % (P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, P_ORIGIN, P_ORIGINREF, P_LOGSOURCE) 
            self.client.command(sql)
        
        return P_GUID   
    
    def insertObject(self, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE):
        '''
            An Object is any virtual or physical item that can be described and associated with a person, location or event.
            The Type could be HairColor, Religion, SocialMediaAccount, CommunicationDevice, Weapon...
            Coorespondng category Brown, Atheist, Twitter, MobilePhone, Hand-Gun
            Cooresponding desc N/A, Doesn't believe in God, Username: Tweeter1 established on 5.05.05 associated with..., Phone Number: ...., SN/ 444 registered to on ...
            Cooresponding Class1 N/A, N/A, Tweeter1, 555-5555, Glock9
            Cooresponding Class2 N/A, N/A, ID-394949, SN-393910, SN-444
        '''
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-insertObject]: process started." % (TS))
        O_CLASS1 = (str(O_CLASS1).replace("'", ""))[:200]
        O_CLASS2 = (str(O_CLASS2).replace("'", ""))[:200]
        O_CLASS3 = (str(O_CLASS3).replace("'", ""))[:200]
        O_ORIGINREF = str(O_ORIGINREF) 
        
        if len(O_LOGSOURCE) > 199:
            O_LOGSOURCE = O_LOGSOURCE[:200]  
        
        if O_CATEGORY != None:
            O_CATEGORY = (O_CATEGORY[:60]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')   
        else:
            O_CATEGORY = 'Unknown'
        if len(str(O_ORIGINREF)) < 5:    
            O_ORIGINREF = ('%s%s%s%s%s%s' % (O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        O_ORIGINREF = (O_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')   
        
        if O_DESC != None:
            O_DESC = ('%s' % O_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', ''))[:5000]
        else:
            O_DESC = O_LOGSOURCE
        O_GUID, exists = self.EntityResolve({'TYPE' : 'Object', 'LOOKUP' : '%s' % O_ORIGINREF})
        if exists == 0:
            sql = ''' create vertex Object set GUID = '%s', TYPE = '%s', CATEGORY = '%s', O_DESC = '%s', O_CLASS1 = '%s', O_CLASS2 = '%s', O_CLASS3 = '%s', ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' 
            ''' % (O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            self.client.command(sql)
            
        return O_GUID    

    def insertEvent(self, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-insertEvent]: process started." % (TS))      
        
        if len(E_LOGSOURCE) > 199:
            E_LOGSOURCE = E_LOGSOURCE[:200]
        E_ORIGINREF = str(E_ORIGINREF)
        E_DATE = self.check_date(E_DATE)
        if ':' not in str(E_TIME):
            E_TIME = '12:00'
        E_DESC = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
        E_DESC = '%s' % E_DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        E_DESC = E_DESC[:5000] 
        if isinstance(E_DTG, int) == False: 
            E_DTG = E_DTG.replace("-", "").replace(":", "").replace(" ", "")
        if E_ORIGIN == None:
            E_ORIGIN = E_LOGSOURCE
        else:
            E_ORIGIN = '%s' % E_ORIGIN.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
        if E_CLASS1 != None and isinstance(E_CLASS1, str) == True:
            E_CLASS1 = (E_CLASS1.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', ''))[:200]
        if len(str(E_ORIGINREF)) < 5:
            E_ORIGINREF = ('%s%s%s%s%s%s' % (E_TYPE, E_CATEGORY, E_DESC, E_DTG, E_CLASS1, E_ORIGIN)).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        E_ORIGINREF = (E_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')
        E_ORIGIN = E_ORIGIN[:200]

        if isinstance(E_XCOORD, int) == False:
            if isinstance(E_XCOORD, float) == False:
                E_XCOORD = 0
        if isinstance(E_YCOORD, int) == False:
            if isinstance(E_YCOORD, float) == False:
                E_YCOORD = 0 
            
        E_GUID, exists = self.EntityResolve({'TYPE' : 'Event', 'LOOKUP' : '%s' % E_ORIGINREF})
        if exists == 0:
        
            sql = '''create vertex Event set GUID = '%s', TYPE = '%s', CATEGORY = '%s', E_DESC = '%s', LANG = '%s', CLASS1 = '%s', TIME = '%s', DATE = '%s',
            DTG = %s, XCOORD = %s, YCOORD = %s, ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' ''' % (
                E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE) 
            self.client.command(sql)

        return E_GUID  
    
    def insertLocation(self, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        L_LOGSOURCE = str(L_LOGSOURCE)
        L_ORIGINREF = str(L_ORIGINREF)
        L_DESC = str(L_DESC).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
 
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
        
        if len(str(L_ORIGINREF)) < 5:    
            L_ORIGINREF = ('%s%s%s%s%s%s' % (L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1))
        L_ORIGINREF = (L_ORIGINREF[:2000]).replace(" ", "").replace("-", "").replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '').replace(',', '').replace('.', '').replace('?', '').replace('!', '')            
        L_CLASS1 = 0
        
        L_GUID, exists = self.EntityResolve({'TYPE' : 'Location', 'LOOKUP' : '%s' % L_ORIGINREF })
        if exists == 0:    
            sql = ''' create vertex Location set GUID = '%s', TYPE = '%s', L_DESC = '%s', XCOORD = %s, YCOORD = %s, ZCOORD = '%s', CLASS1 = '%s', ORIGIN = '%s', ORIGINREF = '%s', LOGSOURCE = '%s' 
            ''' % (L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)
            self.client.command(sql)

        return L_GUID 
    
    def insertRelation(self, SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE):
        
        sql = ''' select expand(out('%s')) from %s where GUID = %s ''' % (TYPE, SOURCETYPE, SOURCEGUID)
        check = self.client.command(sql)
        if len(check) == 0:
            sql = ''' create edge %s from (select from %s where GUID = %s) to (select from %s where GUID = %s) ''' % (TYPE, SOURCETYPE, SOURCEGUID, TARGETTYPE, TARGETGUID)
            self.client.command(sql)

    def preLoadLocationsThread(self):
        #TODO bubble up values for progress meters
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-preLoadLocationsThread]: process started." % (TS))    
        
        Locations = self.Locations
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
    
    def preLoadPeopleThread(self):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-preLoadPeopleThread]: process started." % (TS))         
        A1 = 'A1'
        A2 = 'A2'
        A3 = 'A3'
        B1 = 'B1'
        C1 = 'C1'        
        AUTHS = [A1, B1, C1]
        People = self.People
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
            self.insertRelation(P_GUID, 'Person', 'BornOn', E_GUID, 'Event')
            self.insertRelation(E_GUID, 'Event', 'OccurredAt', L_GUID, 'Location')
            self.insertRelation(P_GUID, 'Person', 'BornIn', L_GUID, 'Location')
        
        print("People complete")  
        
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
        LANG = 'en'
        
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
            
            sql = ''' select GUID from Person where FNAME = '%s' and LNAME = '%s' ''' % (p['P_FNAME'], p['P_LNAME'])
            P = self.client.command(sql)
            P = P[0].oRecordData['GUID']
            if p['P_FNAME'] == 'Johnny':
                Johnny = P
            elif p['P_FNAME'] == 'Janney':
                Janney = P     
            elif p['P_FNAME'] == 'Jimmy':
                Jimmy = P
            elif p['P_FNAME'] == 'Chris':
                Chris = P
            elif p['P_FNAME'] == 'Connie':
                Connie = P      
            elif p['P_FNAME'] == 'Tim':
                Tim = P
            elif p['P_FNAME'] == 'Patty':
                Patty = P
            elif p['P_FNAME'] == 'June':
                June = P 
            elif p['P_FNAME'] == 'Eric':
                Eric = P  
            elif p['P_FNAME'] == 'Ethel':
                Ethel = P
            elif p['P_FNAME'] == 'Sid':
                Sid = P 
            elif p['P_FNAME'] == 'Hakim':
                Hakim = P                 
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
    
    def preLoadTasks(self):
        
        TYPE      = 'UserAction'
        CATEGORY  = 'Task'
        LANG      = 'en'
        CLASS1    = 'RFI'
        today     = datetime.now().strftime("%F %H:%M:%S")
        DATE      = today[:10]
        TIME      = today[-8:]  
        subject   = 'Neighbourhood reports'
        LOGSOURCE = 'A1'
        DESC      = 'RFI task from Tim S with subject Neighbourhood reports on %s in support.' % today
        DTG       = int(today.replace("-", "").replace(":", "").replace(" ", "").strip()) 
        ORIGIN    = 'Open'
        Tim       = int(self.client.command("select GUID from Object where O_CLASS1 = 'Tim S'")[0].oRecordData['GUID'])
        Hakim     = int(self.client.command("select GUID from Object where O_CLASS1 = 'Hakim'")[0].oRecordData['GUID'])
        ORIGINREF = "%s%s%s" % (Tim, subject, DTG)   
        
        Task  = self.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, Tim, Hakim, ORIGIN, ORIGINREF, LOGSOURCE)
        self.insertRelation(Tim, 'Object', 'PUBLISHED_TASK', Task, 'Event')
        self.insertRelation(Task, 'Event', 'TASKED_TO', Hakim, 'Object')

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
    
    def merge_ORGREF_BlockChain(self, GUID1, GUID2, GUID1type, GUID2type):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-merge_ORGREF_BlockChain]: process started with %s %s %s %s" % (TS, GUID1, GUID2, GUID1type, GUID2type))         
        # Get the ORIGINREF of the disolving entity and append to the end of the absorbing entity
        sql = ''' select ORIGINREF FROM %s WHERE GUID = %s ''' % (GUID2type, GUID2)
        bORIGINREFval = self.client.command(sql)[0].oRecordData['ORIGINREF']
        sql = ''' select ORIGINREF FROM %s WHERE GUID = %s ''' % (GUID1type, GUID1)
        aORIGINREFval = self.client.command(sql)[0].oRecordData['ORIGINREF']
        aORIGINREFval = "%s-%s" % (aORIGINREFval, bORIGINREFval)
        sql = ''' update %s set ORIGINREF = '%s' WHERE GUID = %s ''' % (GUID1type, aORIGINREFval, GUID1)
        self.client.command(sql)
        return "[%s_ODB-merge_ORGREF_BlockChain] %s" % (TS, aORIGINREFval)    
    
    def check_entity_type(self, GUID):
        
        if str(GUID)[0] == '1':
            GUIDtype  = 'Person'

        elif str(GUID)[0] == '2':
            GUIDtype  = 'Object' 

        elif str(GUID)[0] == '3':
            GUIDtype  = 'Location'

        elif str(GUID)[0] == '4':
            GUIDtype  = 'Event'
        
        return GUIDtype
                
    
    def merge_entities(self, TYPE, GUID1, GUID2):
        
        guids = [GUID1, GUID2]
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_ODB-merge_entities]: process started." % (TS))   

        GUID1type  = self.check_entity_type(GUID1)
        GUID2type  = self.check_entity_type(GUID2)
        
        self.merge_ORGREF_BlockChain(GUID1, GUID2, GUID1type, GUID2type)  
        
        sql = '''
        match {class: %s, as: u, where: (GUID = %d)}.both() {class: V, as: e } return $elements
        ''' % (GUID2type, GUID2)
        r = self.client.command(sql)
        for e in r:
            e = e.oRecordData
            if e['GUID'] != GUID1:
                for k in e.keys():
                    if 'out_' in k:
                        TYPE = k.replace('out_', '')
                        SOURCEGUID = e['GUID']
                        SOURCETYPE = self.check_entity_type(SOURCEGUID) 
                        TARGETGUID = GUID1
                        TARGETTYPE = GUID1type 
                        print("rel with %s %s %s %s %s" % (SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE))
                    if 'in_' in k:
                        TYPE = k.replace('in_', '')
                        SOURCEGUID = GUID1
                        SOURCETYPE = GUID1type
                        TARGETGUID = e['GUID']
                        TARGETTYPE = self.check_entity_type(TARGETGUID)
                        print("rel with %s %s %s %s %s" % (SOURCEGUID, SOURCETYPE, TYPE, TARGETGUID, TARGETTYPE))
                   
    
    
    def update_user(self, iObj):
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')       
        message = {'SRC' : '%s_SCP_update_user' % TS, 'TXT' : '', 'TYPE' : True, 'TRACE' : []}
        DESC = ''   
        
        if iObj['TYPE'][0] == 'USER_DETAIL':
            if iObj['EMAIL'][0] != '':
                sql = '''
                update Object set ORIGINREF = '%s' 
                where TYPE = 'User' and GUID = %d ''' % (iObj['EMAIL'][0], int(iObj['GUID']))
                DESC = '%s Updated email to %s.\n' % (TS, iObj['EMAIL'][0])
                self.cursor.execute(sql)
            if iObj['TEL'][0] != '':
                sql = '''
                update Object set "O_CLASS3" = '%s' 
                where O_TYPE = 'User' and O_GUID = %d ''' % (iObj['TEL'][0], int(iObj['GUID']))
                DESC = '%s%s Updated telephone to %s.\n' % (DESC, TS, iObj['TEL'][0])
                self.cursor.execute(sql)
            if iObj['PASSWORD'][0] != '':
                sql = '''
                update Object set "O_CLASS2" = '%s' 
                where O_TYPE = 'User' and O_GUID = %d ''' % (bcrypt.encrypt(iObj['PASSWORD'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated password.\n' % (DESC, TS)
                self.cursor.execute(sql)               
            if iObj['ROLE'][0] != '':
                sql = '''
                update Object set "O_CATEGORY" = '%s' 
                where O_TYPE = 'User' and O_GUID = %d ''' % ((iObj['ROLE'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['ROLE'][0])
                self.cursor.execute(sql)
            if iObj['AUTH'][0] != '':
                sql = '''
                update Object set "O_ORIGIN" = '%s' 
                where O_TYPE = 'User' and O_GUID = %d ''' % ((iObj['AUTH'][0]), int(iObj['GUID']))
                DESC = '%s%s Updated role to %s.\n' % (DESC, TS, iObj['AUTH'][0])
                self.cursor.execute(sql)                
            sql = '''
            select O_DESC from Object where GUID = %d
            ''' % (int(iObj['GUID']))
            oDESC = self.cursor.execute(sql).fetchone()
            DESC = '%s\n%s' % (oDESC[0], DESC)
            sql = '''
            update Object set O_DESC = '%s' 
            where O_TYPE = 'User' and O_GUID = %d ''' % (DESC, int(iObj['GUID']))
            self.cursor.execute(sql) 
            
        return message 

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
        self.insertUser('Hans', bcrypt.encrypt(password), 'Hans@email.com', '555-5555', 'A1A2A3B1C1', 'None', 'Analyst')           
        password = 'cantloginbecauserolewontseeanything'        
        self.insertUser('Open Task', bcrypt.encrypt(password), 'OpenTasks@email.com', '555-5555', 'Open Task', 'None', 'Open to any role')        
        
    def initialize_reset(self):
        try:
            self.client.db_drop('POLER', pyorient.STORAGE_TYPE_MEMORY)
        except:
            pass
        self.createPOLER()
        self.initialize_users()
        self.preLoadPeopleThread()
        self.preLoadVPScene1()
        self.preLoadTasks()
        self.openDB('POLER')
        self.client.tx_commit()
        
    def tileStats(self):
        
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-tileStats]: process started." % (TS))          
        
        tile_stats = {}
        DIR = {}
        CATEGORY = 'PIR'
        sql = '''select count(*) from Object where TYPE = '%s' ''' % CATEGORY
        DIR['PIRcount'] = int(self.client.command(sql)[0].oRecordData['count'])
        sql = '''select count(*) from Object where TYPE = '%s' and "O_CLASS2" = 'Imminent' ''' % CATEGORY
        DIR['Critical'] = int(self.client.command(sql)[0].oRecordData['count'])     
        sql = '''select count(*) from Object where TYPE = '%s' and "O_CLASS2" = 'High' ''' % CATEGORY
        DIR['High'] = int(self.client.command(sql)[0].oRecordData['count']) 
        sql = '''select count(*) from Object where TYPE = '%s' and "O_CLASS2" = 'Medium' ''' % CATEGORY
        DIR['Medium'] = int(self.client.command(sql)[0].oRecordData['count']) 
        DIR['Low'] = DIR['PIRcount'] - DIR['Critical'] - DIR['High'] - DIR['Medium'] 
        tile_stats['DIR'] = DIR
        
        DIT = {}
        CATEGORY = 'Task'
        sql = '''select count(*) from Event where CATEGORY = '%s' ''' % CATEGORY
        try:
            DIT['Taskcount'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            DIT['Taskcount'] = 0
        DIT['Outstanding'] = DIT['Taskcount'] / 3
        sql = '''select count(*) from Event where CATEGORY = '%s' ''' % CATEGORY
        try:
            DIT['Last'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            DIT['Taskcount'] = 0        
        tile_stats['DIT'] = DIT
        
        CO = {}
        CATEGORY = 'OSINTSearch'
        
        sql = '''select count(*) from Event where CATEGORY = '%s' ''' % CATEGORY
        try:
            CO['Searches'] = int(self.client.command(sql)[0].oRecordData['count'])   
        except:
            CO['Searches'] = 0
        CO['Channels'] = 3
        sql = '''select count(*) from Event where CATEGORY = '%s' ''' % CATEGORY
        try:
            CO['Lastsearch'] = int(self.client.command(sql)[0].oRecordData['count'])
        except:
            CO['Lastsearch'] = 0
        tile_stats['CO'] = CO
        
        CF = {}
        CATEGORY = 'PROCESSED_INTEL'
        sql = '''select count(*) from Event where CATEGORY  = '%s' ''' % CATEGORY
        try:
            CF['Files'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            CF['Files'] = 0
        CF['Templates'] = 7
        sql = '''select count(*) from Event where CATEGORY  = '%s' ''' % CATEGORY
        try:
            CF['Lastupload'] = int(self.client.command(sql)[0].oRecordData['count'])  
        except:
            CF['Lastupload'] = 0
        tile_stats['CF'] = CF
        
        DET = {}
        sql = ''' select count(*) from Event where E_DESC like '%EXTRACTION_CORE_VOICEOFCUSTOMER%', 'EXTRACTION_CORE_VOICEOFCUSTOMER') '''
        try:
            DET['Sentiment'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            DET['Sentiment'] = 0
        sql = ''' select count(*) from Event where E_DESC like '%EXTRACTION_CORE_VOICEOFCUSTOMER%', 'EXTRACTION_CORE_PUBLIC_SECTOR') '''
        try:
            DET['POLE'] = int(self.client.command(sql)[0].oRecordData['count'])  
        except:
            DET['POLE'] = 0
        sql = ''' select count(*) from Event where E_DESC like '%EXTRACTION_CORE_VOICEOFCUSTOMER%', 'LINGANALYSIS_FULL') '''
        try:
            DET['Linguistic'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            DET['Linguistic'] = 0
        tile_stats['DET'] = DET
        
        AE = {}
        CATEGORY = 'PUBLISHED_INTEL'
        sql = '''select count(*) from Event where CATEGORY  = '%s' ''' % CATEGORY
        try:
            AE['New'] = int(self.client.command(sql)[0].oRecordData['count']) 
        except:
            AE['New'] = 0
        sql = '''select count(*) from Event where CATEGORY  = '%s' ''' % CATEGORY
        try:
            AE['Merged'] = int(self.client.command(sql)[0].oRecordData['count'])
        except:
            sql = '''select count(*) from Event where CATEGORY  = '%s' ''' % CATEGORY
        try:
            AE['Relations'] = int(self.client.command(sql)[0].oRecordData['count'])         
        except:
            AE['Relations'] = 0
        tile_stats['AE'] = AE
        VP = {}
        VP['Children'] = 0
        VP['Adults']   = 5
        VP['Total']    = VP['Children'] + VP['Adults'] 
        tile_stats['VP'] = VP
        
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-tileStats]: %s." % (TS, tile_stats))           
        
        return tile_stats      
        
    def menuFill(self, uaa):
        
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
                'VULADULT'  : []
                }
        
        # Timestamp the process
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: Process started..." % (TS))  
        
        menu['USERS']    = self.get_users()
        menu['VULCHILD'] = self.Graph_VP_CHILDREN(1, 7)
        menu['VULADULT'] = self.Graph_VP_CHILDREN(1, 5)
        menu['VulChildCount'] = len(menu['VULCHILD'])
        menu['TARUNS']   = self.get_ta_runs()

        sql = '''
        select GUID, ORIGIN, XCOORD, YCOORD, LOGSOURCE from Location order by ORIGIN
        '''
        r = self.client.command(sql)
        for e in r:
            p = {}
            e = e.oRecordData
            p['GUID']   = e['GUID']
            p['NAME']   = e['ORIGIN']
            p['XCOORD'] = e['XCOORD']
            p['YCOORD'] = e['YCOORD']
            if e['LOGSOURCE'] in uaa:
                menu['LOCATIONS'].append(p) 
        menu['LOCATIONS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['LOCATIONS'] = sorted(menu['LOCATIONS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Locations loaded" % (TS, len(menu['LOCATIONS']))) 
        
        sql = ''' select GUID, CATEGORY, ORIGIN, O_DESC, CLASS2, LOGSOURCE from Object where TYPE = 'PIR' order by CATEGORY '''
        r = self.client.command(sql)
        for e in r:
            e = e.oRecordData
            d = {}
            d['GUID']     = e['GUID']
            d['CATEGORY'] = e['CATEGORY']
            d['ORIGIN']   = e['ORIGIN'] 
            d['NAME']     = "%s %s" % (d['CATEGORY'], e['O_DESC'])
            d['CLASS2']   = e['CLASS2'] 
            print(e)
            if str(e['LOGSOURCE']) in uaa:           
                menu['PIR'].append(d) 
        menu['PIR'].append({'NAME' : '0', 'GUID' : 0})
        menu['PIR'] = sorted(menu['PIR'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d PIR loaded" % (TS, len(menu['PIR'])))   
        
        sql = '''select GUID, CATEGORY, O_CLASS1, O_DESC, LOGSOURCE  from Object WHERE TYPE = 'STRAT' ORDER BY O_DESC '''
        r = self.client.command(sql)
        for e in r:
            d = {}
            e = e.oRecordData
            d['GUID']     = e['GUID']
            d['CATEGORY'] = e['CATEGORY']
            d['CLASS1']   = e['O_CLASS1']
            d['NAME']     = "%s: %s" % (e['CATEGORY'], e['O_DESC'])   
            if e['LOGSOURCE'] in uaa:  
                menu['STR'].append(d) 
        menu['STR'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['STR'] = sorted(menu['STR'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d STRAT loaded" % (TS, len(menu['STR'])))  
        
        sql = '''select FNAME, LNAME, GUID, LOGSOURCE FROM Person '''
        r = self.client.command(sql)
        for e in r:
            d = {}
            e = e.oRecordData
            d['NAME'] = e['FNAME'] + ' ' + e['LNAME']
            d['GUID'] = e['GUID'] 
            if e['LOGSOURCE'] in uaa:
                menu['PERSONS'].append(d)
        menu['PERSONS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['PERSONS'] = sorted(menu['PERSONS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Persons loaded" % (TS, len(menu['PERSONS']))) 
        
        sql = '''select CATEGORY, GUID, O_CLASS1, O_CLASS2, O_CLASS3, TYPE, O_DESC, LOGSOURCE from Object WHERE TYPE != 'User' order by O_DESC '''
        r = self.client.command(sql)
        for e in r:
            d = {}
            e = e.oRecordData
            class1 = str(e['O_CLASS1'])[:10]
            d['NAME'] = e['CATEGORY'] + ' ' + e['TYPE'] 
            d['DESC'] = class1 + ' ' + e['O_DESC'] + ' ' + e['O_CLASS3']     
            d['GUID'] = e['GUID']
            if e['LOGSOURCE'] in uaa:
                menu['OBJECTS'].append(d) 
        menu['OBJECTS'].append({'NAME' : '_NA_', 'GUID' : 0})
        menu['OBJECTS'] = sorted(menu['OBJECTS'], key=lambda i: i['NAME'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Objects loaded" % (TS, len(menu['OBJECTS'])))    
        
        sql = '''select DTG, GUID, E_DESC, LOGSOURCE from Event order by DTG DESC '''
        r = self.client.command(sql)
        for e in r:
            d = {}
            e = e.oRecordData
            d['NAME'] = e['DTG']
            d['GUID'] = e['GUID'] 
            d['DESC'] = e['E_DESC']
            if e['LOGSOURCE'] in uaa:
                menu['EVENTS'].append(d) 
        menu['EVENTS'].append({'NAME' : 00000, 'GUID' : 0})
        menu['EVENTS'] = sorted(menu['EVENTS'], key=lambda i: i['NAME'])
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Events loaded" % (TS, len(menu['EVENTS']))) 
        
        sql = '''select GUID, CATEGORY, E_DESC, CLASS1, DATE, DTG, ORIGIN, XCOORD, YCOORD, LOGSOURCE from Event where CATEGORY = 'Task'  '''
        r = self.client.command(sql)
        for e in r:
            print(e)
            d = {}
            e = e.oRecordData
            d['GUID']   = e['GUID']
            d['NAME']   = e['CATEGORY'] 
            d['DESC']   = str(e['E_DESC'])
            d['CLASS1'] = e['CLASS1']
            d['DATE']   = datetime.strptime(e['DATE'], '%Y-%m-%d').strftime('%d %b %Y')
            d['DTG']    = int(e['DTG'])
            d['STATUS'] = e['ORIGIN']
            d['FROM']   = e['XCOORD']
            d['TO']     = e['YCOORD']
            if e['LOGSOURCE'] in uaa:
                menu['TASKS'].append(d) 
        menu['TASKS'] = sorted(menu['TASKS'], key=lambda i: i['DTG'], reverse=True)
        menu['Tprofile'].append(d)
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Tasks loaded" % (TS, len(menu['TASKS'])))         
            
        sql = ''' select @class from E '''
        r = self.client.command(sql)
        for e in r:
            d = {}
            e = e.oRecordData
            d['RELTYP'] = e['class']
            if d not in menu['RELS']:
                menu['RELS'].append(d) 
        menu['RELS'] = sorted(menu['RELS'], key=lambda i: i['RELTYP'].lower())
        if self.Verbose == True:
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_ODB-menuFill]: %d Relation types loaded" % (TS, len(menu['RELS'])))         
        
        return menu
    
    def Graph_VP_CHILDREN(self, staPath, endPath):
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        if self.Verbose == True:
            print("[%s_ODB-Graph_VP_CHILDREN]: process started." % (TS))      
        
        # Set the children age date for the query then set the chaining length of degrees of separation for the pattern
        dob18 = (datetime.now() - relativedelta(years=18)).strftime('%Y-%m-%d')
        both = 'both()'
        if endPath > 1:
            i = 1
            while i < endPath:
                both = both + '.both()'
                i+=1
                
        sql = '''match {class: Person, as: p, where: (DOB > '%s')}.%s {class: Event, as: e, where: (TYPE = 'Crime')} 
        return p.FNAME, p.LNAME, p.DOB, p.GUID, e.GUID, e.CATEGORY, e.DATE, e.E_DESC, e.TYPE
        ''' % (dob18, both)
                
        Q = self.client.command(sql)
        results = []
        firstrn = True
        for e in Q:
            e = e.oRecordData
            r = {}
            r['GUID']     = e['p_GUID']
            if firstrn == True:
                r['FNAME']    = e['p_FNAME']
                r['LNAME']    = e['p_LNAME']
                r['NAME']     = "%s %s" % (r['FNAME'], r['LNAME']) 
                r['DOB']      = e['p_DOB']
                r['RISKS']    = []
                R = {}
                R['DESC']     = str(e['e_E_DESC'])
                R['TYPE']     = e['e_TYPE']
                R['CATEGORY'] = e['e_CATEGORY']
                R['DATE']     = e['e_DATE']
                R['E_GUID']   = e['e_GUID']
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
                        R['DESC']     = str(e['e_E_DESC'])
                        R['TYPE']     = e['e_TYPE']
                        R['CATEGORY'] = e['e_CATEGORY']
                        R['DATE']     = e['e_DATE']
                        R['E_GUID']   = e['e_GUID']                     
                        results[i]['RISKS'].append(R)
                        results[i]['VPSCORE'] = len(results[i]['RISKS'])
                        break
                    else:
                        i+=1
                        
                if found == False:

                    r['FNAME']    = e['p_FNAME']
                    r['LNAME']    = e['p_LNAME']
                    r['NAME']     = "%s %s" % (r['FNAME'], r['LNAME'])              
                    r['RISKS']    = []
                    R = {}
                    R['DESC']     = str(e['e_E_DESC'])
                    R['TYPE']     = e['e_TYPE']
                    R['CATEGORY'] = e['e_CATEGORY']
                    R['DATE']     = e['e_DATE']
                    R['E_GUID']   = e['e_GUID']
                    r['RISKS'].append(R)
                    r['VPSCORE']  = len(r['RISKS'])
                    results.append(r)
                    
        results = sorted(results, key=lambda i: i['VPSCORE'], reverse=False)              
        return results    


#OM = OrientModel()
#OM.initialize_reset()
#OM.openDB('POLER')
#OM.tileStats()
#OM.initialize_reset()
#GUID = 21529578459757368
#TYPE = 'Person'
#t = OM.get_entity(GUID, TYPE)
#j = OM.get_entity_relations(GUID)
#OM.get_users()
#OM.Graph_VP_CHILDREN(1, 10)
#OM.menuFill('A1B1C1A2B2C2A3B3C3')
#OM.merge_entities('person', 21529578459757368, 21529578459757368)
