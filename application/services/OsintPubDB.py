# -*- coding: utf-8 -*-
import time, os, json, requests, uuid, bs4
import pandas as pd
import _locale
from io import BytesIO

        
from zipfile import ZipFile

# Ensure printing to screen is formatted correctly
_locale._getdefaultlocale = (lambda *args: ['en_US', 'utf8'])
CellLimit = 32760

class OsintPubDB():
    def __init__(self, DB):
        
        # Base URL for all API calls
        self.DBType = None
        self.Data = None
        self.Path = None
        self.DocType = None
        self.DB = DB
        self.acled_api_url = "https://api.acleddata.com/acled/read"
        
        # setup search
        self.timestamp = time.strftime('%Y-%b-%d_%H%M')
        
        # to run 
        self.autoETL = True
    
    def setProcessID(self, eGUID):
        self.processID = eGUID

    def createACLEDFolder(self):
        '''
        Create a folder for the ACLED data to be stored within a common data folder
                Most data analysis can be carried out using the standard Excel file. In this file, both Actor 1 and Actor 2 appear in the same row, with 
        each event constituting a single unit of analysis. However, in order to analyse conflict actors and actor types, a monadic file is more useful.
        This is a file in which Actor 1 and Actor 2 appear in a single column, with each actor’s activity constituting a single unit of analysis. This
        allows users to analyse different trends and patterns, like the proportion of events in which a particular actor or actor type is involved or 
        the geographic patterns of activity of specific actors. Creating a monadic file involves duplicating the events so that each actor is 
        represented as participating in a single event (almost doubling the number of events in the dataset). For this reason, monadic files are not
        useful for analysis of the number of events or overall patterns of violence in a country, etc. They should be used for analysis of actors, 
        actor types and patterns in their activity. The dyadic actor file allows analysis on specific actors within the dataset. Actor 1 and Actor 2 
        are each assigned a unique actor ID and the actor dyad column represents the two actors involved in each event. This allows users to analyse 
        the number of events; number of fatalities; type of event; or geographic location of events in which two discrete actors interact, for example,
        events involving Boko Haram and the Military Forces of Nigeria.

        
        '''        
        acledRealTime = 'http://www.acleddata.com/wp-content/uploads/2017/09/ACLED-All-Africa-File_20170101-to-20170923_csv.zip'
        acledActorDyad = 'http://www.acleddata.com/wp-content/uploads/2017/02/ACLED-Version-7-All-Africa-1997-2016_actordyad_csv.zip'
        acled7Standard = 'http://www.acleddata.com/wp-content/uploads/2017/01/ACLED-Version-7-All-Africa-1997-2016_csv_dyadic-file.zip'
        acled7Monadic = 'http://www.acleddata.com/wp-content/uploads/2017/01/ACLED-Version-7-All-Africa-1997-2016_monadic-file_csv.zip'
        
        # A list of the sources which can be adjusted more easily
        ACLED = [acledRealTime, acledActorDyad, acled7Standard, acled7Monadic]        
        # Cycle through the zips and extract the contents into data/ACLED

                    
    def processURL(self, url):
        '''
        IN: 
            - url
        OUT: 
            - dict with text separated by cell length with overspill in txt2 and links in an array
        '''
        processedURL = {'text' : '', 'txt2' : '', 'links' : []}
        try:
            paras = bs4.BeautifulSoup(requests.get(url).text).findAll('p')
            if len(paras) > 1:
                for p in paras:
                    processedURL['text'] = ('%s%s' % (processedURL['text'], p.get_text())).replace("[", "").replace("]", "").replace("'", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
            
            if len(p) + len(processedURL['text']) > csvCellLimit:
                print("[*] Max CSV cell length reached with %s" % url)
                processedURL['txt2'] = processedURL['text'][(len(processedURL['text'])-CellLimit):]
                processedURL['text'] = processedURL['text'][:CellLimit]
 
            links = bs4.BeautifulSoup(requests.get(url).text).findAll('a')
            failed = 0
            if len(links) > 1:
                for l in links:
                    try:
                        processedURL['links'].append(l['href'])
                    except:
                        failed+=1
            #print("[*] %d links found." % (len(links) - failed))
        except:
            return None

        return processedURL
        
    
    def ETLSocial2Graph(self, Data):
        
        '''
        Form row objects based on existance of ORIGINREF variables
        Rules: ContactSource, Age, Children, Income

        '''
        percent = -1
        rows = int(Data.shape[0])
        cols = []
        for c in list(Data):
            cols.append(c.replace(" ", ""))
        Data.columns = cols

        # Data Cleaning
        Data['Lat']                    = Data['Lat'].fillna(0.00)
        Data['Long']                   = Data['Long'].fillna(0.00)
        Data['ConMethod']              = Data['ConMethod'].fillna('Unk')
        Data['ConReason']              = Data['ConReason'].fillna('Unk')
        Data['RefLinkedDate']          = Data['RefLinkedDate'].fillna('NoDate')
        Data['ReferralID']             = Data['ReferralID']
        Data['CODE']                   = Data['CODE'].fillna('NOCODE')
        Data['RouteOfAccessToService'] = Data['RouteOfAccessToService'].fillna('NoRoute')
        Data['SequelToRequest']        = Data['SequelToRequest'].fillna('NONE')
        Data['SequelToRequestDate']    = Data['SequelToRequestDate'].fillna('NoDate')
        Data['ContactSource']          = Data['ContactSource'].fillna('Unk')
        Data['RAPCode']                = Data['RAPCode'].fillna('NOCODE')
        Data['RAPConSource']           = Data['RAPConSource'].fillna('NOSOURCE')
        Data['RAPOrder']               = Data['RAPOrder'].fillna(0)
        Data['StaffID']                = Data['StaffID'].fillna('Unk')
        Data['RecordedBy']             = Data['RecordedBy'].fillna('Unk')
        Data['Department']             = Data['Department'].fillna('Unk')
        Data['ContactOutcome']         = Data['ContactOutcome'].fillna('Unk')
        Data['Postcode']               = Data['Postcode'].fillna('Unk')
        Data['MOSAICType']             = Data['MOSAICType'].fillna('Unk')
        Data['Age']                    = Data['Age'].fillna('Unk')
        Data['Income']                 = Data['Income'].fillna('Unk')
        Data['HHComposition']          = Data['HHComposition'].fillna('Unk')
        Data['Children']               = Data['Children'].fillna('Unk')
        Data['Tenure']                 = Data['Tenure'].fillna('Unk')
        Data['Type']                   = Data['Type'].fillna('Unk')
        Data['Easting']                = Data['Easting'].fillna('Unk')
        Data['Northing']               = Data['Northing'].fillna('Unk')
        Data['ContactNarrative']       = Data['ContactNarrative'].fillna('Na')
        Data['AssmntDate']             = Data['AssmntDate'].fillna('Unk')
        Data['First Name']             = Data['FirstName'].fillna('Unk')
        Data['Last Name']              = Data['LastName'].fillna('Unk')

        # Fill for unused entity attributes
        uLOGSOURCE = 'B1'
        uUNK = 'U'
        LANG = 'en'
        uDOB = '2000-01-01'
        
        # Start Rules
        ContactSources = []
        ContactSourceN1 = ['Home Care', 'Private Residential Care Home', 'Other Social Care Team']
        ContactSources.append(ContactSourceN1)          
        ContactSource0 = ['Safeguarding Mental Health - Community Mental Health', 'Emergency Duty Team',
                              'Safeguarding Mental Health - Inpatient', 'Safeguarding Mental Health - Substance and Alcohol Misuse',
                                  'Safeguarding Primary Health - SWYPFT General Community', 'Safeguarding Primary Health - SWYPFT General Inpatient'
                              ]
        ContactSources.append(ContactSource0)        
        ContactSource1 = ['Safeguarding Mental Health - Community Mental Health', 'Emergency Duty Team',
                              'Safeguarding Mental Health - Inpatient', 'Safeguarding Mental Health - Substance and Alcohol Misuse',
                                  'Safeguarding Primary Health - SWYPFT General Community', 'Safeguarding Primary Health - SWYPFT General Inpatient'
                              ]
        ContactSources.append(ContactSource1)        
        ContactSource2 = ['Safeguarding Mental Health - Community Mental Health', 'Emergency Duty Team',
                              'Safeguarding Mental Health - Inpatient', 'Safeguarding Mental Health - Substance and Alcohol Misuse',
                          'Safeguarding Primary Health - SWYPFT General Community', 'Safeguarding Primary Health - SWYPFT General Inpatient'
                          ]
        ContactSources.append(ContactSource2)
        ContactSource3 = ['Safeguarding Secondary Health - Barnsley Hospital', 'Hospice', 'Hospital', 'Hospital in Borough', 'Other Health',
                              'School', 'Other Education' 
                           ]
        ContactSources.append(ContactSource3)
        ContactSource4 = ['Agency', 'Care Agency', 'Care Home', 'Carer', 'Home Care', 'Intermediate Care', 'Private Home Care Agency',
                              'Private Nursing Care Home', 'Private Residential Care Home', 'Private Residential Home', 'Community Nurse',
                           'Community Psychiatric Nurse', 'Consultant Psychiatrist', 'General Practitioner', 'Ambulance Service - Primary Care',
                           'Mental Health Worker'
                           ]
        ContactSources.append(ContactSource4)
        ContactSource5 = ['Voluntary Organisation', 'Advocate', 'Councillor', 'Elsewhere Within Department', 'Local Authority Housing',
                              'Other Local Authority', 'Other Social Care Team', 'Other SSD'
                           ]
        ContactSources.append(ContactSource5)
        ContactSource6 = ['Friend Or Neighbour', 'In Person', 'Self', 'Neighbour/Friend', 'Relative', 'Police']
        ContactSources.append(ContactSource6)
    
        Ages = []
        AgesN1 = ['31-35']
        Ages.append(AgesN1)
        Ages0 = ['41-45']
        Ages.append(Ages0) 
        Ages1 = ['26-30']
        Ages.append(Ages1)          
        Ages2 = ['18-25']
        Ages.append(Ages2)
        Ages3 = ['71-75', '76-80', '86-90']
        Ages.append(Ages3)
    
        Children3 = ['3', '4+']        
        Data['VP_RISK'] = 0
        Data['VP_TIME'] = 0
        Data = Data.sort_values(by=['PersonID', 'ContactDate'])
        st = time.time()
        LastPersonID = 0
        for index, row in Data.iterrows():
            
            CurrentPersonID = row['PersonID']
            if CurrentPersonID == LastPersonID:
                VP_TIME = VP_TIME + 1
                VP_RISK = 0
            else:
                LastPersonID = CurrentPersonID
                VP_TIME = 0
                VP_RISK = 0
            # Police Rule
            if row['ContactSource'] == 'Police':
                    VP_RISK = row['VP_RISK'] + 2
                    VP_TIME = row['VP_TIME'] + 1
                    Data.set_value(index, 'VP_RISK', VP_RISK)
                    Data.set_value(index, 'VP_TIME', VP_TIME)
            
            # Contact Source Rule    
            i = -1    
            for CSList in ContactSources:
                for CS in CSList:
                    if row['ContactSource'] == CS:
                            VP_RISK = VP_RISK + i
                            VP_TIME = VP_TIME + 1
                            Data.set_value(index, 'VP_RISK', VP_RISK)
                            Data.set_value(index, 'VP_TIME', VP_TIME)                            
                            break
                i+=1
            # Age Rule
            i = -1
            for Age in Ages:
                if row['Age'] == CS:
                    VP_RISK = VP_RISK + i
                    VP_TIME = VP_TIME + 1
                    Data.set_value(index, 'VP_RISK', VP_RISK)                          
                    break
                i+=1
            # Children Rule
            i = 2
            for children in Children3:
                if row['Children'] == children:
                    VP_RISK = VP_RISK + i
                    Data.set_value(index, 'VP_RISK', VP_RISK)                         
                    break                   
            # Income Rule  
            if '70-99' in row['Income']:
                VP_RISK = VP_RISK - 1
                Data.set_value(index, 'VP_RISK', VP_RISK)
            if '100-149' in row['Income']:
                VP_RISK = VP_RISK - 2
                Data.set_value(index, 'VP_RISK', VP_RISK)                
            if '15' in row['Income']:
                VP_RISK = VP_RISK + i
                Data.set_value(index, 'VP_RISK', VP_RISK)   
            # MOSAIC Rule
            if row['MOSAICType'][:1] == 'K':
                VP_RISK = VP_RISK - 1
                Data.set_value(index, 'VP_RISK', VP_RISK)   
            if row['MOSAICType'][:3] == 'M56' or row['MOSAICType'][:3] == 'A03' or row['MOSAICType'][:3] == 'B09' or row['MOSAICType'][:3] == 'B07':
                VP_RISK = VP_RISK - 2
                Data.set_value(index, 'VP_RISK', VP_RISK)             
            if row['MOSAICType'][:1] == 'L':
                VP_RISK = VP_RISK + 1
                Data.set_value(index, 'VP_RISK', VP_RISK) 
            if row['MOSAICType'][:3] == 'N58' or row['MOSAICType'][:3] == 'N60' or row['MOSAICType'][:3] == 'N62':
                VP_RISK = VP_RISK + i
                Data.set_value(index, 'VP_RISK', VP_RISK)              
                                         
            LANG = 'en'
            
            # Reported Location
            TYPE      = 'Social Services Clinic'
            DESC      = 'Location associated with %s' % row['Department']
            XCOORD    = row['Lat']
            YCOORD    = row['Long']
            ZCOORD    = row['RAPCode']
            CLASS1    = row['Postcode']
            ORIGIN    = row['ContactID']
            ORIGINREF = "%s%s%s%s%s" % (row['Lat'], row['Long'], row['Department'], Data['StaffID'], Data['RecordedBy'])
            LOGSOURCE = uLOGSOURCE
            lGUID = self.DB.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
    
            # Contact Event            
            TYPE      = 'ContactReport'
            CATEGORY  = row['ConMethod']
            DESC      = "%s : %s\n%s\nReason: %s" % (row['ContactID'], row['ContactDate'], row['ContactNarrative'], row['ConReason'])
            eDESC      = 'VP %d: %s' % (VP_RISK, DESC)
            CLASS1    = row['Status']
            TIME      = row['RefLinkedDate']
            DATE      = row['ContactDate']
            ORIGIN    = row['CODE']
            ORIGINREF = "HealthRecord%s" % row['ContactID']
            LOGSOURCE = uLOGSOURCE
            DTG = ("%s%s" % (DATE, TIME)).replace("-", "").replace(":", "").replace(" ", "").replace("/", "")
            eGUID = self.DB.insertEvent(TYPE, CATEGORY, eDESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
            
            # Linked Event
            TYPE      = row['RouteOfAccessToService']
            CATEGORY  = row['SequelToRequest']
            DESC      = 'Referral %s\nRequest from %s' % (row['ReferralID'], row['ContactID'])
            CLASS1    = row['Status']
            TIME      = row['RefLinkedDate']
            DATE      = row['SequelToRequestDate']
            ORIGIN    = row['CODE']
            ORIGINREF = "HealthRecord%s" % row['ReferralID']
            LOGSOURCE = uLOGSOURCE
            e2GUID = self.DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
            
            # Form Object
            TYPE      = 'Form'
            CATEGORY  = 'ContactReport'
            DESC      = row['RAPConSource']
            DESC      = "'%s'" % DESC
            CLASS1    = row['ContactSource']
            CLASS2    = row['RAPCode']
            CLASS3    = row['RAPOrder']
            ORIGIN    = row['ContactID']
            ORIGINREF = "HealthRecord%s%s" % (row['FormNo'], row['ContactID'])
            LOGSOURCE = uLOGSOURCE
            oGUID = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            
            # Case Person
            GEN       = uUNK
            FNAME     = row['FirstName']
            LNAME     = row['LastName']
            DOB       = row['Age']
            POB       = row['Tenure']
            ORIGIN    = 'VP %d' % (VP_RISK)
            ORIGINREF = "%s%s%s" % (row['PersonID'], row['FirstName'], row['LastName'])
            LOGSOURCE = uLOGSOURCE
            DESC = eDESC
            pGUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE, DESC)
            
            # Case Recorder
            GEN       = uUNK
            spc    = row['RecordedBy'].find(' ')
            if spc == None:
                FNAME     = row['RecordedBy']
                LNAME     = row['RecordedBy']
            else:
                FNAME     = row['RecordedBy'][:spc].strip()
                LNAME     = row['RecordedBy'][spc:].strip()                
            DOB       = uDOB
            POB       = uUNK
            ORIGIN    = row['Department']
            ORIGINREF = "HealthRecord%s%s%s" % (row['StaffID'], FNAME, LNAME)
            LOGSOURCE = uLOGSOURCE
            p2GUID = self.DB.insertPerson(GEN, FNAME, LNAME, DOB, POB, ORIGIN, ORIGINREF, LOGSOURCE, DESC)
            
            # Risk Score
            rGUID = self.DB.insertObject("RiskScore", "Vulnerable Adult", str(VP_RISK), VP_RISK, 'VPA', 'Barnsley', 'B1', 'RiskScoreVPA%s' % str(VP_RISK), 'B1')
            
            # Create the relations
            self.DB.insertRelation(eGUID, 'Event', 'SubjectofContact', pGUID, 'Person')
            self.DB.insertRelation(eGUID, 'Event', 'RecordedBy', p2GUID, 'Person')
            self.DB.insertRelation(eGUID, 'Event', 'DocumentedIn', oGUID, 'Object')
            self.DB.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')
            self.DB.insertRelation(eGUID, 'Event', 'ReferenceLink', e2GUID, 'Event')
            self.DB.insertRelation(oGUID, 'Object', 'DocumentMentioning', pGUID, 'Person')
            self.DB.insertRelation(oGUID, 'Object', 'DocumentedBy', p2GUID, 'Person')
            self.DB.insertRelation(pGUID, 'Person', 'HasStatus', rGUID, 'Object')
            
            percent_old = percent
            percent = round(index/rows*100)
            
            if(percent % 1 == 0 and percent != percent_old):
                
                p = float(float(index)/float(rows))*100             
         
        Data.to_csv('DemoDataAfterRules.csv')   


    def ETLGTD2Graph(self, gtdData):
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
        rows = int(gtdData.shape[0])
        gtdData['latitude'] = gtdData['latitude'].fillna(0.00)
        gtdData['longitude'] = gtdData['longitude'].fillna(0.00)
        
        for index, gtd in gtdData.iterrows():
            
            TYPE = 'ConflictEvent'
            CATEGORY = gtd['event_type']
            LOGSOURCE = str(gtd['latitude']) + '-' + str(gtd['longitude'])
            DESC = '%s %s at %s' % (CATEGORY, str(gtd['notes']), LOGSOURCE)
            DESC = '%s' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            try:
                CLASS1 = gtd['nkill'] + gtd['nwound']
            except:
                CLASS1 = 0
            TIME = '12:00:00'
            DATE = gtd['event_date']
            DTG = int('%s%s' % (DATE.replace("/", "").replace("-", ""), TIME.replace(":", "")))
            ORIGIN = int(gtd['eventid'])
            ORIGINREF = 'GTD-%s-%s' % (gtd['eventid'], LOGSOURCE)
            eGUID = self.DB.insertEvent(TYPE, CATEGORY, DESC, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', eGUID, 'Event')
            
            TYPE = 'Organization'
            CATEGORY = 'ConflictActor'
            DESC = '%s' % gtd['actor1']
            DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS1 = str(gtd['admin1']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = str(gtd['weaptype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS3 = gtd['year']
            ORIGIN = str(gtd['country']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            ORIGIN = '"%s"' %  ORIGIN.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            ORIGINREF = "GTD-%s" % DESC
            LOGSOURCE = 'OsintPubDB'
            oGUID1 = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', oGUID1, 'Object')
            
            DESC = '"%s"' % gtd['actor2']
            DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS1 = str(gtd['targtype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            CLASS2 = str(gtd['targsubtype1_txt']).replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            ORIGINREF = "GTD-%s" % DESC
            oGUID2 = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', oGUID2, 'Object')
            
            TYPE = 'ConflictLocation'
            DESC = gtd['region_txt'] + ':' + DESC
            DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            XCOORD = gtd['latitude'] 
            YCOORD = gtd['longitude']
            ZCOORD = 'Zcoord'
            lGUID = self.DB.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', lGUID, 'Location')
            
            self.DB.insertRelation(oGUID1, 'Object', 'InvolvedIn', eGUID, 'Event')
            self.DB.insertRelation(oGUID2, 'Object', 'InvolvedIn', eGUID, 'Event') 
            
            self.DB.insertRelation(eGUID, 'Event', 'OccurredAt' , lGUID, 'Location')
            self.DB.insertRelation(oGUID1, 'Object', 'ReportedAt' , lGUID, 'Location')
            self.DB.insertRelation(oGUID2, 'Object', 'ReportedAt' , lGUID, 'Location')
            
            if rows > 1000 and int(index) > 0:
                p = float(float(index)/float(rows))*100
                print("[*] ROW %d: %f complete" % (int(index), p))
        
        return
    
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
        
        # Set up the Event
        if isinstance(acledData, pd.DataFrame):
            
            for index, acled in acledData.iterrows():
                
                if acled['event_date'] != '':
                    DATE = acled['event_date']
                    TYPE = "ConflictEvent"
                    if acled['event_type'] != '':
                        CATEGORY = acled['event_type']
                    else:
                        CATEGORY = 'ACLED'
                        
                    if acled['notes'] != '':
                        DESC = acled['notes'] + ' ' + acled['data_id']
                        DESC = '%s' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                    else:
                        DESC = acled['data_id']
                    
                    if acled['fatalities'] != '':
                        CLASS1 = acled['fatalities']
                    else:
                        CLASS1 = 0
                    
                    if acled['source'] != None:
                        ORIGIN = acled['source'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                    else:
                        ORIGIN = 'ACLEDdata'
                    TIME = '12:00:00'    
                    DTG = int('%s%s' % (str(DATE).replace("/", "").replace("-", ""), TIME.replace(":", "")))
                    ORIGINREF = 'ACLED-%s' % alced['data_id']
                    LOGSOURCE = '%s, %s' % (acled['latitude'], acled['longitude'])
                    if acled['latitude'] != '':
                        XCOORD = float(acled['latitude'])
                        YCOORD = float(acled['longitude'])  
                    else:
                        XCOORD = 0.0
                        YCOORD = 0.0
                        
                    eGUID = self.DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE)
                    newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', eGUID, 'Event')             
                    
                    # Set up the Location
                    if acled['latitude'] != '':
                        
                        TYPE = 'ConflictLocation'
                        DESC = '%s, %s, %s :%s' % (acled['country'], acled['admin1'], acled['location'], acled['notes'])
                        DESC = '"%s"' %  DESC.replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                        ZCOORD = 0
                        CLASS1 = acled['fatalities']
                        lGUID = self.DB.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
                        newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', eGUID, 'Event')
                        self.DB.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')
                
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
                    LOGSOURCE = '%s-%s' % (CLASS1, CLASS2)
                    oGUID = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE)
                    newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', oGUID, 'Object')
                
                    self.DB.insertRelation(oGUID, 'Object', 'InvolvedIn', eGUID, 'Event')
                    self.DB.insertRelation(oGUID, 'Object', 'OccurredAt', lGUID, 'Location')
                
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
                    LOGSOURCE = '%s-%s' % (CLASS1, CLASS2)
                    oGUID2 = self.DB.insertObject(TYPE, CATEGORY, DESC, CLASS1, CLASS2, CLASS3, ORIGIN, ORIGINREF, LOGSOURCE) 
                    newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', oGUID2, 'Object')
                
                    self.DB.insertRelation(oGUID2, 'Object', 'Involved', eGUID, 'Event')
                    self.DB.insertRelation(oGUID2, 'Object', 'OccurredAt', lGUID, 'Location')   
                
    
    def ETLInfoEx2Graph(self, Data):
        rows = int(Data.shape[0])
        
        objMap = {'TYPE' : 'Objective', 'CATEGORY' : 'IW'}
        Obj = {}
        Obj['ID'] = ''       
        Obj['DESC'] = ''
        Obj['CLAS']
        
    
    
    
    def ETLPolice2Graph(self, Data):
    
        rows = int(Data.shape[0])
    
        # Data Cleaning
        Data['CrimeID']             = Data['CrimeID'].fillna('Unk')
        Data['Month']               = Data['Month'].fillna('Unk')
        Data['GoldenNominal']       = Data['GoldenNominal'].fillna('Unk')
        Data['Reportedby']          = Data['Reportedby'].fillna('Unk')
        Data['Fallswithin']         = Data['Fallswithin'].fillna('Unk')
        Data['Longitude']           = Data['Longitude'].fillna(0.00)
        Data['Latitude']            = Data['Latitude'].fillna(0.00)
        Data['Location']            = Data['Location'].fillna('Unk')
        Data['LSOAcode']            = Data['LSOAcode'].fillna('Unk')    
        Data['LSOAname']            = Data['LSOAname'].fillna('Unk')
        Data['CrimeTypeCode']       = Data['CrimeTypeCode'].fillna('Unk')
        Data['Crimetype']           = Data['Crimetype'].fillna('Unk')
        Data['Lastoutcomecategory'] = Data['Lastoutcomecategory'].fillna('Unk')        
        Data['Context']             = Data['Context'].fillna('Unk')   
        
        uLOGSOURCE = 'PoliceLSOA'
        uUNK = 'Unk'
        st = time.time() 
        for index, row in Data.iterrows():
            
            if row['CrimeID'] == 'Unk':
                ORIGINREF = str(uuid.uuid4())
            else:
                ORIGINREF = row['CrimeID']
            
            if row['Month'] == 'Unk':
                DATE = '1900-01-01'
            else:
                DATE = row['Month']
            
            TYPE      = 'ReportedCrime'
            CATEGORY  = row['Crimetype']
            DESC      = '%s %s' % (row['Context'], row['Lastoutcomecategory'])
            CLASS1    = row['GoldenNominal']
            TIME      = '12:00:00'
            ORIGIN    = row['Reportedby']
            LOGSOURCE = 'PoliceRecord'
            DTG = int('%s%s' % (DATE.replace("/", "").replace("-", ""), TIME.replace(":", "")))
            XCOORD = row['Longitude']
            YCOORD = row['Latitude'] 
            LANG = 'en'
     
            eGUID = self.DB.insertEvent(TYPE, CATEGORY, DESC, LANG, CLASS1, TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGINREF, LOGSOURCE) 
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', eGUID, 'Event')
  
            DESC = row['Fallswithin']
            ZCOORD = row['LSOAcode']
            CLASS1 = row['LSOAname']
            ORIGIN = row['Location']
            ORIGINREF = row['CrimeID']
            lGUID = self.DB.insertLocation(TYPE, DESC, XCOORD, YCOORD, ZCOORD, CLASS1, ORIGIN, ORIGINREF, LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', lGUID, 'Location')

            self.DB.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')
 
        return
    
    def ETLText2Graph(self, Data):
    
        rows = int(Data.shape[0])
    
        return    
    def ETLGDELT2Graph(self, Data):
       
        st = time.time() 
        percent = -1
        percent_old = 0
        rows = int(Data.shape[0])
        print("[*] %d records found." % rows)
        index = 0
        Data['TEXT'] = ""
        Data['TX2T'] = ""
        urls = Data['SOURCEURL'].unique().tolist()
        urlTexts = {}
        i = 1
        
        # First load all URL texts for cases where URLs are used more than once in the corpus
        for url in urls:
            processedURL = self.processURL(url)
            if processedURL != None:
                urlTexts[url] = processedURL['text']
            else:
                urlTexts[url] = 'No Text Available'
            percent_old = percent
            percent = round(i/len(urls)*100)
            if(percent % 1 == 0 and percent != percent_old):
                p = float(float(i)/float(len(urls)))*100
                print("[*] URL %d: %f in %f seconds" % (int(i), p, time.time() - st)) 
            i+=1
        
        # Then map all URL text to the sources
        percent = -1
        percent_old = 0        
        for index, row in Data.iterrows():
            if row['SOURCEURL'] in urlTexts:
                Data.set_value(index, 'TEXT', urlTexts[row['SOURCEURL']])
            percent_old = percent
            percent = round(index/rows*100)
            if(percent % 1 == 0 and percent != percent_old):
                p = float(float(index)/float(rows))*100
                print("[*] ROW %d: %f in %f seconds" % (int(index), p, time.time() - st))                    
        print("[*] Text and links extracted." % len(urls))
        Data.to_csv('GDET-%s.csv' % time.time(), index= False) 
    
    
    def ETLEvent(self, row):
        # Dictionary for entity look ups
        entity = {'TYPE': 'Event'}
        E_TYPE      = "Report"
        E_CATEGORY  = row['DocumentType']
        E_DESC      = row['ArabicText'].replace("'", "").replace('"', '')	
        E_LANG	    = 'Arabic'                   
        E_CLASS1    = row['DocumentPageCount']	
        E_TIME      = row['Time']
        E_DATE      = row['Date']
        E_DTG	    = ('%s%s' % (E_DATE, E_TIME)).replace("'", "").replace(":", "").replace("-", "")		
        E_XCOORD    = 0.0
        E_YCOORD    = 0.0	
        E_ORIGIN    = row['CollectionPlan']
        E_ORIGINREF = row['DocumentNumber']	
        E_LOGSOURCE = 'J2'
    
        entity['LOOKUP'] = E_DESC
        E_GUID, exists = self.DB.EntityResolve(entity)
        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % E_GUID)
        else:
            self.DB.insertEvent(E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, 
                             E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', E_GUID, 'Event')
        
            # Put in an english version of the report
            E_DESC = row['EnglishText'].replace("'", "").replace('"', '')	
            E_LANG = 'English'
            E_GUID_ENG = int(E_GUID) + 1
            self.DB.insertEvent(E_GUID_ENG, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, 
                                         E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE) 
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', E_GUID_ENG, 'Event')
            
            self.DB.insertRelation(self.DB.R_GUID, E_GUID, 'EVENT', 'Translation', E_GUID_ENG, 'EVENT')
        self.DB.CurEvent = E_GUID
        return exists
    
    def ETLPerson(self, pName, row):
        # Dictionary for entity look ups
        entity = {'TYPE': 'Person'}
        pNames = pName.split(' ')
        if len(pNames) > 2:
            i = 2
            while i < len(pNames):
                pNames[1] = pNames[1] + ' ' + pNames[i]
                i+=1
        else:
            pNames.append('Unk')
        P_GEN       = 'U'
        P_FNAME     = pNames[0]
        P_LNAME     = pNames[1]
        P_DOB       = '1900-01-01'
        P_POB       = 'POB'
        P_ORIGIN    = 'J2-HUMINT'
        P_ORIGINREF = ('%s%s' % (pNames[0], pNames[1])).replace(' ', '')
        P_LOGSOURCE = 'J2'
        
        entity['LOOKUP'] = P_ORIGINREF
        P_GUID, exists = self.DB.EntityResolve(entity)   
        
        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % P_GUID)
        else:
            self.DB.insertPerson(P_GUID, P_GEN, P_FNAME, P_LNAME, P_DOB, P_POB, 
                             P_ORIGIN, P_ORIGINREF, P_LOGSOURCE) 
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', P_GUID, 'Person')
        
        self.DB.CurPerson = P_GUID
        return exists
    
    def ETLLocation(self, lName, row):
        # Dictionary for entity look ups
        entity = {'TYPE': 'Location'}
        lNames = lName.split(' ')
        if len(lNames) > 1:
            i = 1
            while i < len(lNames):
                lNames[0] = lNames[0] + ' ' + lNames[i]
                i+=1 
        L_TYPE = 'Location'
        L_DESC = '%s' % lNames[0]
        entity['LOOKUP'] = L_DESC
        L_GUID, exists = self.DB.EntityResolve(entity)          

        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % L_GUID)
        else:
            L_XCOORD = 0.0
            L_YCOORD = 0.0
            L_ZCOORD = 0.0
            L_CLASS1 = 0
            L_ORIGIN = 'J2-HUMINT'
            L_ORIGINREF = None
            L_LOGSOURCE = 'J2'            
            self.DB.insertLocation(L_GUID, L_TYPE, L_DESC, L_XCOORD, L_YCOORD, L_ZCOORD, 
                                       L_CLASS1, L_ORIGIN, L_ORIGINREF, L_LOGSOURCE)  
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', L_GUID, 'Location')
        
        self.DB.CurLocation = L_GUID
    
    def ETLObject(self, oName, row):
        # Dictionary for entity look ups
        entity = {'TYPE': 'Object'}
        oNames = oName.split(' ')
        if len(oNames) > 1:
            i = 1
            while i < len(oNames):
                oNames[0] = oNames[0] + ' ' + oNames[i]
                i+=1  
        
        O_TYPE      = 'Organization'
        O_CATEGORY  = 'Reported'
        O_DESC      = oNames[0]
        O_CLASS1    = 1
        O_CLASS2    = 1
        O_CLASS3    = 1
        O_ORIGIN    = 'J2-HUMINT'
        O_ORIGINREF = 'Org : %s' % (oNames[0])
        O_LOGSOURCE = 'J2'     
        
        entity['LOOKUP'] = O_DESC
        O_GUID, exists = self.DB.EntityResolve(entity)          
        
        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % O_GUID)
        else:
            self.DB.insertObject(O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, 
                                O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE) 
            newRel = self.DB.insertRelation(self.processID, 'Event', 'FROM_FILE', O_GUID, 'Object')
        
        self.DB.CurObject = O_GUID
        return exists    
    
    def ETLReport2HANA(self):
        
        for index, row in self.Data.iterrows():
            if self.ETLEvent(row) == 0:
                if len(row['People']) > 1: 
                    # List comprehension to make the string of elements into a list
                    Names = [str(x) for x in row['People'].split(',') if x]
                    for pName in Names:
                        pName = pName.strip()
                        if len(pName) > 1:
                            self.ETLPerson(pName, row)
                            self.DB.insertRelation(self.DB.R_GUID, self.DB.CurEvent, 'EVENT', 'Mentioned', self.DB.CurPerson, 'PERSON')
                        
                    Places = [str(x) for x in row['Locations'].split(',') if x]
                    for lName in Places:
                        lName = lName.strip()
                        if len(lName) > 1:
                            self.ETLLocation(lName, row)
                            self.DB.insertRelation(self.DB.R_GUID, self.DB.CurEvent, 'EVENT', 'Mentioned', self.DB.CurLocation, 'LOCATION')  
                                
                    Orgs = [str(x) for x in row['Objects'].split(',') if x]
                    for oName in Orgs:
                        oName = oName.strip()
                        if len(oName) > 1:
                            self.ETLObject(oName, row)
                            self.DB.insertRelation(self.DB.R_GUID, self.DB.CurEvent, 'EVENT', 'Mentioned', self.DB.CurObject, 'OBJECT')  
                                    
    def getAcledData(self, eGUID):
        
        # Set the page at 1 for api pagination and nopages for exit. 307 estimated pages
        page = 300 # 303 throws cant map char code 302 error with baker
        nopages = 0
        self.setProcessID(eGUID)

        while nopages == 0:
            print("[*] ACLED API Page: %d" % page)
            url = self.acled_api_url + '?page=%s' % page
            response = requests.get(url)
            r = response.json()
            acledData = pd.DataFrame(r['data'])
            self.ETLACLED2Graph(acledData)
            page -=1
            
            
    def getAcledData_with(self, eGUID, searchdate, searchlocation):
        
        # Set the page at 1 for api pagination and nopages for exit. 307 estimated pages
        self.setProcessID(eGUID)
        searchdatestart = searchdate[:4]
        print(searchdate, searchlocation)
        if searchdate != '':
            url = '%s?year=%s' % (self.acled_api_url, searchdatestart)
        else:
            url = self.acled_api_url
            
        #check if the query had been made
        #if not make the query, if it was return that it was made
        response = requests.get(url)
        r = response.json()
        acledData = pd.DataFrame(r['data'])
    
        return acledData
    
    def getAcledDataThread(self, results, intelGUID):
        
        self.DB.ETLACLED2Graph(results)
        
    def getUcpdData_with(self, eGUID, startDate, endDate, countries, geography):
        
        # Set the page at 1 for api pagination and nopages for exit. 307 estimated pages
        url = "http://ucdpapi.pcr.uu.se/api/gedevents/17.2?pagesize=2000"
        if startDate != '':
            url = "%s&StartDate=%s" % (url, startDate)
        if endDate != '':
            url = "%s&EndDate=%s" % (url, endDate)
        if countries != '':
            url = "%s&Country=%s" % (url, countries)
        elif geography != '':
            url = "%s&Geography=%s" % (url, geography)   
        response = requests.get(url)
        r = response.json()
        results = r['Result']
        pageCount = r['TotalPages']
        
        return r
    
    
    def getUcpdDataThread(self, r, intelGUID):
        nextPage = r['NextPageUrl']
        results = r['Result']
        while nextPage != '':
            response = requests.get(nextPage)
            print(nextPage)
            r = response.json()
            results = results + r['Result']
            nextPage = r['NextPageUrl'] 
        
        self.DB.ETLUCDP2Graph(results, intelGUID)
           
    def POLERExtract(self, view):
        
        '''
        For each row  
           For each col in row.keys                check each column to see if it is part of an entity map
              For each entity in entities          cycle through each entity to compare keys to the row's current col
                 For each key in entity            for each key in the entity
                    if entity[key] == col          if the entity[key].value == the current col value
                       for nE in newEntities:      cycle through all the entities created so far
                          NewEntity = {}           if the current label is equal to the found entity label use this to map the new 
                          NewEntity['LABEL'] = e   else create a new entity based on the LABEL first letter
                          NE[key] = col.value
                        
        '''
        data = {'Persons' : [], 'Objects' : []}
        for index, row in view.iterrows(): 
            newE = []
            for c in row.keys():
                for e in self.vMap['entities']:
                    for k in e.keys():
                        # The keys match so find the entity in the row
                        if e[k].strip().lower() == c.strip().lower():
                            found = False
                            for nE in newE:
                                if found == False:
                                    if nE['LABEL'] == e['LABEL']:
                                        if e['LABEL'][0] == 'p':
                                            nE[k] = row[c] 
                                            found = True
                                        elif e['LABEL'][0] == 'o':
                                            nE[k] = row[c] 
                                            found = True
                                        elif e['LABEL'][0] == 'l':
                                            nE[k] = row[c] 
                                            found = True
                                        else:
                                            nE[k] = row[c]                                                                                   
                                            found = True
                            
                            # No existing entities so start with a new shell
                            if found == False:
                                if e['LABEL'][0] == 'p':
                                    mE = {'TYPE' : 'Person', 'P_FNAME' : '', 'P_LNAME' : '', 'P_DOB' : '', 'P_ORIGIN' : '', 'P_LOGSOURCE' : '', 'P_POB' : '', 'P_GEN' : '', 'DESC' : ''}
                                elif e['LABEL'][0] == 'o':
                                    mE = {'TYPE' : 'Object', 'O_TYPE' : '', 'O_CATEGORY' : '', 'O_DESC' : '', 'O_CLASS1' : '', 'O_CLASS2' : '', 'O_CLASS3': '', 'O_ORIGIN' : '', 'O_LOGSOURCE' : ''}
                                elif e['LABEL'][0] == 'l':
                                    mE = {'TYPE' : 'Location', 'L_TYPE' : '', 'L_DESC' : '', 'L_XCOORD' : '', 'L_YCOORD' : '', 'L_ZCOORD' : '', 'L_CLASS1': '', 'L_ORIGIN' : '', 'L_LOGSOURCE' : ''}
                                else:
                                    mE = {'TYPE' : 'Event', 'E_TYPE' : '', 'E_CATEGORY' : '', 'E_DESC' : '', 'E_LANG' : '', 'E_CLASS1' : '', 'E_TIME' : '', 'E_DATE' : '', 'E_DTG' : '', 'E_XCOORD' : '', 'E_YCOORD' : '', 'E_ORIGIN' : '', 'E_ORIGINREF' : '', 'E_LOGSOURCE' : ''}
                                mE['LABEL'] = e['LABEL']
                                newE.append(mE)
                                
                            # Assign the key value to this entity
                            mE[k] = row[c]
            
            # Check the entities extracted in the row and fill the GUID to complete the entity
            for mE in newE:
                if mE['LABEL'][0] == 'p':
                    if mE not in data['Persons']:
                        mE['GUID'] = self.DB.insertPerson(mE['P_GEN'], mE['P_FNAME'], mE['P_LNAME'], mE['P_DOB'], mE['P_POB'], mE['P_ORIGIN'], None, mE['P_LOGSOURCE'], mE['DESC'])
                        data['Persons'].append(mE)
                        
                elif mE['LABEL'][0] == 'o':
                    if mE not in data['Objects']: 
                        mE['GUID'] = self.DB.insertObject(mE['O_TYPE'], mE['O_CATEGORY'], mE['O_DESC'], mE['O_CLASS1'], mE['O_CLASS2'], mE['O_CLASS3'], mE['O_ORIGIN'], None, mE['O_LOGSOURCE']) 
                        data['Objects'].append(mE) 
                
                elif mE['LABEL'][0] == 'l':
                    if mE not in data['Locations']: 
                        mE['GUID'] = self.DB.insertLocation(mE['L_TYPE'], mE['L_DESC'], mE['L_XCOORD'], mE['L_YCOORD'], mE['L_ZCOORD'], mE['L_CLASS1'], mE['L_ORIGIN'], None, mE['L_LOGSOURCE'])
                        data['Locations'].append(mE) 
                
                else:
                    if mE not in data['Events']: 
                        mE['GUID'] = self.DB.insertEvent(mE['E_TYPE'], mE['E_CATEGORY'], mE['E_DESC'], mE['E_LANG'], mE['E_CLASS1'], mE['E_TIME'], mE['E_DATE'], mE['E_DTG'], mE['E_XCOORD'], mE['E_YCOORD'], mE['E_ORIGIN'], None, mE['E_LOGSOURCE'])
                        data['Events'].append(mE) 
            
            # Use the relations table to determine the final steps
            for r in self.vMap['relations']:
                for S in newE:
                    for T in newE:
                        if S['LABEL'] == r['S_LABEL'] and T['LABEL'] == r['T_LABEL']:
                            print("Insert rel %d %s %s %d %s" % ( S['GUID'], S['TYPE'], r['R_TYPE'], T['GUID'], T['TYPE']))    
    
    def getColumns(self, view):
        
        cols = []
        for col in list(view):
            cols.append({'name' : col, 'dtype' : view[col].dtype.name})
        
        return cols    
    
    
    def getFile(self, fileURL):

        print(fileURL)
        self.Path = fileURL
        
        if fileURL[-4:] == '.csv':
            try:
                view = pd.read_csv(fileURL, encoding='latin-1')
            except:
                view = pd.read_csv(fileURL, encoding='utf_8')
                
            
            print('[*] Transformed %s to view.' % (fileURL))
                    
        if fileURL[-5:] == '.xlsx':
            try:
                view = pd.read_excel(fileURL, encoding='latin-1')
            except:
                view = pd.read_excel(fileURL, encoding='utf_8')   
            print('[*] Transformed %s to view.' % (fileURL))        
        
        return view
    
    def getFolderData(self, folder):
        
        self.Path = './data/%s/' % (folder)
        for f in os.listdir(self.Path):
            if f[-4:] == '.csv':
                try:
                    view = pd.read_csv('%s/%s' % (self.Path, f), encoding='latin-1')
                except:
                    view = pd.read_csv('%s/%s' % (self.Path, f), encoding='utf_8')
                print('[*] Transformed %s to view.' % (f))
                        
            if f[-5:] == '.xlsx':
                try:
                    view = pd.read_excel('%s/%s' % (self.Path, f), encoding='latin-1')
                except:
                    view = pd.read_excel('%s/%s' % (self.Path, f), encoding='utf_8')   
                print('[*] Transformed %s to view.' % (f))
            
        view = view.fillna('')
        
        if folder == 'ACLED':
            self.Data = view
            self.ETLACLED2Graph(view)
        elif folder == 'GTD':
            self.Data = view
            self.ETLGTD2Graph(view)   
        elif folder == 'VP':
            self.Data = view
            self.ETLVP2Graph(view)    
        elif folder == 'POLICE':
            self.Data = view
            self.ETLPolice2Graph(view)
        elif folder == 'TEXT':
            self.Data = view
            self.ETLText2Graph(view)
        elif folder == 'GDELT':
            self.Data = view
            self.ETLGDELT2Graph(view)
        elif folder == 'REPORTS':
            self.Data = view
            self.DocType = 'REPORT'
            
    def setvMap(self):
        
        CLASSIFICATION = 'B1'
        
        LABEL = 'LABEL'
        P_GEN = 'P_GEN'
        P_FNAME = 'P_FNAME'
        P_LNAME = 'P_LNAME'
        P_DOB = 'P_DOB'
        P_POB = 'P_POB' 
        P_ORIGIN = 'P_ORIGIN'
        P_LOGSOURCE = 'P_LOGSOURCE'
        O_TYPE = 'O_TYPE'
        O_CATEGORY = 'O_CATEGORY'
        O_DESC = 'O_DESC'
        O_CLASS1 = 'O_CLASS1'
        O_CLASS2 = 'O_CLASS2'
        O_CLASS3 = 'O_CLASS3'
        O_ORIGIN = 'O_ORIGIN'
        O_LOGSOURCE = 'O_LOGSOURCE'
        R_TYPE = 'R_TYPE'
        S_LABEL = 'S_LABEL'
        T_LABEL = 'T_LABEL'
        
        vMap = {}
        vMap['mapname']   = 'VP'
        vMap['relations'] = [{S_LABEL : 'pSubject', T_LABEL : 'oMosaic', R_TYPE : 'HasAttribute'}]
        
        vMap['entities']  = [{LABEL : 'pSubject', P_GEN : 'Gender', P_FNAME : 'Forename', P_LNAME : 'Surname', P_DOB : 'Age', P_POB : 'Town', P_ORIGIN : 'PersonID', P_LOGSOURCE : CLASSIFICATION}, 
                            {LABEL : 'oMosaic', O_TYPE : 'MOSAIC', O_CATEGORY : 'MOSAIC Type', O_DESC : 'MOSAIC Property', O_CLASS1 : 'MOSAIC HH Composition', O_CLASS2 : 'MOSAIC Age Brand', O_CLASS3: 'MOSAIC No of Children', O_ORIGIN : 'MOSAIC Household Income', O_LOGSOURCE : CLASSIFICATION}
                            ]
        self.vMap = vMap    
            
    def getAllData(self):
        
        for folder in os.listdir('./data'):
            print("[*] %s tables found in %s" % (len(os.listdir('./data/%s' % folder))-1, folder))
            for f in os.listdir('./data/%s' % folder):
                fpath = './data/%s/%s' % (folder,f )
                print('    Checking %s...' % fpath)
                if f[-4:] == '.csv':
                    try:
                        view = pd.read_csv(fpath, encoding='latin-1')
                    except:
                        view = pd.read_csv(fpath, encoding='utf_8')
                if f[-4:] == '.tsv':
                    return
                        
                if folder == 'ACLED':
                    self.ETLACLED2Graph(view)
                if folder == 'GTD':
                    self.ETLGTD2Graph(view)   
                if folder == 'VP':
                    self.ETLVP2Graph(view)    
                if folder == 'POLICE':
                    self.ETLPolice2Graph(view)
                if folder == 'TEXT':
                    self.ETLText2Graph(view)    
                       
# Command Line 
#import OrientModels as om
#DB = om.OrientModel(None)
#folder = 'C:\\Users\\d063195\\Desktop\\_Projects\\20170120_VP\\Barnsley MBC Secure collaboration\\Vulnerable People PoC - Data files\\'
#fname = 'Person.xlsx'
#fileURL = folder + fname
#folder = 'REPORTS'
#pdb = OsintPubDB(DB)
#pdb.setvMap()
#v = pdb.getFile(fileURL)
#cols = pdb.getColumns(v)
#pdb.POLERExtract(v)
#pdb.processURL(url)

#pdb.getFolderData(folder)
#pdb.ETLReport2HANA()

#pdb.getAllData()
#pdb.ETLACLED2Graph(views[0])
#pdb.ETLGTD2Graph(views[1])
#pdb.ETLGDELT2Graph(view)

#views = pdb.getAPIData()
