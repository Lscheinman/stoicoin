import sys, os, string, time
import pandas as pd
from datetime import datetime
               

ORIGIN = 'YoT to 30'
ORIGINREF = ''
LOGSOURCE = 'C1'
DESC = ''
        
class FileManager():
    
    def __init__(self, DB):
        
        self.lakeURL = None
        self.TFURL = None
        self.sep = None
        self.DB = DB
        self.fpaths = []
        self.setPaths()

    def setPaths(self):
        cwd = os.getcwd()
        if 'C:\\' in cwd:
            
            self.lakeURL = 'C:\\Users\\d063195\\Desktop\\Lake\\'
            self.TFURL = 'C:\\Users\\d063195\\Desktop\\Lake\\Think_Family\\'
            self.sep     = '\\'
        else:
            dataURL = '%s/data/sets' % cwd
            lakeURL = 'C:\\'  
            self.sep = '/'
               
    def get_files(self):
        
        for f in os.listdir(self.TFURL):
            if os.path.isfile(self.TFURL + f) == True:
                self.fpaths.append({'path' : self.TFURL + f, 'size' : os.path.getsize(self.TFURL + f)})
        
        return {'message' : '%d files found' % len(self.fpaths)}
       
    def open_file(self, fpath):
        
        if fpath[-3:] == 'csv':
            return pd.read_csv(fpath)
        elif fpath[-3:] == 'lsx':
            return pd.read_excel(fpath)

    
    def map_Civica(self):
        
        for f in self.fpaths:
            if 'PostProcessed_TBL_Civica_ASB' in f['path']:  
                df = self.open_file(f['path'])
                
                for index, row in df.iterrows():
                    UPRN = row['UPRN']
                    REF = row['Civica_ReferenceNo']
                    DATE = row['ASB_Date']
                    DESC = row['ASB_Description']
                    ADDR = row['Civica_Address']
                    ADSC = row['ASB_Decision']
                    TENU = row['Civica_Tenure']
                    OCCU = row['ASB_Occupier Name']                   
                    
                    if len(str(UPRN)) > 1:
                        uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number' % UPRN, UPRN, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)                    
                        
                        if len(str(REF)) > 1:
                            rGUID = self.DB.insertObject('Reference Code', 'Civica', '%s reference on %s' % (REF, DATE), REF, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'Involves', rGUID, 'Object')
                        
                        if len(str(ADDR)) > 1:
                            aGUID = self.DB.insertLocation('Civic Location', ADDR, 0, 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'OccurredAt', aGUID, 'Location')
                            
                        if len(str(DATE)) > 1:
                            eGUID = self.DB.insertEvent('ASB', ADSC, '%s with decision %s by %s about %s' % (DESC, ADSC, TENU, OCCU), 'en', 0, '12:00', DATE, 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(eGUID, 'Event', 'Involves', uGUID, 'Object')
        
    def map_EducationPeriod(self):
        
        for f in self.fpaths:
            if 'TBL_Education_Period' in f['path']:
                df = self.open_file(f['fpath'])
                
                for index, row in df.iterrows():
                    P_FNAME = row['FIRSTNAME']
                    P_LNAME = row['LASTNAME']
                    P_DOB   = row['DOB']
                    P_GEN   = row['GENDER']
                    REC_ID  = row['ID']
                    FREE_MEALS = row['FREE SCHOOL MEALS']
                    NOR_TYPE = row['NOR_TYPE']
                    EXCLUSN = row['EXCLUSION']
                    Percent = row['PERCENT ATTENDANCE']
                    HOUSE = row['HOME']
                    HOUSNO = row['HOUSE NUMBER']
                    APARTMENT = row['APARTMENT']
                    ADDRESS0 = row['ADDRESS0']
                    ADDRESS1 = row['ADDRESS1']
                    ADDRESS2 = row['ADDRESS2']
                    TOWN = row['TOWN']
                    POSTCODE = row['POSTCODE']
                    BASE_NAME = row['BASE_NAME']
                    
                    if len(P_FNAME) > 1:
                        pGUID = self.DB.insertPerson(P_GEN, P_FNAME, P_LNAME, P_DOB, '', ORIGIN, ORIGINREF, LOGSOURCE, DESC)
                        FullAddress = ('%s %s %s %s %s %s %s %s' % (HOUSENO, HOUSE, APARTMENT, ADDRESS0, ADDRESS1, ADDRESS2, TOWN, POSTCODE)).strip()
                        lGUID = self.DB.insertLocation('Home Address', FullAddress, 0, 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                        self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')
                        
                        if str(FREE_MEALS) == 'T':
                            mGUID = self.DB.insertObject('Plan', 'Free School Meals', 'Education based plan for free meals at school', 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(pGUID, 'Person', 'HasStatus', mGUID, 'Object')
                        
                        if len(str(NOR_TYPE)) > 1:
                            nGUID = self.DB.insertObject('Plan', 'Education', 'NOR TYPE: %s' % NOR_TYPE, 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            
                        if len(str(Percent)) > 1:
                            aGUID = self.DB.insertObject('Rate', 'School Attendance', '%s attendance rate', Percent, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            
                            
                            
    
    def map_Benefits(self):
        
        for f in self.fpaths:
            if 'TBL_Academy_Benefits' in f['path']:
                df = self.open_file(f['path'])
                
                for index, row in df.iterrows():
                    P_FNAME = row['Forename'] + ' ' + row['Surname']
                    P_GEN = str(row['Gender'][0]).upper()
                    P_DOB = row['DOB']
                    
                    NIN = row['NI_Number']
                    UPRN = row['UPRN']
                    
                    ACADD = row['Academy_Address']
                    ClaimRef = row['ClaimRefNo']
                    ClaimType = row['ClaimType']
                    Income_Code = row['Income_Code']
                    
                    if len(P_FNAME) > 1:
                        pGUID = self.DB.insertPerson(P_GEN, P_FNAME, '', P_DOB, '', ORIGIN, ORIGINREF, LOGSOURCE, DESC)
                        
                        if len(str(NIN)) > 1:
                            nGUID = self.DB.insertObject('Reference Code', 'NIN', '%s National Insurance Number assigned to %s' % (NIN, P_FNAME), NIN, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(nGUID, 'Object', 'Involves', pGUID, 'Person')
                        
                        if len(str(UPRN)) > 1:
                            uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number.' % UPRN, UPRN, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')
                        
                        if len(str(Income_Code)) > 1:
                            iGUID = self.DB.insertObject('Reference Code', 'Income', 'Income code %s assigned to %s' % (Income_Code, P_FNAME), Income_Code, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(iGUID, 'Object', 'Involves', pGUID, 'Person')
                                            
                        if len(str(ClaimRef)) > 1:
                            cGUID = self.DB.insertObject('Reference Code', 'Claim', '%s claim %s involving %s' % (ClaimType, ClaimRef, P_FNAME), ClaimRef, ClaimType, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                            self.DB.insertRelation(cGUID, 'Object', 'Involves', pGUID, 'Person')
    
    
    def map_YOT_File(self):
        
        df = self.open_file(self.fpaths[2]['path'])
        df['Client No'] = df['Client No'].fillna(0)
        df['Ref No'] = df['Ref No'].fillna(0)
        df['Client No'] = df['Client No'].fillna(0)
        df['Client Name'] = df['Client Name'].fillna('unk')
        
        for index, row in df.iterrows():
            P_FNAME = row['Client Name']
            P_DOB = row['DOB']
            HOUSENO = row['House Number']
            ADDRESS = row['Address']
            POSTCODE = row['Postcode']
            UPRN = row['UPRN Number']
            OUTCOME = row['Outcome Type Desc']
            OUTDATE = row['Outcome Date']
            REF = '%s-%s' % (row['Ref No'], row['Client No'])
            
            if len(P_FNAME) > 1:
                pGUID = self.DB.insertPerson('U', P_FNAME, '', P_DOB, '', ORIGIN, ORIGINREF, LOGSOURCE, DESC)
            
                if (len(str(HOUSENO)) + len(str(ADDRESS)) + len(str(POSTCODE))) > 1:
                    lGUID = self.DB.insertLocation('Home', '%s %s %s' % (HOUSENO, ADDRESS, POSTCODE), 0, 0, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                    self.DB.insertRelation(pGUID, 'Person', 'LivesAt', lGUID, 'Location')
                    
                if len(str(UPRN)) > 1:
                    uGUID = self.DB.insertObject('Reference Code', 'UPRN', '%s Unique Pupil Registration Number' % UPRN, UPRN, 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                    self.DB.insertRelation(uGUID, 'Object', 'Involves', pGUID, 'Person')
    
                if len(str(OUTCOME)) > 1:
                    eGUID = self.DB.insertEvent('U18', OUTCOME, '%s type of outcome for %s on %s' % (OUTCOME, P_FNAME, OUTDATE), 'en', REF, '12:00', OUTDATE, '', 0, 0, ORIGIN, ORIGINREF, LOGSOURCE)
                    self.DB.insertRelation(eGUID, 'Event', 'SubjectofContact', pGUID, 'Person')
            
            else:
                pGUID = None
                
            #TODO how to handle Concatenation rules
            
            
            # Create the person

            

import OrientModels
DB = OrientModels.OrientModel(None) 
FM = FileManager(DB)
step1 = FM.get_files()
#FM.map_YOT_File()
#FM.map_Benefits()
FM.map_Civica()
