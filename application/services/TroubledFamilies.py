import sys, os, string, time
import pandas as pd
from datetime import datetime
               
        
class FileManager():
    
    def __init__(self):
        
        self.lakeURL = None
        self.TFURL = None
        self.sep = None
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
    
    def name_extract(self, name):
    
        if name.count(' ') == 1:
            #1st space
            sp = name.find(' ') 
            LNAME = name[sp+1:len(name)]  
            
        elif name.count(' ') == 2:
            #2nd space
            sp = name[name.find(' ')+1:].find(' ') + name.find(' ') + 1
            LNAME = name[sp+1:len(name)]  
            
        elif name.count(' ') == 3:
            #3rd space
            sp = name[name[name.find(' ')+1:].find(' ') + name.find(' ') + 2:].find(' ') + name[name.find(' ')+1:].find(' ') + name.find(' ') + 2
            LNAME = name[sp+1:len(name)]          
           
        else:
            sp = len(name)
            LNAME = 'Doe'
            
        FNAME = name[:sp]
            
        return FNAME, LNAME
    
    def map_YOT_File(self):
        
        df = self.open_file(self.fpaths[2]['path'])
        df['Client No'] = df['Client No'].fillna(0)
        df['Ref No'] = df['Ref No'].fillna(0)
        df['Client No'] = df['Client No'].fillna(0)
        df['Client Name'] = df['Client Name'].fillna('unk')
        
        for index, row in df.iterrows():
            FNAME = self.name_extract(row['Client Name'])
            DOB = row['DOB']
            
            
            # Create the person
            
    




FM = FileManager()
step1 = FM.get_files()
FM.map_YOT_File()
