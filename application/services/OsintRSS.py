# -*- coding: utf-8 -*-
import time, os, json, requests, uuid, bs4, math
from datetime import datetime
import pandas as pd
import _locale
from google.cloud import bigquery

# Ensure printing to screen is formatted correctly
_locale._getdefaultlocale = (lambda *args: ['en_US', 'utf8'])
csvCellLimit = 32760
debugging = False
class OsintRSS():
    
    def __init__(self, DB):
        
        if '\\' in os.getcwd():
            if debugging == False:
                self.gdeltPath = '%s\\application\\services\\data\\GDELT\\' % (os.getcwd())
                self.RSSPath   = '%s\\application\\services\\data\\RSS\\' % (os.getcwd()) 
                auth = '%s\\application\\services\\config\\AUTH_Google.json' % (os.getcwd())
            else:
                self.gdeltPath = '%s\\data\\GDELT\\' % (os.getcwd()) # debugging line 
                self.RSSPath   = '%s\\data\\RSS\\' % (os.getcwd()) # debugging line  
                auth = '%s\\config\\AUTH_Google.json' % (os.getcwd())
        else:
            if debugging == False:
                self.gdeltPath = '%s/application/services/data/GDELT/' % (os.getcwd())
                self.RSSPath   = '%s/application/services/data/RSS/' % (os.getcwd()) 
                auth           = '%s/application/services/config/AUTH_Google.json' % (os.getcwd())
            else:
                self.gdeltPath = '%s/data/GDELT/' % (os.getcwd()) # debugging line
                self.RSSPath   = '%s/data/RSS/' % (os.getcwd()) 
                auth           = '%s/config/AUTH_Google.json' % (os.getcwd())
                
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = auth        
        
        # setup Feed Attributes
        self.Live      = False
        self.DB        = DB
        self.timestamp = time.strftime('%Y-%b-%d_%H%M') 
        self.Data      = None
        self.Columns   = []
        self.Query     = None
        self.Path      = None
        self.NewsAPIArticles = []
        self.NewsAPISources  = []
        self.O_LOGSOURCEs    = []
        self.GUIDIndex       = {}

    def responseHandler(self, response):
    
        if response.status_code == 200:
            results = json.loads(response.text)
            return results
        
        if response.status_code == 400:
            results = json.loads(response.text)
            print(results['message'])
            return None
        
        else:
            print("[!] Error:%s" % response)
            return None 
    def processURL(self, url):
        
        processedURL = {'text' : '', 'txt2' : '', 'links' : []}
        try:
            paras = bs4.BeautifulSoup(self.DB.getResponse(url, None).text).findAll('p')
        except:
            print('[!] Error')
            return None
        if len(paras) > 1:
            for p in paras:
                processedURL['text'] = ('%s%s' % (processedURL['text'], p.get_text())).replace("[", "").replace("]", "").replace("'", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
        
            if len(paras) + len(processedURL['text']) > csvCellLimit:
                print("[*] Max CSV cell length reached with %s" % url)
                processedURL['text'] = processedURL['text'][:csvCellLimit]

        return processedURL    
    
    
    def newsApiArticles(self, source):
        
        '''
        ENDPOINT:NEWSAPI.ORG:ARTICLES
        params:
        - source(required) - The identifer for the news source or blog you want headlines
          from. Use the /sources endpoint to locate this or use the sources index. https://newsapi.org/sources
        
        - apiKey(required) - Your API key. Alternatively you can provide this via the 
          X-Api-Key HTTP header.
        
        - sortBy(optional) - Specify which type of list you want. The possible options are 
          top, latest and popular. Note: not all options are available for all sources. 
          Default: top.
          
        response:
        - status(string) - If the request was successful or not. Options: ok, error. In the 
          case of error a code and message property will be populated.
        - source(string) - The identifier of the source requested.
        - sortBy(string) - Which type of article list is being returned. Options: top, latest, popular.
        - articles(array) - containing ['author', 'description', 'title', 'url', 'urlToImage', publishedAt]
        
        sample request url: 
        - https://newsapi.org/v1/articles?source=the-next-web&sortBy=latest&apiKey=04b71df7d53d41b89953a1104c6140e8
        
        '''
        # authentication and options for results
        auth = '%s\\config\\AUTH_NewsAPI.json' % (os.getcwd())
        keys = json.loads(open(auth).read())        
        apiNews = keys['client_key']
        apiUrl = 'https://newsapi.org/v1/articles?source=%s&apiKey=%s' % (source, apiNews)
        print("[*] Calling %s" % apiUrl)
       
        response = self.DB.getResponse(apiUrl, None)
        news = self.responseHandler(response) 
        if news == None:
            print("[!] No articles found.")
            return
        else:
            print("[*] Processing articles.")
   
        for a in news['articles']:
            # Create an Event for the creation of the account and relate it to the account
            E_TYPE = "RSS"
            E_CATEGORY = "NewsStory"
            if a['description'] != None:
                sdesc = a['description'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                E_DESC = "'%s'" % sdesc
            else:
                E_DESC = "DESC"
            if a['title'] != None:
                sdesc = a['title'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
                E_CLASS1 = "'%s'" % sdesc                
            else:
                E_CLASS1 = "CLASS1"
            if a['publishedAt'] != None:
                try:
                    if 'T' in a['publishedAt']:
                        sDTG = a['publishedAt'].replace('T', ' ')
                    sDTG = sDTG[:19]
                    E_TIME = time.strftime('%H:%M:%S', time.strptime(sDTG, '%Y-%m-%d %H:%M:%S'))
                    E_DATE = time.strftime('%Y-%m-%d', time.strptime(sDTG,'%Y-%m-%d %H:%M:%S'))
                except:
                    E_TIME = time.strftime("%H:%M:%S", time.localtime())
                    E_DATE = time.strftime("%Y-%m-%d", time.localtime())
            else:
                E_TIME = time.strftime("%H:%M:%S", time.localtime())
                E_DATE = time.strftime("%Y-%m-%d", time.localtime())
            if ['author'] != None:
                E_ORIGIN = a['author'] 
            else:
                E_ORIGIN = 'Author'
            E_ORIGINREF = "%s" % a['url']  
            E_LOGSOURCE = '%s' % a['urlToImage']
            E_XCOORD = "E_XCOORD"
            E_YCOORD = "E_YCOORD"
            E_LANG = "LANG"
            E_DTG = ("%s%s" % (E_DATE, E_TIME)).replace("-", "").replace(":", "")
            
            processedURL = self.processURL(E_ORIGINREF)
            if processedURL != None:
                text = processedURL['text'].replace("'", "").replace('"', '')  
                E_DESC = '%s Full Story: %s' % (E_DESC, text)
            
            self.ETLNewsAPIEvent2HANA(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE) 
            if self.Live == True:
                for O_LOGSOURCE in self.GUIDIndex:
                    if O_LOGSOURCE in E_LOGSOURCE:
                        self.DB.CurObject = self.GUIDIndex[O_LOGSOURCE]
                        self.DB.insertRelation(self.DB.R_GUID, self.DB.CurObject, 'Object', 'ReportedBy', self.DB.CurEvent, 'Event')                    
            else:
                d = {'E_TYPE' : E_TYPE,
                     'E_CATEGORY' : E_CATEGORY,
                     'E_DESC' : E_DESC,
                     'E_LANG' : E_LANG,
                     'E_CLASS1' : E_CLASS1,
                     'E_TIME' : E_TIME,
                     'E_DATE' : E_DATE,
                     'E_DTG' : E_DTG,
                     'E_XCOORD' : E_XCOORD,
                     'E_YCOORD' : E_YCOORD,
                     'E_ORIGIN' : E_ORIGIN,
                     'E_ORIGINREF' : E_ORIGINREF,
                     'E_LOGSOURCE' : E_LOGSOURCE
                     }
                self.NewsAPIArticles.append(d)  

            #eGUID = Gdb.insertEvent(self.graph, TYPE, CATEGORY, DESC, CLASS1, TIME, DATE, ORIGIN, ORIGINREF, LOGSOURCE) 
            # Look up the source by using the id contained within the ORIGINREF and an extra attribute to narrow results
            #oGUID = Gdb.ContainsQuery(self.graph, 'Object', 'ORIGINREF', source, 'CATEGORY', 'NewsSource')
            #Gdb.insertRelation(self.graph, oGUID, 'Object', 'PostedArticle', eGUID, 'Event')
            
    def newsApiGetAll(self, Live):
        
        if Live == 'Y' or Live == 'y':
            self.DB.goLive()
        
        if len(self.NewsAPISources) < 1:
            print("[*] No news sources yet. Filling roster.")
            self.newsApiSources()
        for source in self.NewsAPISources:
            self.newsApiArticles(source)    
            
        if self.DB.connected == False:
            df = pd.DataFrame(self.NewsAPIArticles)
            df.to_excel("%sNewsAPI_Articles_%s.xlsx" % (self.RSSPath, self.timestamp))
            print("[*] %d news articles stored at %sNewsAPI_Articles%s.xlsx " % (len(self.NewsAPIArticles), self.RSSPath, self.timestamp)) 
            
    def newsApiSources(self):
        '''
        ENDPOINT:NEWSAPI.ORG:SOURCES
        params:
        - category(optional) - The category you would like to get sources for. Possible 
          options: business, entertainment, gaming, general, music, politics, science-and-nature, 
          sport, technology. Default: empty (all sources returned)
          
        - language(optional) - The 2-letter ISO-639-1 code of the language you would like to
          get sources for. Possible options: en, de, fr. Default: empty (all sources returned)
    
        - country(optional) - The 2-letter ISO 3166-1 code of the country you would like to get 
          sources for. Possible options: au, de, gb, in, it, us. Default: empty (all sources returned)
    
        response:
        - id(string) - The unique identifier for the news source. This is needed when querying the
          /articles endpoint to retrieve article metadata.  
        - description(string) - A brief description of the news source and what area they specialize in.  
        - url(string) - The base URL or homepage of the source.
        - name(string) - The display-friendly name of the news source.
        - country
     
        sample request url: 
        - https://newsapi.org/v1/sources?language=en
    
        ''' 
        language = 'en'
        apiUrl = 'https://newsapi.org/v1/sources'    
        
        response = self.DB.getResponse(apiUrl, None)
        news = self.responseHandler(response) 
        DataFrameList = []
            
        for s in news['sources']:
            # Create an Event for the creation of the account and relate it to the account
            O_TYPE = "Website"
            O_CATEGORY = "NewsSource"
            sdesc = s['description'].replace('"', "").replace("'", '').replace('\\', '').replace('\n', '').replace('\t', '')
            O_DESC = "'%s'" % sdesc
            O_CLASS1 = s['name']
            O_CLASS2 = s['category']
            O_CLASS3 = s['country']
            O_ORIGIN = s['language']
            O_ORIGINREF = "%s-%s" % (s['id'], s['url'])
            O_LOGSOURCE = '%s' % s['url']
            
            sSource = O_CLASS1.replace(" ", "-").replace("(", "").replace(")", "").replace(".", "-")
            if sSource not in self.NewsAPISources:
                if 'FourFourTwo' in sSource:
                    sSource = 'four-four-two'
                if 'Reddit' in sSource:
                    sSource = 'reddit-r-all'
                self.NewsAPISources.append(sSource)
                self.O_LOGSOURCEs.append(O_LOGSOURCE)
                
            if self.DB.connected == True:
                self.ETLNewsAPIObject2HANA(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
            else:
                d = {'O_TYPE' : O_TYPE,
                     'O_CATEGORY' : O_CATEGORY,
                     'O_DESC' : O_DESC,
                     'O_CLASS1' : O_CLASS1,
                     'O_CLASS2' : O_CLASS2,
                     'O_CLASS3' : O_CLASS3,
                     'O_ORIGIN' : O_ORIGIN,
                     'O_ORIGINREF' : O_ORIGINREF,
                     'O_LOGSOURCE' : O_LOGSOURCE
                     }
                DataFrameList.append(d)
            print("[*] News source %s stored" % O_CLASS1)
                
        if self.DB.connected == False:
            df = pd.DataFrame(DataFrameList)
            df.to_excel("%sNewsAPI_Sources_%s.xlsx" % (self.RSSPath, self.timestamp))
            print("[*] %d news sources stored at %sNewsAPI_Sources_%s.xlsx " % (len(DataFrameList), self.RSSPath, self.timestamp))        
                              
    def newsApiToXLSX(self):
        
        if self.DB.connected == False:
            df = pd.DataFrame(self.NewsAPIArticles)
            df.to_excel("%sNewsAPI_Sources_%s.xlsx" % (self.RSSPath, self.timestamp))
            print("[*] %d news sources stored at %sNewsAPI_Sources_%s.xlsx " % (len(DataFrameList), self.RSSPath, self.timestamp))
                     
    def ETLNewsAPIObject2HANA(self, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE):
        
        entity = {'TYPE' : 'Object', 'LOOKUP' : O_ORIGINREF}
        O_GUID, exists = self.DB.EntityResolve(entity)
        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % O_GUID)
        else:        
            self.DB.insertObject(O_GUID, O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
        
        self.GUIDIndex.update({O_LOGSOURCE : O_GUID})
     
    def ETLNewsAPIEvent2HANA(self, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE):
        
        entity = {'TYPE' : 'Event', 'LOOKUP' : E_DESC}
        E_GUID, exists = self.DB.EntityResolve(entity)
        if exists == 1:
            print('[*] %s Already exists in HANA. No entry made.' % E_GUID)
        else: 
            try:
                self.DB.insertEvent(E_GUID, E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
            except Exception as e:
                print("[!] %s" % str(e))
    
    def ETLNewsAPI2HANAFromFile(self):
        
        if self.DB.connected == False:
            print("[*] Not connected to HANA. Connecting now...")
            self.DB.ConnectToHANA()
        
        if "O_DESC" in self.Data.columns:
            for index, row in self.Data.iterrows():
                O_TYPE       = row['O_TYPE'] 
                O_CATEGORY   = row['O_CATEGORY'] 
                O_DESC       = row['O_DESC'].replace("'", "").replace('"', '')
                O_CLASS1     = row['O_CLASS1'] 
                O_CLASS2     = row['O_CLASS2'] 
                O_CLASS3     = row['O_CLASS3'] 
                O_ORIGIN     = row['O_ORIGIN'] 
                O_ORIGINREF  = row['O_ORIGINREF'] 
                O_LOGSOURCE  = row['O_LOGSOURCE'] 
                
                self.ETLNewsAPIObject2HANA(O_TYPE, O_CATEGORY, O_DESC, O_CLASS1, O_CLASS2, O_CLASS3, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)       

        if "E_DESC" in self.Data.columns:
            for index, row in self.Data.iterrows():
                E_TYPE      = row['E_TYPE']
                E_CATEGORY  = row['E_CATEGORY'] 
                E_DESC      = row['E_DESC'].replace("'", "").replace('"', '')
                E_DESC      = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
                E_LANG      = row['E_LANG']
                E_CLASS1    = 0
                E_TIME      = row['E_TIME'] 
                E_DATE      = row['E_DATE']
                E_DTG       = row['E_DTG']
                E_XCOORD    = 0
                E_YCOORD    = 0
                E_ORIGIN    = row['E_ORIGIN']
                E_ORIGINREF = row['E_ORIGINREF']
                E_LOGSOURCE = row['E_LOGSOURCE']
                
                try:
                    self.ETLNewsAPIEvent2HANA(E_TYPE, E_CATEGORY, E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD,  E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
                
                    # Check for the GUID of the Source to form a relationship by checking if the url of sources is in the Article url
                    for O_LOGSOURCE in self.GUIDIndex:
                        if O_LOGSOURCE in E_LOGSOURCE:
                            self.DB.CurObject = self.GUIDIndex[O_LOGSOURCE]
                            self.DB.insertRelation(self.DB.R_GUID, self.DB.CurObject, 'Object', 'ReportedBy', self.DB.CurEvent, 'Event')
                except Exception as e:
                    print("[!] Likely encoding error %s" % str(e))
    
    def getNewsAPIData(self):
        
        articles = []
        
        for f in os.listdir(self.RSSPath):
            if 'Source' in f:
                try:
                    sview = pd.read_excel('%s/%s' % (self.RSSPath, f), encoding='utf_8')
                except:
                    sview = pd.read_excel('%s/%s' % (self.RSSPath, f), encoding='latin-1') 
                
                # Sources need to get set first to get the index of GUIDs for linking articles
                self.Data = sview.fillna('')   
                self.ETLNewsAPI2HANAFromFile()                
              
            if 'Article' in f:
                try:
                    aview = pd.read_excel('%s/%s' % (self.RSSPath, f), encoding='utf_8')
                except:
                    aview = pd.read_excel('%s/%s' % (self.RSSPath, f), encoding='latin-1')  
                articles.append(aview.fillna(''))
            print('[*] Transformed %s to view.' % (f))
                        
        for aview in articles:
            self.Data = aview
            self.ETLNewsAPI2HANA()        
                    
    def gdeltGetArgs(self):
        
        sDTG = 20150302000000
        eDTG = 20150304000000
        Persons = '%news%'
        Location = 'AE'
        LIMIT = 300        
        
        return sDTG, eDTG, Persons, LIMIT
        
    def gdeltGeoStability(self, qVars):
        '''
        Using the Goldstein Scale, run collection based on a country to build a timeline of events that affect stability.
        http://web.pdx.edu/~kinsella/jgscale.html
        The time series in mets7999.sav are combined measures of conflict and cooperation. When there was more cooperation
        than conflict during the month, the indicator is positive; when there was more conflict than cooperations, the indicator is negative.
        '''
        self.Query = """
        SELECT ActionGeo_Lat, ActionGeo_Long, ActionGeo_FullName, DATEADDED, AvgTone, Actor1Name, NumArticles, EventRootCode, NumSources, GoldsteinScale, NumMentions, SOURCEURL
        FROM [gdelt-bq:full.events] 
        WHERE Actor1Geo_Lat IS NOT null AND ActionGeo_CountryCode == '%s'
        LIMIT %d
        """ % (qVars['ActionGeo'], qVars['LIMIT'])
    
    def gdeltThemes(self, qVars):
        
        self.Query = """
            SELECT theme, COUNT(*) as count
            FROM (
            select UNIQUE(REGEXP_REPLACE(SPLIT(V2Themes,';'), r',.*', '"')) theme
            from [gdelt-bq:gdeltv2.gkg]
            where DATE>%d and DATE < %d and V2Persons like '%s'
            )
            group by theme
            ORDER BY 2 DESC
            LIMIT %d
            """  % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'])
    
    def gdeltPersons(self, qVars):
        self.Query = """
            SELECT person, COUNT(*) as count
            FROM (
            select REGEXP_REPLACE(SPLIT(V2Persons,';'), r',.*', '"') person
            from [gdelt-bq:gdeltv2.gkg]
            where DATE>%d and DATE < %d and V2Persons like '%s'
            )
            group by person
            ORDER BY 2 DESC
            LIMIT %d;
            """  % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'])

    def gdeltNetwork(self, qVars):
        self.Query = '''
        SELECT a.name, b.name, COUNT(*) as count
        FROM (FLATTEN(
        SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons,';'), r',.*', '"')) name
        FROM [gdelt-bq:gdeltv2.gkg] 
        WHERE DATE > %d and DATE < %d and V2Persons like '%s'
        ,name)) a
        JOIN EACH (
        SELECT GKGRECORDID, UNIQUE(REGEXP_REPLACE(SPLIT(V2Persons,';'), r',.*', '"')) name
        FROM [gdelt-bq:gdeltv2.gkg] 
        WHERE DATE> %d and DATE < %d and V2Persons like '%s'
        ) b
        ON a.GKGRECORDID=b.GKGRECORDID
        WHERE a.name<b.name
        GROUP EACH BY 1,2
        ORDER BY 3 DESC
        LIMIT %d
        ''' % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'])
        
    def gdeltEventsFromActorAction(self, qVars):
        
        # Build the query by filling in for blanks
        for key, value in qVars.items():    
            if isinstance(value, str) and len(value) < 1:
                qVars[key] = 'IS NOT null'
            elif isinstance(value, str) and len(value) > 1:
                qVars[key] = "= '%s'" % qVars[key]
        
        self.Query = '''
        SELECT *
        FROM [gdelt-bq:full.events] 
        WHERE SQLDATE > %d 
        AND ActionGeo_CountryCode %s
        AND GoldsteinScale %s 0
        AND Actor1Name %s
        AND Actor1Code %s
        AND Actor2Name %s
        LIMIT %d;
        ''' % (qVars['sDTG'], qVars['ActionGeo'], qVars['GoldsteinBin'], qVars['Actor1'],
               qVars['Actor1Code'], qVars['Actor2Name'], qVars['LIMIT'])
        print('[*] Query being sent to GoogleBQ: %s' % self.Query)
        
    def gdeltTopicsByLanguage(self, qVars):
        
        self.Query = '''
        SELECT theme, COUNT(*) as count
        FROM (
        select
        UNIQUE(REGEXP_REPLACE(SPLIT(V2Themes,';'),r',.*',"))theme
        from[gdelt-bq:gdeltv2.gkg]
        where DATE>%d and DATE <%d and AllNames like '%s' and
        TranslationInfo like '%srclc:%s%'
        )
        group by theme
        ORDER BY 2 DESC
        LIMIT %d
        ''' % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'], qVars['Language'])

    def gdeltOrgs(self, qVars):
        
        self.query = """
            SELECT organization, COUNT(*) as count
            FROM (
            select REGEXP_REPLACE(SPLIT(V2Organizations,';'), r',.*', '') organization
            from [gdelt-bq:gdeltv2.gkg]
            where DATE>%d and DATE < %d and V2Organizations like '%s'
            )
            group by organization
            ORDER BY 2 DESC
            LIMIT %d;
            """  % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'])  
    
    def gdeltBigQuery(self, qVars):
        '''
        Interface between accepting query criteria, building query, running query, and getting data by changing a list of tuples into a pandas dataframe
        
        bqJob = Big Query Jobs are actions that you construct and that BigQuery executes on your behalf to load data, export data, query data, or copy data.
        Because jobs can potentially take a long time to complete, they execute asynchronously and can be polled for their status. Shorter actions,
        such as listing resources or getting metadata are not managed by a job resource.
        
        bqC = Big Query Client
        '''
        # Set authentication and environment variables

        # Set the big query client and search arguments
        bqC = bigquery.Client()
        self.searchType = qVars['Type']
        # Build the query statement
        if qVars['Type'] == 'Orgs':
            self.gdeltOrgs(qVars)
        elif qVars['Type'] == 'Events':
            self.gdeltEventsFromActorAction(qVars)
        elif qVars['Type'] == 'Persons':
            self.gdeltThemes(qVars) 
        elif qVars['Language'] != 'all':
            self.gdeltTopicsByLanguage(qVars)
        elif qVars['Type'] == 'Network':
            self.gdeltNetwork(qVars) 
        elif qVars['Type'] == 'Location':
            self.gdeltPersonLocationHistogram(qVars)        
        # Build the GDELT query job with an ID and the query
        bqJob = bqC.run_async_query(str(uuid.uuid4()), self.Query)
        # Start the query
        bqJob.begin()
        # Process the result in google cloud
        bqJob.result()
        bqTbl = bqJob.destination
        # Put the data into a table
        bqTbl.reload()
        self.Data = bqTbl.fetch_data()
        for SchemaField in self.Data.schema:
            self.Columns.append(SchemaField.name)        
        DFtuples = []
        for row in self.Data:
            DFtuples.append(row)
        
        self.Data = pd.DataFrame(data=DFtuples, columns=self.Columns)
        rows = int(self.Data.shape[0])
        print('[*] Dataframe with Query Data complete with %d rows.' % rows)
        return rows
    
    def gdeltPersonLocationHistogram(self, qVars):
        self.Query = '''
        SELECT location, COUNT(*)
        FROM (
        select REGEXP_EXTRACT(SPLIT(V2Locations,';'),r'^.*?#(.*?)#') as location
        from [gdelt-bq:gdeltv2.gkg] 
        where DATE > %d and DATE < %d and V2Persons like '%s'
        )
        group by location
        ORDER BY 2 DESC
        LIMIT %d
        ''' % (qVars['sDTG'], qVars['eDTG'], qVars['Persons'], qVars['LIMIT'])
    
    
    def gdeltFillContent(self):
        '''
        Takes the Data from a query and fills content from the SOURCEURL
        
        '''
        st = time.time() 
        percent = -1
        percent_old = 0
        rows = int(self.Data.shape[0])
        print("[*] %d records found to fill content." % rows)
        index = 0
        self.Data['TEXT'] = ""
        urls = self.Data['SOURCEURL'].unique().tolist()
        urlTexts = {}
        i = 1

        # First load all URL texts for cases where URLs are used more than once in the corpus
        for url in urls:
            processedURL = self.processURL(url)
            if processedURL != None:
                urlTexts[url] = processedURL['text']
            else:
                urlTexts[url] = '[!] No Text Available'
            percent_old = percent
            percent = round(i/len(urls)*100)
            if(percent % 1 == 0 and percent != percent_old):
                p = float(float(i)/float(len(urls)))*100
                print("[*] URL %d: %f percent in %f seconds" % (int(i), p, time.time() - st)) 
            i+=1

        # Then map all URL text to the sources
        percent = -1
        percent_old = 0        
        for index, row in self.Data.iterrows():
            if row['SOURCEURL'] in urlTexts:
                self.Data.set_value(index, 'TEXT', urlTexts[row['SOURCEURL']])
            percent_old = percent
            percent = round(index/rows*100)
            if(percent % 1 == 0 and percent != percent_old):
                p = float(float(index)/float(rows))*100
                print("[*] ROW %d: %f percent in %f seconds" % (int(index), p, time.time() - st))                    
        print("[*] %d unique linked content added to each GDELT entry." % len(urls))

    def gdeltToXLSX(self):
        self.Data.to_excel('%s/GDELT-%s.xlsx' % (self.gdeltPath, self.timestamp))
        print('[*] Saved to %s/GDELT-%s.xlsx' % (self.gdeltPath, self.timestamp))    
    def gdeltToCSV(self):
        self.Data.to_csv('%s/GDELT-%s.csv' % (self.gdeltPath, self.timestamp))
        print('[*] Saved to %s/GDELT-%s.csv' % (self.gdeltPath, self.timestamp))
    def gdeltToJSON(self):
        self.Data.to_csv('%s/GDELT-%s.json' % (self.gdeltPath, self.timestamp))    
        print('[*] Saved to %s/GDELT-%s.json' % (self.gdeltPath, self.timestamp))
    
    def ta_run_counts(self, TA_VIEW, TA_RUN, intel_id):
        
        for Node in TA_VIEW:
            if Node['TA_TYPE'] == 'Topic':
                self.DB.insertRelation(intel_id, 'Event', 'TA_REFERENCE', Node['GUID'], 'Object')
                TA_RUN['COUNTS']['Objects']+=1
                TA_RUN['COUNTS']['GUID'].append(Node['GUID'])
            elif Node['TA_TYPE'] == 'PERSON':
                self.DB.insertRelation(intel_id, 'Event', 'TA_REFERENCE', Node['GUID'], 'Person')
                TA_RUN['COUNTS']['Persons']+=1
                TA_RUN['COUNTS']['GUID'].append(Node['GUID'])        
        
        return TA_VIEW, TA_RUN    
    
    
    def ETLGDELT(self, qVars):  
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        entity = {'TYPE': 'Event'}
        E_TYPE      = "OSINT"
        E_CATEGORY  = "GDELT-%s" % self.searchType  
        E_TIME      = datetime.fromtimestamp(time.time()).strftime('%H:%M')
        E_ORIGINREF = O_ORIGINREF = O_ORIGIN = ''
        E_LOGSOURCE = O_LOGSOURCE = 'A1'
        sourceURLS = [{'SOURCEURL' : None, 'TEXT' : None}]
        Actors     = [{'ActorName' : None, 'GUID' : None}]
        GSMeasures = [{'CLASS1' : None, 'GUID' : None}]
        
        if self.searchType == 'Events':
            for index, row in self.Data.iterrows():
                try:
                    E_DESC = row['TEXT'].replace("'", " ").replace('"', ' ')
                    E_DESC = bytes(E_DESC, 'utf-8').decode('utf-8', 'ignore')
                except:
                    E_DESC = ('%s- %s- %s' % (row['Actor1Geo_FullName'], row['Actor1Name'], row['DATEADDED'])).replace("'", "").replace('"', '')	
                
                E_LANG	 = row['AvgTone']
                E_CLASS1 = row['GoldsteinScale']
                E_DATE   = '%s-%s-%s' % (str(row['SQLDATE'])[:4], str(row['SQLDATE'])[4:6], str(row['SQLDATE'])[6:])	
                E_DTG	 = ('%s%s' % (E_DATE, E_TIME)).replace("'", "").replace(":", "").replace("-", "")		
                E_XCOORD = row['ActionGeo_Lat']	
                E_YCOORD = row['ActionGeo_Long']	
                E_ORIGIN = row['ActionGeo_FullName']
                
                # Ensure non-duplicate processing of URL
                found = False
                for U in sourceURLS:
                    if row['SOURCEURL'] == U['SOURCEURL']:
                        E_GUID = U['GUID']
                        found = True
                if found == False:
                    E_DESC = self.processURL(row['SOURCEURL'])
                    if E_DESC:
                        E_DESC = E_DESC['text']
                    else:
                        E_DESC = row['SOURCEURL']
                    E_GUID = self.DB.insertEvent(E_TYPE, 'Webpage', E_DESC, E_LANG, E_CLASS1, E_TIME, E_DATE, E_DTG, E_XCOORD, E_YCOORD, E_ORIGIN, E_ORIGINREF, E_LOGSOURCE)
                    sourceURLS.append({'SOURCEURL' : row['SOURCEURL'], 'TEXT' : E_DESC, 'GUID' : E_GUID})
                
                AC1_GUID = self.DB.insertObject('Actor', 'GDELT', row['Actor1Name'], row['Actor1Code'], row['Actor1CountryCode'], row['Actor1EthnicCode'], O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)
                AC2_GUID = self.DB.insertObject('Actor', 'GDELT', row['Actor2Name'], row['Actor2Code'], row['Actor2CountryCode'], row['Actor2EthnicCode'], O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)            

                # Make an object for the measures
                found = False
                for GS in GSMeasures:
                    try:
                        if float(E_CLASS1) > 0:
                            O_CLASS1 = 'Positive'
                        else:
                            O_CLASS1 = 'Negative'
                    except:
                        O_CLASS1 = 'Neutral'
                        
                    if O_CLASS1 == GS['CLASS1']:
                        GS_GUID = GS['GUID']
                        
                if found == False:
                        GS_GUID = self.DB.insertObject('Measure', 'Political Stablility', 'Goldstein scale based on -10 to 10', O_CLASS1, 'Goldstein', 20, O_ORIGIN, O_ORIGINREF, O_LOGSOURCE)	
                        GSMeasures.append({'GUID' : GS_GUID, 'CLASS1' : O_CLASS1})
                
                self.DB.insertRelation(E_GUID, 'Event', "HasAttribute", GS_GUID, 'Object')
                self.DB.insertRelation(E_GUID, 'Event', "Involves", AC1_GUID, 'Object')
                self.DB.insertRelation(E_GUID, 'Event', "Involves", AC2_GUID, 'Object')    
                if qVars != None:
                    if len(E_DESC) < 5000:
                        try:
                            TA_VIEW, qVars = self.DB.TextAnalytics('EXTRACTION_CORE_PUBLIC_SECTOR', E_DESC, qVars, self.DB)
                            TA_VIEW, qVars = self.ta_run_counts(TA_VIEW, qVars, intel_id)
                        except Exception as error:
                            print("ERROR %s %s" % (qVars, error))
                    else:
                        i = 0
                        rounds = math.ceil(len(E_DESC)/5000)
                        while i < rounds:                         
                            partialText = E_DESC[:5000]
                            try:
                                i+=1
                                TA_VIEW, qVars = self.DB.TextAnalytics('EXTRACTION_CORE_PUBLIC_SECTOR', partialText, qVars, self.DB)
                                TA_VIEW, qVars = self.ta_run_counts(TA_VIEW, qVars, intel_id)
                                E_DESC = E_DESC[5000:len(E_DESC)] 
                            except Exception as error:
                                print("ERROR %s %s" % (qVars, error))
                                break


    def getLiveGDELT(self, qVars):
        
        
        qVars['Persons'] = '%' + qVars['Persons'] + '%'
        if len(str(qVars['sDTG'])) != 14 and qVars['Type'] != 'Events':
            dtglen = len(str(qVars['sDTG']))
            while len(str(qVars['sDTG'])) < 14:
                qVars['sDTG'] = int((str(qVars['sDTG']) + '0'))
                
        if len(str(qVars['eDTG'])) != 14 and qVars['Type'] != 'Events':
            dtglen = len(str(qVars['eDTG']))
            while len(str(qVars['eDTG'])) < 14:
                qVars['eDTG'] = int((str(qVars['eDTG']) + '0'))  
                
        self.DB.goLive()
        rows = self.gdeltBigQuery(qVars)
        self.ETLGDELT(qVars)
        
    def getGDELTData(self):
        
        for f in os.listdir(self.gdeltPath):
            if f[-4:] == '.csv':
                self.Data = pd.DataFrame.from_csv('%s%s' % (self.gdeltPath, f))
            if f[-5:] == '.xlsx':
                try:
                    self.Data = pd.read_excel('%s/%s' % (self.gdeltPath, f), encoding='latin-1').fillna(0)
                except:
                    self.Data = pd.read_excel('%s/%s' % (self.gdeltPath, f), encoding='utf_8').fillna(0)  
                print('[*] Transformed %s to view.' % (f))
            
            if self.Data is not None:
                self.ETLGDELT(None)
            
        return

# Single File Debugging Command Line
#import OrientModels as ODB
#odb = ODB.OrientModel(None)
#odb.openDB('POLER')
#rss = OsintRSS(odb)

qVars = {'Type' : 'Events',
         'sDTG' : 20160304, 
         'eDTG' : 20180304, 
         'Persons' : 'Salman', 
         'LIMIT' : 1000, 
         'Actor1' : '',
         'ActionGeo' : '',
         'Actor1Code' : 'SAU',
         'Actor2Name' : '',
         'GoldsteinBin' : '>',
         'Language' : 'all'}
#rss.getNewsAPIData()
#rss.gdeltPersons(qVars)
#rss.getGDELTData()
#rss.newsApiGetAll('Y')
#rss.getLiveGDELT(qVars)
#rss.gdeltBigQuery(qVars)



