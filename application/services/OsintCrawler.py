#!/usr/bin/env python3
# -*- coding: utf-8 -*
import requests, os, time, random
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as cOptions
from selenium.webdriver.firefox.options import Options as fOptions
from selenium.webdriver.common.keys import Keys

debugging = False
class Crawler():
    
    def __init__(self, iObj):
        
        self.setPath()
        self.data = {}
        self.startURL       = iObj['startURL'][0] + iObj['searchLanguage'][0]
        self.setSearch(iObj)
        self.data['links']  = []
        self.showNavigation = iObj['showNavigation'][0]
        self.searchLanguage = iObj['searchLanguage'][0]
        self.searchDepth    = iObj['searchDepth'][0]
    
    def getAttributes(self):
        print('startURL: %s\nshowNavigation: %s\n' % (self.startURL, self.showNavigation))
    
    def setSearch(self, iObj):
        
        if iObj['searchTerms'][0] != '':
            self.searchString = iObj['searchTerms'][0]
            self.searchTerms = self.searchString.split()

            if 'aljazeera' in iObj['startURL'][0]:
                self.searchURL = self.startURL + '/search?q='
                urlAnd = '20%'
            else:
                self.searchURL = self.startURL + '/search/?s='
                urlAnd = '+'
            i = 1
            for t in self.searchTerms:
                if i == len(self.searchTerms):
                    self.searchURL = '%s%s' % (self.searchURL, t)
                else:
                    self.searchURL = '%s%s%s' % (self.searchURL, t, urlAnd)
                i+=1
            self.data['startURL'] = self.startURL
            self.data['searchURL'] = self.searchURL
        else:
            self.searchURL = None

    def setPath(self):
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
    
    def startFireFox(self):
        self.driverType = 'FF'
        if self.showNavigation == 'false':
            firefox_options = fOptions()
            firefox_options.add_argument("-headless")
            self.FFdriver = webdriver.Firefox(executable_path=self.firefoxPath, firefox_options=firefox_options)
        else:
            self.FFdriver = webdriver.Firefox(executable_path=self.firefoxPath)
            
        self.ActiveDriver = self.FFdriver
        self.ActiveDriver.get(self.startURL)
    
    def stopFireFox(self):
        self.FFdriver.quit()
    
    def stopDriver(self):
        self.ActiveDriver.quit()
                
    def startChrome(self):
        self.driverType = 'CH'
        chrome_options = cOptions()
        chrome_options.add_argument('--no-sandbox')
        if self.showNavigation == 'false':
            chrome_options = cOptions()
            chrome_options.add_argument("--headless")
            print("headless")
            self.chromeDriver = webdriver.Chrome(executable_path=self.chromePath, chrome_options=chrome_options)
        else:  
            self.chromeDriver = webdriver.Chrome(executable_path=self.chromePath)
        
        self.ActiveDriver = self.chromeDriver    
        self.chromeDriver.get(self.startURL)
        
    
    def getText(self):
        text = self.ActiveDriver.find_elements_by_class_name('text')
        self.data['firstPagetext'] = []
        for t in text:
            self.data['firstPagetext'].append(t.text)        
    
    def getLinks(self):
        
        time.sleep(random.randint(1,3))
        
        if 'aljazeera' in self.startURL:
            links = self.ActiveDriver.find_elements_by_tag_name('a')
        else:
            links = self.ActiveDriver.find_elements_by_xpath('.//a')
        for l in links:
            Link = {'text' : l.text, 'link' : l.get_attribute('href')}
            if Link['text'].count(' ') > 2 and Link['link'] not in self.data['links'] and Link['link'] != None:
                if Link['text'] != 'Art, Graphics & Video':
                    self.data['links'].append(Link)
                print("!!LINK: %s" % Link)
        self.linkcount = len(self.data['links'])
        print(self.linkcount)
    
    def getSearch(self):
        
        if 'aljazeera' in self.startURL:
            search = self.ActiveDriver.find_element_by_class_name('searchbtnlink')
            search.click() 
            time.sleep(1)
            searchInput = self.ActiveDriver.find_element_by_id('searchInput')            
        else:
            search = self.ActiveDriver.find_element_by_id('search')
            search.click() 
            time.sleep(1)
            searchInput = self.ActiveDriver.find_element_by_class_name('searhmain')           
        if search:

            time.sleep(1)
            searchInput.send_keys(self.searchString)
            searchInput.send_keys(Keys.ENTER) 
            time.sleep(random.randint(1,3))
        else:
            print("No search found")
    
    def getGraphics(self):
        
        images = self.data['images'] = []
        for l in self.data['links']:
            if l['text'] == 'infographic':
                self.ActiveDriver.get(l['link'])
                images = self.ActiveDriver.find_elements_by_class_name('img-responsive')
        if len(images) > 0:
            for i in images:
                image = {}
                image['src'] = i.get_attribute('src')
                image['text'] = i.get_attribute('alt')
                self.data['images'].append(image)
                  
    def search(self):
        self.ActiveDriver.get(self.searchURL)
        s = self.ActiveDriver.find_element_by_xpath('''//*[@id="search-page"]/div[1]/div[1]/label''')
        button = self.ActiveDriver.find_element_by_xpath('''//*[@id="search-page"]/div[1]/div[3]/button''')
        s.click()
        s = self.ActiveDriver.find_element_by_id('field-s').send_keys('go')
        
    def getSelectionShareable(self):
        
        e = {}
        try:
            texts = self.ActiveDriver.find_elements_by_class_name('selectionShareable')
            story = texts[0].text
            i+=1
            for t in texts:
                if t.text not in story:
                    story = story + ' ' + t.text
                    if '.201' in t.text and i < 5:
                        dtg = t.text.find('2018')
                        e['date'] = t.text[dtg-6:dtg+5]
                i+=1
            e['fulltext'] = story.replace('\n', ' ').replace("'", '').replace('"', '')                  
        except:
            pass
        
        return e['fulltext']
        
    
    def getSearchURL(self, search):
        arabic = True
        i = 1
        for sw in self.searchTerms:
            if len(self.searchTerms) == i:
                if arabic == True:
                    search = '%s%s' % (sw, search)
                else:
                    search = '%s%s' % (search, sw)
            else:
                if arabic == True:
                    search = '%s%s' % (search, sw) + '20' + '%'
                else:
                    search = '%s%s20%' % (search, sw)
            i+=1 
        return search
    
    def getURL(self, url):
        u = requests.get(url)
        soup = BeautifulSoup(u.content, "xml")
        paras = soup.findAll('p')
        links = soup.findAll('a')
        
        return {'p' : paras, 'l' : links}
        
    
    def getSecondDegree(self):
        
        story = {'title' : ''}
        i = 0
        visited = []
        for e in self.data['links']:
            i = 0
            if e['link'] not in visited:
                visited.append(e['link'])
                try:
                    self.ActiveDriver.get(e['link'])
                    try:
                        closeD = self.ActiveDriver.find_element_by_id('ensCloseBanner')
                        closeD.click()
                    except:
                        pass
                    try:
                        closeD = self.ActiveDriver.find_element_by_id('ensNotifyBanner')
                        closeD.click()
                    except:
                        pass   
                    if 'aljazeera' in self.startURL:
                        if self.driverType == 'CH':
                            texts = self.ActiveDriver.find_elements_by_id('skip')
                            #$0.textContent
                        if len(texts) == 0:
                            texts = self.ActiveDriver.find_elements_by_id('DynamicContentContainer')
                        if len(texts) == 0:
                            texts = self.ActiveDriver.find_elements_by_tag_name('span')                        
                    
                    elif 'nairaland.com' in self.startURL:
                        e['user'] = self.ActiveDriver.find_element_by_class_name('user').text
                        e['date']  = self.ActiveDriver.find_element_by_class_name('s').text
                        e['fulltext'] = self.ActiveDriver.find_element_by_class_name('narrow').text.replace('\n', ' ').replace("'", '').replace('"', '')  
                        print(e)
                    else:
                        texts = self.ActiveDriver.find_elements_by_class_name('selectionShareable')
                    
                    if 'nairaland.com' not in self.startURL:
                        try:
                            paras = BeautifulSoup(requests.get(self.ActiveDriver.current_url).content, 'lxml').findAll('p')
                        except:
                            paras = texts
                        story = texts[0].text
                        i+=1
                        for t in paras:
                            if t.text not in story:
                                story = story + ' ' + t.text
                                if '.2018' in t.text and i < 5:
                                    dtg = t.text.find('2018')
                                    e['date'] = t.text[dtg-6:dtg+5]
                            i+=1
                        e['fulltext'] = story.replace('\n', ' ').replace("'", '').replace('"', '')                  
                except:
                    pass

        return '%d texts from %d links' % (i, len(self.data['links']))
    
    def stopChrome(self):
        self.chromeDriver.quit()

# 'http://www.aljazeera.net/portal'
# 'http://aa.com.tr/'
# ''
CRAWL = {}
CRAWL['searchDepth'] = ['Single']
CRAWL['startURL'] = ['https://www.nairaland.com/']
#CRAWL['startURL'] = ['https://www.aljazeera.com/']
#CRAWL['searchTerms'] = ['محمد بن سلمان']
CRAWL['searchTerms'] = ['']
CRAWL['searchLanguage'] = ['']
CRAWL['searchURL'] = ['%s%s/search' % (CRAWL['startURL'], CRAWL['searchLanguage'] )]
CRAWL['searchURL'] = 'http://www.aljazeera.net/home/search?q=%D9%85%D8%AD%D9%85%D8%AF%20%D8%A8%D9%86%20%D8%B3%D9%84%D9%85%D8%A7%D9%86'
CRAWL['showNavigation'] = ['true']
#c = Crawler(CRAWL)
#url = c.getURL(c.startURL)
#surl = c.getURL(c.searchURL)
#print(surl)
#c.startFireFox()
#c.startChrome()

#c.getLinks()
#c.getSearch()
#c.getGraphics()
#c.getSecondDegree()
#print(c.data)  
#c.search(CRAWL['searchURL'], CRAWL['searchTerms'])

    
                
    
