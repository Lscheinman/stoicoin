# -*- coding: utf-8 -*-
import time, os, json, requests, random, jsonify, shutil
from PIL import Image, ImageFilter, ExifTags
from requests_oauthlib import OAuth2
from requests.auth import HTTPBasicAuth

debugging = False
if debugging == True:
    import OrientModels as om
Locations =[(35.6892, 51.3890),
            (24.7136, 46.6753),
            (-0.1807, -78.4678),
            (48.8566, 2.3522),
            (40.7128, -74.0059),
            (-1.2883, 36.8363),
            (-55.7423, 37.6324),
            (43.0425, -88.0312),
            (51.5074, 0.1278),
            (6.8732, 3.6271),
            (22.2185, 55.3017),
            (42.3985, -83.1212),
            (-10.4806, -66.9036),
            (-33.9598, 18.6202),
            (30.0674, 31.5743),
            (-34.6037, -58.3816),
            (52.4800, -1.8635),
            (52.5200, 13.4050),
            (53.5470, -1.4785),
            (37.9921, 23.7352),
            (39.9529, 32.8947),
            (9.0941, 7.3340)]

class LeonardoModel():

    def __init__(self, ODB):

        self.Verbose = True
        self.setPath()
        self.ODB = ODB

    def setPath(self):
        parentdir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        if '\\' in os.getcwd():
            if debugging == False:
                self.AUTH = ('%s\\application\\services\\config\\AUTH_Leonardo.json' % (os.getcwd()))
                self.Photos = ('%s\\application\\services\\data\\photos\\' % (os.getcwd()))
                self.Static = ('%s\\application\\static\\' % (os.getcwd()))
            else:
                self.AUTH = ('%s\\config\\AUTH_Leonardo.json' % (os.getcwd())) # debugging line
                self.Photos = ('%s\\services\\data\\photos\\' % (parentdir))
                self.Static = ('%s\\static\\' % (parentdir))
        else:
            if debugging == False:
                self.AUTH   = ('%s/application/services/config/AUTH_Leonardo.json' % (os.getcwd()))
                self.Photos = ('%s/application/services/data/photos/' % (os.getcwd()))
                self.Static = ('%s/static/' % (os.getcwd()))
            else:
                self.AUTH   = ('%s/data/AUTH_Leonardo.json' % (os.getcwd())) # debugging line
                self.Photos = ('%s/application/services/data/photos' % (parentdir))
                self.Static = ('%s/static/' % (parentdir))

        f = open(self.AUTH, 'r')
        self.auth = json.load(f)
        f.close()


    def sendRequest(self, URL):
        clientid  = self.auth['clientid']
        clientkey = self.auth['clientsecret']
        url       = self.auth['serviceurls'][URL]
        auth      = HTTPBasicAuth(clientid, clientkey)
        headers   = {'client_id': clientid, 'client_secret': clientkey}
        r = requests.get(url+'/#/default/POST_inference_sync', auth=auth, headers=headers)
        print(r)

    def find_nth(self, haystack, needle, n):
        start = haystack.find(needle)
        while start >= 0 and n > 1:
            start = haystack.find(needle, start+len(needle))
            n -= 1
        return start


    def getPhotoName(self, folder):

        fn = folder.find('_')
        e = len(folder)
        if fn < 0:
            fn = folder.find(' ')

            if fn < 0:
                fn =  e
        if e > 20:
            e = 20

        ln = folder[fn+1:].find('_')
        if ln < 0:
            ln = folder[fn+1:].find(' ')
            if ln < 0:
                ln = e
        FULLNAME = folder[:fn+ln+1]
        if fn > 0:
            FNAME = (folder[:fn].lower()).title()
            LNAME = (folder[fn+1:ln+fn+1].lower()).title()
            newfolder = '%s_%s' % (FNAME.lower(), LNAME.lower())
        else:
            FNAME = LNAME = newfolder = folder[:e]

        return FNAME, LNAME, newfolder


    def getPhotos(self):

        response = {'photos' : [], 'folders' : []}

        if '\\' in os.getcwd():
            fdiv = '\\'
        else:
            fdiv = '/'
        print(self.Photos)
        pdir = os.listdir(self.Photos)
        for p in pdir:

            if os.path.isfile(self.Photos + p):
                FNAME, LNAME, folder = self.getPhotoName(p)
                if not os.path.exists(self.Photos + folder):
                    os.mkdir(self.Photos + folder)
                    response['folders'].append(folder)
                cpath = self.Photos + p
                fpath = self.Photos + folder + fdiv + p
                os.rename(cpath, fpath)
                response['photos'].append(p)

        return response

    def getFolderPhotos(self, folder):
        '''
        Single Folder process
        create persons and objects based on the folders containing photos of the subject
        moves all the photos from their labeled folders to the static folder for viewing in the UI
        '''
        response = {'photos' : [], 'folders' : []}
        if '\\' in os.getcwd():
            fpath = self.Photos + folder + '\\'
        else:
            fpath = self.Photos + folder + '/'

        FNAME, LNAME, folderNotUsed = self.getPhotoName(folder)

        FULL  = '%s %s' % (FNAME, LNAME)
        DOB   = '1900-01-01'
        POB   = folder
        GEN   = 'U'
        ORIGIN = 'Leonardo_getPhotos'
        LOGSOURCE = 'C1'
        DESC = 'Person created from photo extraction process sourced at %s' % fpath
        GUID  = self.ODB.insertPerson(GEN, FNAME, LNAME, DOB, POB, ORIGIN, ORIGIN, LOGSOURCE, DESC)

        # Cycle through all the photos
        for f in os.listdir(fpath):
            if folder not in response['folders']:
                response['folders'].append(folder)
            response['photos'].append(f)
            # Create photo location
            L = random.choice(Locations)
            XCOORD = L[0]
            YCOORD = L[1]
            lGUID = self.ODB.insertLocation('Registration Station', 'Registration', XCOORD, YCOORD, (XCOORD + YCOORD), GEN, ORIGIN, ORIGIN, LOGSOURCE)

            # Create photo event
            YYYY = random.randint(1980, 2016)
            MM   = random.randint(1, 12)
            DD   = random.randint(1, 28)
            DATE = '%d-%d-%d' % (YYYY, MM, DD)
            HH   = random.randint(1, 12)
            MM   = random.randint(10, 59)
            TIME = '%d:%d:00' % (HH, MM)
            DTG = ('%s%s' % (DATE, TIME)).replace(":", "").replace("-", "")
            eGUID = self.ODB.insertEvent('Registration', 'Photo', 'Photo taken of %s taken on %s.' % (FULL, DATE), 'en', (XCOORD + YCOORD), TIME, DATE, DTG, XCOORD, YCOORD, ORIGIN, ORIGIN, LOGSOURCE)

            ipath  = '%s%s' % (fpath, f)
            im     = Image.open(ipath)
            O_DESC = '%s%s' % (self.Static, f)
            CLASS1 = FULL
            CLASS2 = '%s %s' % (im.size[0], im.size[1])
            CLASS3 = "'/static/%s'" % f

            oGUID = self.ODB.insertObject('Photo', 'Person', O_DESC, CLASS1, CLASS2, CLASS3, ORIGIN, GEN, LOGSOURCE)
            im.close()
            os.rename(ipath, O_DESC) # Move the file to the static folder to be served on the site
            self.ODB.insertRelation(GUID, 'Person', 'ReferenceLink', oGUID, 'Object')
            self.ODB.insertRelation(GUID, 'Person', 'LocatedAt', lGUID, 'Location')
            self.ODB.insertRelation(eGUID, 'Event', 'OccurredAt', lGUID, 'Location')
            self.ODB.insertRelation(eGUID, 'Event', 'CreatedOn', oGUID, 'Object')
        self.ODB.client.tx_commit()
        return response

    def getPersonPhotos(self):
        '''
        Automated Process to clear all photos
        create persons and objects based on the folders containing photos of the subject
        moves all the photos from their labeled folders to the static folder for viewing in the UI
        '''
        response = {'photos' : [], 'folders' : []}
        for folder in os.listdir(self.Photos):
            r = self.getFolderPhotos(folder)
            response['photos'] = response['photos'] + r['photos']
            response['folders'] = response['folders'] + r['folders']

        self.ODB.client.tx_commit()

#LM = LeonardoModel(om.OrientModel(None))
#URL = 'SCENE_TEXT_RECOGNITION_URL'
#LM.sendRequest(URL)
#LM.getPersonPhotos()
#LM.getPhotos()
