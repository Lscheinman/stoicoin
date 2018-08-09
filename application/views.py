'''
Flask application views (routes).
'''
import os, time, csv, requests, base64
from flask import Flask, request, session, redirect, url_for, render_template, flash, send_file, jsonify
from werkzeug.utils import secure_filename
from application import flask, app
from functools import wraps
from application import models
from application.models import (User, get_users, get_locations, get_PIR, get_STR, get_tasks, todays_recent_intel, tileStats, 
                                todays_recent_posts, todays_recent_searches, recent_all, 
                                recent_PIR, dashboard, get_uploads, get_entity_profile, recent_OSINT, get_TAruns)
from datetime import datetime
from threading import Thread
threads = []
listlimit = 5
UPLOAD_FOLDER = '%s' % (os.getcwd())
ALLOWED_EXTENSIONS = set(['txt', 'csv', 'xlsx', 'xls', 'jpg', 'jpeg', 'png', 'gif'])
HANA = True

def check_user():      
    username = session.get("username")
    
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    #print("[%s_APP-View-check_user]: %s-%s" % (TS, username, session))      
    if not username:
        return False
    else:
        flask.g.user = username
        return True   

@app.route("/oldindex")
def oldindex():
    
    posts = todays_recent_posts(listlimit)
    intel = todays_recent_intel(listlimit)
    recent = recent_all()
    users = get_users()
    dash = dashboard(None)
    return render_template("index.html", posts=posts, intel=intel, recent=recent, users=users, dash=dash)

@app.route("/register", methods=["POST", "GET"])
def register():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        email    = request.form["email"]
        tel      = request.form["tel"]
        image    = request.form["image"]
        location = request.form["location"]
        utype    = request.form["utype"]
        user = User(username)

        if not user.register(password, email, tel, location, image, utype):
            flash("A user with that username already exists.")
        
        else:
            flash("Successfully registered.")
            return redirect(url_for("login"))

    return render_template("register.html")

@app.route("/register_proxy", methods=["POST", "GET"])
def register_proxy():
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-register_proxy]: starting with %s" % (TS, iObj))    
    username = iObj["username"][0]
    password = iObj["password"][0]
    email    = iObj["email"][0]
    tel      = iObj["tel"][0]
    image    = iObj["image"][0]
    location = iObj["location"][0]
    utype    = iObj["utype"][0]
    print("[%s_APP-View-register_proxy]: %s" % (TS, iObj)) 
    user = User(username)  
   
    
    if not user.register(password, email, tel, location, image, utype):
        MSG = {'messages' :["A user with that username already exists."]}
        print(MSG)
        return(jsonify(MSG['messages'][0]))  
    else:
        MSG = {'messages' :[{'ROLE' : utype,
                             'DESC' : 'Username %s of type %s created on %s can be reached at %s' % (username, utype, TS, email),
                             'GUID' : user.GUID,
                             'EMAIL' : email,
                             'NAME' : username}]}
        print(MSG)

        return jsonify(MSG['messages'][0])    
    
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["ID"]
        password = request.form["PW"]
        user = User(username)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-login]: Verification..." % (TS))       

        if not user.verify_password(password):
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_APP-View-login]: ...fail" % (TS))             
        else:
            session["username"] = user.username
            session["email"] = user.email
            session["tel"] = user.tel
            session['location'] = user.location
            session['utype'] = user.utype
            
            TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("[%s_APP-View-login]: ...success %s-%s" % (TS, flask.g, session.get("username")))              
            
            message = {'message' : 'Welcome %s. Please wait while your authorized view is loaded.' % session["username"], 'status' : 'success'}
                     
            return jsonify(message)
    
    message = {'message' : 'Password for user %s was not verified' % username, 'success' : 'fail'}

    return jsonify(message)


@app.route("/add_watchlist", methods=["POST"])
def add_watchlist():
    
    if check_user() == True:
        listname = request.form["listname"]
        terms = request.form["terms"]
        names = request.form["names"]
        locations = request.form["locations"]   
        events = request.form["events"] 

        return redirect(url_for("index"))
    
    else:
        flash("Log in to access Watchlist functionality.")
        return redirect(url_for("login"))         

@app.route("/add_person", methods=["POST"])
def add_person():
    
    iObj = request.form.to_dict(flat=False) 
    
    if check_user() == True:     
    
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        iObj['iType']       = 'Person'
        iObj['FName']       = request.form["pFNAME"]
        iObj['LName']       = request.form["pLNAME"]
        iObj['GEN']         = request.form["pGEN"]
        iObj['POB']         = request.form["pPOB"]
        iObj['DOB']         = request.form["pDOB"] 
        iObj['Description'] = request.form["pDESC"] 
        iObj['ORIGIN']      = request.form['PIRREF']
        iObj['ORIGINREF']   = ('COIN%s%s%s%s' % (iObj['FName'], iObj['LName'], iObj['GEN'], iObj['DOB'])).replace(" ", "")
        iObj['LOGSOURCE']   = 'OSINT'
        newIntel = user.add_intel(iObj)
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_person]: Received: %s %s" % (TS, type(iObj), iObj))        
    
        return jsonify(newIntel)
    else:
        flash("Log in to access adding intel functionality.")
        return redirect(url_for("login"))      
    
@app.route("/add_object", methods=["POST"])
def add_object():
    
    iObj = request.form.to_dict(flat=False) 
    
    if check_user() == True:     
        
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        iObj = {}
        iObj['iType']       = 'Object'
        iObj['oTYPE']       = request.form["oTYPE"]
        iObj['oCATEGORY']   = request.form["oCATEGORY"]
        iObj['oCLASS1']     = request.form["oCLASS1"]
        iObj['oCLASS2']     = request.form["oCLASS2"]
        iObj['oCLASS3']     = request.form["oCLASS3"]  
        iObj['oDESC']       = request.form["oDESC"] 
        iObj['Locations']   = request.form["oLOC"]
        iObj['ORIGIN']      = request.form['PIRREF']
        iObj['ORIGINREF']   = ('COIN%s%s%s' % (user.GUID, iObj['ORIGIN'], iObj['oDESC'])).replace(" ", "")
        iObj['LOGSOURCE']   = 'COIN' 
        iObj['Description'] = iObj['oDESC'] 
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_object]: Received: %s %s" % (TS, type(iObj), iObj))          
        newIntel = user.add_intel(iObj)

        return jsonify(newIntel)
    else:
        flash("Log in to access adding intel functionality.")
        return redirect(url_for("login"))       
   
@app.route("/add_location", methods=["POST"])
def add_location():
    
    iObj = request.form.to_dict(flat=False) 
    
    if check_user() == True:    
        
        today = datetime.now().strftime("%F")
        user = User(session["username"])        
        iObj = {}
        iObj['iType']       = 'Location'
        iObj['lTYPE']       = request.form["lTYPE"]
        iObj['lCLASS1']     = request.form["lCLASS1"]
        iObj['lXCOORD']     = request.form["lXCOORD"]
        iObj['lYCOORD']     = request.form["lYCOORD"]
        iObj['lZCOORD']     = request.form["lZCOORD"]
        iObj['lDESC']       = request.form["lDESC"]  
        iObj['Locations']   = request.form['lLOC']
        iObj['ORIGIN']      = request.form['PIRREF']
        iObj['ORIGINREF']   = ('COIN%s%s%s' % (user.GUID, iObj['ORIGIN'], iObj['lDESC'])).replace(" ", "")
        iObj['LOGSOURCE']   = 'COIN' 
        iObj['Description'] = iObj['lDESC'] 
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_object]: Received: %s %s" % (TS, type(iObj), iObj))          
        newIntel = user.add_intel(iObj)
    
        return jsonify(newIntel)
    else:
        flash("Log in to access adding intel functionality.")
        return redirect(url_for("login"))       
    
@app.route("/add_event", methods=["POST"])
def add_event():
    
    iObj = request.form.to_dict(flat=False) 
    
    if check_user() == True:  
        
        today = datetime.now().strftime("%F")
        user = User(session["username"])        
        iObj = {}
        iObj['iType']       = 'Event'
        iObj['eTYPE']       = request.form["eTYPE"]
        iObj['eCATEGORY']   = request.form["eCATEGORY"]
        iObj['eDESC']       = request.form["eDESC"]
        iObj['eCLASS1']     = request.form["eCLASS1"]
        iObj['eDATE']       = request.form["eDATE"]  
        iObj['eTIME']       = request.form["eTIME"] 
        iObj['Locations']   = request.form['eLOC']
        iObj['ORIGIN']      = request.form['PIRREF']
        iObj['ORIGINREF']   = ('COIN%s%s%s' % (user.GUID, iObj['ORIGIN'], iObj['eDESC'])).replace(" ", "")
        iObj['LOGSOURCE']   = 'COIN' 
        iObj['Description'] = iObj['eDESC'] 
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_event]: Received: %s %s" % (TS, type(iObj), iObj))          
        newIntel = user.add_intel(iObj)
    
        return jsonify(newIntel)
    else:
        flash("Log in to access adding intel functionality.")
        return redirect(url_for("login"))       

@app.route("/merge_entities", methods=["POST"])
def merge_entities():
    
    iObj = request.form.to_dict(flat=False)      
    
    if check_user() == True:   
        
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        iObj = {}
        iObj['iType']       = 'Relation'
        iObj['AGUID']       = request.form["AGUID"]
        iObj['PGUID']       = request.form["PGUID"]
        iObj['OGUID']       = request.form["OGUID"]
        iObj['R_DESC']      = request.form["R_DESC"]
        iObj['ORIGIN']      = today
        iObj['ORIGINREF']   = 'COIN-%s' % user.GUID
        iObj['LOGSOURCE']   = 'COIN' 
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-merge_entities]: Received: %s %s" % (TS, type(iObj), iObj))     
        message = user.merge_entities(iObj)
    
        return jsonify(message)
    
    else:
        
        return None   
    
@app.route("/add_relation", methods=["POST"])
def add_relation():
    
    iObj = request.form.to_dict(flat=False) 
    
    if check_user() == True:   
        
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        iObj = {}
        iObj['iType']   = 'Relation'
        iObj['pAGUID']  = request.form["pAGUID"]
        iObj['pBGUID']  = request.form["pBGUID"]
        iObj['oAGUID']  = request.form["oAGUID"]
        iObj['oBGUID']  = request.form["oBGUID"]      
        iObj['AGUIDfree']   = request.form["AGUIDfree"]
        iObj['BGUIDfree']   = request.form["BGUIDfree"]
        iObj['Description'] = request.form["rDESC"]
        iObj['RELTYP']      = request.form["RELTYP"] 
        iObj['ORIGIN']      = today
        iObj['ORIGINREF']   = 'COIN-%s' % user.GUID
        iObj['LOGSOURCE']   = 'COIN' 
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_relation]: Received: %s %s" % (TS, type(iObj), iObj))          
        user.add_intel(iObj)
    
        AUTH, TILESTATS, recentPIR, USERS, TASKS = indexFill()
        MENUS = user.menus()
        return render_template("index.html", MENUS=MENUS, AUTH=AUTH, recentPIR=recentPIR, TILESTATS=TILESTATS, TASKS=TASKS, USERS=USERS) 

    else:
        flash("Log in to access adding intel functionality.")
        return redirect(url_for("login"))       

@app.route("/run_youtube", methods=["POST"])
def run_youtube():
    
    if check_user() == True: 
        
        user = User(session["username"])  
        
        searchtype = request.form.getlist('searchtype')
        searchterm = request.form["searchterm"] 
        radius     = request.form["radius"] 
        latitude   = request.form["latitude"]
        longitude  = request.form["longitude"] 
        PIRREF     = request.form["PIRREF"]
        
        messages = user.run_youtube(PIRREF, searchtype, searchterm, latitude, longitude, radius)
        for m in messages:
            flash(m, 'success')
        
        OSINT = recent_OSINT()
        return render_template("CO.html", OSINT=OSINT)
    
    else:
        flash("Log in to access COIN functionality.")
        return redirect(url_for("login"))       

@app.route("/run_twitter", methods=["POST"])
def run_twitter():

    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_twitter]: Received: %s %s" % (TS, type(iObj), iObj))     
    if check_user() == True:  
       
        user = User(session["username"]) 
    
        searchtype = request.form['searchtype']
        searchterm = request.form["searchterm"]
        latitude   = request.form["latitude"]
        longitude  = request.form["longitude"] 
        PIRREF = origin = request.form['PIRREF']
                      
        messages = user.run_twitter(PIRREF, searchtype, searchterm, latitude, longitude, session["username"], origin)
        return jsonify(messages)
    
    else:
        flash("Log in to access OSINT functionality.")
        return redirect(url_for("login")) 

@app.route("/run_crawler", methods=['POST'])
def run_crawler():
    iObj = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_crawler]: Received: %s %s" % (TS, type(iObj), iObj))   
    user = User(session["username"]) 
    message = user.run_crawler(iObj)
    
    return jsonify(message)

@app.route("/run_gdelt", methods=["POST"])
def run_gdelt():
    
    iObj = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_gdelt]: Received: %s %s" % (TS, type(iObj), iObj))   
    user = User(session["username"]) 
    message = user.run_gdelt(iObj)
    
    return jsonify(message)
    
@app.route("/run_acled", methods=["POST"])
def run_acled():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_acled]: Received: ACLED run %s" % (TS, iObj))     
    if check_user() == True:  
       
        user = User(session["username"]) 
        try:
            searchdate     = request.form['searchdate']
        except:
            searchdate = ''
        try:
            searchlocation = request.form["searchlocation"]
        except:
            searchlocation = ''
        try:
            PIRREF         = request.form['PIRREF']
        except:
            PIRREF = ''
                      
        messages = user.run_acled(PIRREF, searchdate, searchlocation)
        print(messages)
        return jsonify(messages)
    
@app.route("/run_ucdp", methods=["POST"])
def run_ucdp():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_ucdp]: Received: UCDP run %s" % (TS, iObj))      
       
    user       = User(session["username"]) 
    startDate  = iObj['startDate'][0][:10]
    endDate    = iObj["startDate"][0][-10:]
    countries  = iObj["countries"][0]
    geography  = iObj["geography"][0]   
    PIRREF     = iObj["PIRREF"][0] 
    print("WHAT", PIRREF, startDate, endDate, countries, geography)
                  
    messages = user.run_ucdp(PIRREF, startDate, endDate, countries, geography)
    print(messages)
    return jsonify(messages)
    

@app.route("/run_ta", methods=["POST"])
def run_ta():
    
    iObj = request.form.to_dict(flat=False) 
    text = iObj['dtext'][0]
    TA_CONFIG = iObj['submit'][0]
    
    if check_user() == True:
        user = User(session["username"])
        TA_Profile = user.run_ta(text, TA_CONFIG)
        print(TA_Profile)
        return jsonify(TA_Profile)
    
    else:
        return redirect(url_for("login"))  
    
@app.route("/run_vulchild_recalc", methods=["POST"])
def run_vulchild_recalc():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-run_vulchild_recalc]: Received: %s %s" % (TS, type(iObj), iObj)) 
    if check_user() == True:
        user = User(session["username"])
        if iObj['LOW'][0] > iObj['HIGH'][0]:
            t = iObj['LOW'][0] 
            iObj['LOW'][0] = iObj['HIGH'][0]
            iObj['HIGH'][0] = t
        results = user.run_vulchild_recalc(iObj['LOW'][0], iObj['HIGH'][0])
        if results:
            return jsonify(results)
    else:
        message = {'message' : 'User not logged in'}
        return jsonify(message)    

@app.route("/from_file", methods=["GET", "POST", "PUT"])
def from_file():
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    if request.method == "POST":
    
        iObj = request.form.to_dict(flat=False) 
        
        print("[%s_APP-View-from_file]: Received: %s\n%s\n%s\n%s" % (TS, type(iObj), iObj['size'], iObj['name'], iObj['type'])) 
        base64url = iObj['result'][0]
        #File = base64.b64decode(requests.get(base64url).content)

        '''
        
        with open('%s\\%s' % (os.getcwd(), iObj['name'][0]), 'w', newline= '') as file:
            writer = csv.writer(file, delimiter=',')
            for t in iObj['result']:
                writer.writerow(t)
        file.close()
        print(file)
        '''      
        return jsonify(iObj)
    
    elif request.method == "PUT":
        iObj = request.files
        print("[%s_APP-View-from_file]: Received: %s\n%s" % (TS, type(iObj), iObj)) 
        return jsonify(iObj)

@app.route("/process_photos", methods=["POST"])
def process_photos():
    
    user = User(session["username"])
    message = {'response' : 200}
    message['text'] = user.process_photos()
    
    return jsonify(message)

@app.route("/upload_file", methods=["GET", "POST"])
def upload_file():
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    if request.method == "POST":
        user = User(session["username"])
        file = request.files['file']
        filename = secure_filename(file.filename)
        cwd = os.getcwd()
        photos = ['jpg', 'png', 'bmp', 'gif', 'pdf']
        documents = ['doc', 'xls', 'lsx']
        if filename[-3:] in photos:
            ftype = 'photos'
        elif filename[-3:] in documents:
            ftype = 'documents'
            
        if '\\' in cwd:
            path = os.path.join(app.config['UPLOAD_FOLDER'], '%s\\application\\services\\data\\%s\\%s' % (cwd, ftype, filename))
        else:
            path = os.path.join(app.config['UPLOAD_FOLDER'], '%s/application/services/data/%s/%s' % (cwd, ftype, filename))
        
        file.save(path)
        
        message = {'response' : 200, 'text' : "%s loaded to %s at %s" % (filename, path, TS)}
        print(message)
        #user.from_file(filename, fileType, fileURL)
        return render_template("upload_file.html")
    
    else:
        return render_template("upload_file.html")
    
@app.route("/etl_file", methods=["GET", "POST"])
def etl_file():
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    if request.method == "POST":
        iObj = request.form.to_dict(flat=False) 

        user = User(session["username"])

        file = request.files['file']
        fileType = request.form['fileType']  
        filename = secure_filename(file.filename)
        cwd = os.getcwd()
        photos = ['jpg', 'png', 'bmp', 'gif', 'pdf']
        documents = ['doc', 'xls', 'lsx']
        if filename[-3:] in photos:
            ftype = 'photos'
        elif filename[-3:] in documents:
            ftype = 'documents'
            
        if '\\' in cwd:
            path = os.path.join(app.config['UPLOAD_FOLDER'], '%s\\application\\services\\data\\%s' % (cwd, filename))
        else:
            path = os.path.join(app.config['UPLOAD_FOLDER'], '%s/application/services/data/%s' % (cwd, filename))
        
        file.save(path)
        user.from_file(filename, fileType, path, user.GUID)
           

        message = {'response' : 200, 'text' : "%s loaded to %s at %s" % (filename, path, TS)}

        return render_template("etl_file.html")
    
    else:
        return render_template("etl_file.html")
    

@app.route("/user_tokens", methods=["POST"])
def user_tokens():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-user_tokens]: Received: %s %s" % (TS, type(iObj), iObj)) 
    
    if check_user() == True:
        user = User(session["username"]) 
        message = user.user_tokens(iObj, session["username"])
        
    else:
        flash("Log in to access collection functionality.")
        return redirect(url_for("login"))     
        
    return message


@app.route("/user_systems", methods=["POST"])
def user_systems():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-user_tokens]: Received: %s %s" % (TS, type(iObj), iObj)) 
    
    if check_user() == True:
        user = User(session["username"]) 
        message = user.user_systems(iObj, session["username"])
        
    else:
        flash("Log in to access collection functionality.")
        return redirect(url_for("login"))     
        
    return message

@app.route("/from_SPF", methods=["GET", "POST"])
def from_SPF():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-from_SPF]: Received: %s %s" % (TS, type(iObj), iObj))  
    user = User(session["username"])
    message = user.from_SPF(iObj)
    print(message)
        
    return jsonify(message)


@app.route("/polerize", methods=["POST"])
def polerize():
    
    file = request.files['file']
    filename = secure_filename(file.filename)
    
    print('POLERIZE')
    message = {'response' : 200}
    return jsonify(message)

@app.route("/from_HSS", methods=["GET", "POST"])
def from_HSS():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-from_HSS]: Received: %s %s" % (TS, type(iObj), iObj))  
    user = User(session["username"])
    message = user.from_HSS(iObj)
    print(message)
        
    return jsonify(message)


@app.route("/logout", methods=["POST"])
def logout():
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    message = '%s logged out as of %s' % (session['username'], TS)
    session.pop("username")
    session.pop("utype")
    print(message)
    return jsonify({'message' : message})

def indexFill():
    
    AUTH = {'DIR' : ['Admin', 'Manager', 'Director', 'Analyst'],
            'DIT' : ['Admin', 'Analyst', 'Field', 'Manager', 'Director'],
            'STR' : ['Admin', 'Manager', 'Director'],
            'CO'  : ['Admin', 'Analyst', 'Field', 'Manager'],
            'CF'  : ['Admin', 'Analyst', 'Field', 'Manager','Director'],
            'DET' : ['Admin', 'Analyst', 'Field', 'Manager'],
            'DEP' : ['Admin', 'Analyst', 'Field', 'Manager'],
            'AE'  : ['Admin', 'Analyst', 'Field', 'Manager'],
            'USE' : ['Admin'],
            'POL' : ['Analyst', 'Field', 'Manager', 'Director'],
            'PL2' : ['Admin', 'Analyst', 'Manager', 'Director'],
            'HSS' : ['Health', 'Social'], # Health and social services
            'ARA' : ['Arabic']
            }        
    
    TILESTATS = tileStats()
    DR = {'Predictive' : 0, 'Geo' : 9, 'Dashboards': 0}
    TILESTATS['DR'] = DR

    recentPIR = recent_PIR() 
    USERS     = get_users()
    TASKS     = get_tasks()
    
    return AUTH, TILESTATS, recentPIR, USERS, TASKS
    
@app.route("/load_model", methods=["POST", "GET"])
def load_model():
    
    if check_user() == True:     
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        print(session.get("username"), '-', session.get("utype"))    
        jModel = user.menus()
    
    else:
        jModel = None
    
    return jsonify(jModel)

@app.route("/load_stats", methods=["POST", "GET"])
def load_stats():
    
    TILESTATS = jsonify(tileStats())
    
    return TILESTATS

@app.route("/load_stored_procedure", methods=["POST", "GET"])
def load_stored_procedure():
    
    iObj = request.form.to_dict(flat=False) 
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-from_file]: Received: %s %s" % (TS, type(iObj), iObj))      
    
    if check_user() == True:
        
        user = User(session["username"])
        message = user.load_stored_procedure(request.form['type'], user.GUID)
    else:
        message = None
        
    return jsonify(message)

@app.route("/")
def index():  
    
    # Load up all data for the application
    AUTH, TILESTATS, recentPIR, USERS, TASKS = indexFill()
    
    if check_user() == True:     
        today = datetime.now().strftime("%F")
        user = User(session["username"])
        print(session.get("username"), '-', session.get("utype"))

    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-index]: current session %s-%s-%s" % (TS, flask.g, session.get("username"), session.get("email"), ))  
    print("[%s_APP-View-index]: TILESTATS %s" % (TS, TILESTATS)) 
    print("[%s_APP-View-index]: USERS %s" % (TS, USERS)) 
    
    return render_template("index.html", AUTH=AUTH, recentPIR=recentPIR, TILESTATS=TILESTATS, TASKS=TASKS, USERS=USERS)

@app.route("/add_pir", methods=["POST"])
def add_pir():
    
    PIR = request.form.to_dict(flat=False)  
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-add_pir]: Received: %s %s" % (TS, type(PIR), PIR))    
    PIR['CATEGORY']   = PIR['CATEGORY'][0]
    PIR['DESC']       = PIR['DESC'][0]
    PIR['CLASS1']     = PIR['CLASS1'][0]
    PIR['CLASS2']     = PIR['CLASS2'][0]
    PIR['CLASS3']     = PIR['CLASS3'][0]
    PIR['Locations'] = []
    PIR['Locations'].append(PIR['Location'][0])  #TODO Need to get multiple locations but UI5 takes too long to render ComboBox
    PIR['References']  = []
    for r in PIR['References']:
        PIR['References'].append(r)   

    if check_user() == True: 
        user = User(session["username"])         
  
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_pir]: Received: %s %s" % (TS, type(PIR), PIR))
        newPIR = user.add_pir(PIR)    
      
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_pir]: current session %s-%s-%s" % (TS, flask.g, session.get("username"), session.get("utype") ))  
        flash("PIR-%s" % newPIR['GUID'])
        
        return jsonify(newPIR)
    
    else:
        flash("Log in to access PIR functionality.")
        return redirect(url_for("login"))      

@app.route("/profile_user", methods=["POST"])
def profile_user():
    
    iObj = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-profile_user]: Received: %s %s" % (TS, type(iObj), iObj))         
    if check_user() == True: 
        user = User(session["username"])       
        Profile = user.get_user_profile(int(iObj['GUID'][0]))
    print(Profile)
    return jsonify(Profile)

@app.route("/delete_user", methods=['POST'])
def delete_user():
    
    iObj = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-profile_user]: Received: %s %s" % (TS, type(iObj), iObj))         
    if check_user() == True: 
        user = User(session["username"])       
        message = user.delete_user(int(iObj['GUID'][0]))
    return jsonify(message['messages'][0])    
    
@app.route("/add_strat", methods=["POST"])
def add_strat():
    
    STR = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-add_pir]: Received: %s %s" % (TS, type(STR), STR))    
    STR['CATEGORY'] = STR['CATEGORY'][0]
    STR['DESC']     = STR['DESC'][0]
    STR['CLASS1']   = STR['CLASS1'][0]
    try:
        STR['Locations'] = STR['Location[]']
    except:
        STR['Locations'] = None
    if check_user() == True: 
        user = User(session["username"])    
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_strat]: Received: %s %s" % (TS, type(STR), STR))        
    
        newSTRAT = user.add_strat(STR)
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_strat]: current session %s-%s-%s" % (TS, flask.g, session.get("username"), session.get("utype") ))      
        
        return jsonify(newSTRAT)

    else:
        flash("Log in to access Strategy functionality.")
        return redirect(url_for("login"))           

@app.route("/load_task_profile", methods=["POST"])
def load_task_profile():
    
    taskProfile = request.form.to_dict(flat=False)
    TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    print("[%s_APP-View-add_pir]: Received: %s %s" % (TS, type(taskProfile), taskProfile))      
    if check_user() == True:    
        user = User(session["username"])    
        task = user.get_task(int(taskProfile['GUID'][0]))
        print(task)
        return jsonify(task)
        
    else:
        flash("Log in to access tasking functionality.")
        return redirect(url_for("login"))  
    
@app.route("/load_VP_entity_profile", methods=["POST"])
def load_VP_entity_profile():
    
    if check_user() == True:    
        user = User(session["username"])    
        Profile = request.form.to_dict(flat=False)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-load_VP_entity_profile]: Received: %s %s" % (TS, type(Profile), Profile))
        if 'LOW' in Profile.keys():
            result = user.get_VP_entity_profile(int(Profile['GUID'][0]), Profile['TYPE'][0], Profile['LOW'][0], Profile['HIGH'][0])
        else:
            result = user.get_VP_entity_profile(int(Profile['GUID'][0]), Profile['TYPE'][0], 0, 0)
        print("[%s_APP-View-load_VP_entity_profile]: Complete with: %s\n%s" % (TS, type(result), result)) 
        return jsonify(result)
        
    else:
        return redirect(url_for("login"))  
    
@app.route("/load_entity_profile", methods=["POST"])
def load_entity_profile():
    
    if check_user() == True:    
        user = User(session["username"])    
        Profile = request.form.to_dict(flat=False)
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-load_entity_profile]: Received: %s %s" % (TS, type(Profile), Profile)) 
        result = user.get_entity_profile(int(Profile['GUID'][0]), Profile['TYPE'][0])
        return jsonify(result)
        
    else:
        flash("Log in to access tasking functionality.")
        return redirect(url_for("login"))  

@app.route("/add_task", methods=["POST"])
def add_task():
    
    TSK = request.form.to_dict(flat=False)
    tasktype = TSK['CLASS1'][0]
    subject  = TSK['DESC'][0]
    tags     = TSK['CLASS3'][0]
    actionUserGUID = TSK['CLASS2'][0]
    PIRGUID = TSK['ORIGIN'][0]
    
    if check_user() == True:    
        user = User(session["username"])
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-add_task]: Received: %s %s" % (TS, type(TSK), TSK))        
        task = user.add_task(tasktype, subject, tags, actionUserGUID, PIRGUID)

        return jsonify(task)
    
    else:
        flash("Log in to access tasking functionality.")
        return redirect(url_for("login"))    

@app.route("/update_user", methods=["POST"])
def update_user():
    
    if check_user() == True:    
        user = User(session["username"])    
        iObj = request.form.to_dict(flat=False) 
        iObj['GUID']  = int(request.form["GUID"])
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print("[%s_APP-View-update_user]: Received: %s %s" % (TS, type(iObj), iObj))      
        message = user.update_user(iObj)
        
        TS = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        print(message)
    
        return jsonify(iObj)

@app.route("/update_condis/<username>")
def update_condis(username):
    
    if check_user() == True:   
        
        user1 = User(session.get("username"))
        t = Thread(target=user1.update_condis)
        t.start()
        
        user2 = User(username)
        posts = user2.recent_posts(listlimit)
        intel = user1.recent_intel(listlimit)
        tasks = user1.recent_tasks()
        searches = user1.recent_searches(listlimit)
    
        if user1.username == user2.username:
            similar = user1.similar_users(3)
            common = []
        else:
            common = user1.commonality_of_user(user2)
            similar = []
    
        return render_template("profile.html", AUTH=AUTH, recentPIR=recentPIR, TILESTATS=TILESTATS, TASKS=TASKS, USERS=USERS, MENUS=MENUS) 
    
    else:
        flash("Log in to access Condis functionality.")
        return redirect(url_for("login"))       

@app.route("/PIRAPP")
def PIRAPP():
    
    if check_user() == True:
        
        user = User(session["username"])  
        Locations = get_locations()
        PIR       = get_PIR()
        STR       = get_STR()
        recentPIR = recent_PIR()
        
        AUTH, TILESTATS, recentPIR, USERS, TASKS = indexFill()
        return render_template("index.html", AUTH=AUTH, recentPIR=recentPIR, TILESTATS=TILESTATS, TASKS=TASKS, USERS=USERS, MENUS=MENUS)
    
    else:
        flash("Log in to access PIR functionality.")
        return redirect(url_for("login"))           

@app.route("/DITAPP")
def DITAPP():
    
    if check_user() == True:      
        users = get_users()
        TASKS = get_tasks()
        return render_template("DIT.html", users=users, TASKS=TASKS)
    
    else:
        flash("Log in to access Tasking functionality.")
        return redirect(url_for("login"))           

@app.route("/COAPP")
def COAPP():
    if check_user() == True: 
        OSINT = recent_OSINT()
        return render_template("CO.html", OSINT=OSINT)
    
    else:
        flash("Log in to access OSINT functionality.")
        return redirect(url_for("login"))           

@app.route("/CFAPP")
def CFAPP():
    if check_user() == True:    
        UPLOADS = get_uploads()
        return render_template("CF.html", UPLOADS=UPLOADS)
    
    else:
        flash("Log in to access collection functionality.")
        return redirect(url_for("login"))           

@app.route("/DETAPP")
def DETAPP():
    if check_user() == True:  
        TAruns = get_TAruns()
        return render_template("DET.html", TAruns=TAruns)
    
    else:
        flash("Log in to access TA functionality.")
        return redirect(url_for("login"))           

@app.route("/AEAPP")
def AEAPP():
    if check_user() == True:
        user = User(session["username"])
        user.menus()
        GUIDS = []
        Locations = user.Lcache
        Persons = user.Pcache
        Objects = user.Ocache
        Events = user.Ecache
        Rels = user.Rcache
        
        return render_template("AE.html", Locations=Locations, Persons=Persons, Objects=Objects, Events=Events, Rels=Rels)
    
    else:
        flash("Log in to access analysis functionality.")
        return redirect(url_for("login"))           

@app.route("/DRAPP")
def DRAPP():
    return render_template("DR.html")


    
