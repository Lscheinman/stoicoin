# stoicoin
## Set up
### OrientDB
- sudo wget -O orientdb-community-2.2.20.tar.gz http://orientdb.com/download.php?file=orientdb-community-2.2.20.tar.gz&os=linux
- sudo tar -zxvf orientdb-community-2.2.20.tar.gz
- sudo mv ~/orientdb-community-2.2.20 /opt/orientdb
- sudo /opt/orientdb/bin/server.sh
- Set up the root password
- Go to the http://xxx.xxx.xxx.xxx:2480 to interact with the studio
- Press ctrl-c to stop the instance once verified
- Follow steps on https://orientdb.com/docs/2.2.x/Unix-Service.html to set up Database service
### Python
- sudo apt-get update
- sudo apt-get install build-essential libssl-dev libffi-dev python3-dev python3-pip nginx python-setuptools
- sudo pip3 install --upgrade pip
- sudo apt-get update
- sudo pip3 install virtualenv
- sudo pip3 install gunicorn
- sudo virtualenv stoicoinenv
- source stoicoinenv/bin/activate
### For SUSE
- no sudo for OrientDB
- set the proxy for git: git config --global http.proxy http://proxy:8080
- sudo zypper install python3-devel
- pip install --proxy=http://proxy:8080 -r requirements.txt
### Set up the Application service
- mv startup.service /etc/systemd/system/startup.service
- chmod +x startup.sh
- systemctl start startup.service
- systemctl status startup.service
## Operations
### OSINT
- Sign in with the SYSTEM user and load up necessary tokens for social media sites in the Administration Tile.
- In the same tile, set up the necessary SAP HANA details including host, ConDis credentials and port
- Once complete, Text Analytics and OSINT will be set


