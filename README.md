# stoicoin
## Set up OrientDB
- sudo wget -O orientdb-community-2.2.20.tar.gz http://orientdb.com/download.php?file=orientdb-community-2.2.20.tar.gz&os=linux
- sudo tar -zxvf orientdb-community-2.2.20.tar.gz
- sudo mv ~/orientdb-community-2.2.20 /opt/orientdb
- sudo /opt/orientdb/bin/server.sh
- Set up the root password
- Go to the http://xxx.xxx.xxx.xxx:2480 to interact with the studio

## Set up Python
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
## Set up the Application service
- mv startup.service /etc/systemd/system/startup.service
- systemctl start startup.service
- systemctl status startup.service


