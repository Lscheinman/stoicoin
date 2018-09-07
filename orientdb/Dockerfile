############################################################
# Dockerfile to run an OrientDB (Graph) Container
############################################################

FROM openjdk:8-jdk-alpine

RUN wget -O orientdb-community-2.2.20.tar.gz http://orientdb.com/download.php?file=orientdb-community-2.2.20.tar.gz&os=linux

RUN tar -zxvf orientdb-community-2.2.20.tar.gz

WORKDIR /orientdb-community-2.2.20

RUN sudo bin/server.sh
