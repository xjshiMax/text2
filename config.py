#!/usr/bin/python
# -*- coding: UTF-8 -*-
#Config 数据库配置文件


DBHOST = "localhost"
DBPORT = 3306
DBUSER = "root"
DBPWD = "123456"
DBNAME = "GElog"
DBCHAR = "utf8"

#设置Elasticsearch 的服务ip
ES_HOST = {  "host": "192.168.202.44",  "port": "9200" }

#logstash数据发送给es的index源名称
index_name="my_test"