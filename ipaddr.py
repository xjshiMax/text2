#!/usr/bin/python
#-*- coding:utf-8 -*-
import urllib2
import re
#import chardet
import codecs
from dbmange import Mysql
import pdb
from EShelplog import getlogInst
g_logisnt=getlogInst()

# CHN country mapping
CHN_CITY = {
    "河北":0,"山西":1,"辽宁":2,"吉林":3,"黑龙江":4,"江苏":5,"浙江":6,"安徽":7,"福建":8,"江西":9,"山东":10,"河南":11,
    "湖北":12,"湖南":13,"广东":14,"海南":15,"四川":16,"贵州":17,"云南":18,"陕西":19,"甘肃":20,"青海":21,"台湾":22,
    "北京":23,"天津":24,"上海":25,"重庆":26,
    "广西壮族":27,"内蒙古":28,"西藏":29,"宁夏回族":30,"新疆维吾尔":31,
    "香港":32,"澳门":33
}
def initIpMapping(mysqlinst):
    sqlstr="create table if not exists ipmappings (ipDomain varchar(255) not NULL primary key, ipcountry varchar(255) not NULL, des varchar(255) not NULL) DEFAULT CHARSET=utf8;"
    iret=1
    pdb.set_trace()
    try:
        iret = mysqlinst.insertOne(sqlstr)
        str1="insert into ipmappings (ipDomain,ipcountry,des) values ('8.8.8.8', '美国', '这是一个美国的免费的DNS服务器的地址')"
        str2="insert into ipmappings (ipDomain,ipcountry,des) values ('127.0.0.1', '本地地址', '这是本机器的回环网络地址，常做测试使用')"
        str3 = "insert into ipmappings (ipDomain,ipcountry,des) values ('0.0.0.0','Error','这是IP地址的中的保留地址，常作为源地址')"
        str4 = "insert into ipmappings (ipDomain,ipcountry,des) values ('255.255.255.255','广播地址','这是广播地址，不参与通信')"
        iret = mysqlinst.insertOne(str1)
        iret = mysqlinst.insertOne(str2)
        iret = mysqlinst.insertOne(str3)
        iret = mysqlinst.insertOne(str4)
    except:
        pass
    return iret
    pass
def subString(string,length):
    if length >= len(string):
        return string
    result = ''
    i = 0
    p = 0
    while True:
        ch = ord(string[i])
        #1111110x
        if ch >= 252:
            p = p + 6
        #111110xx
        elif ch >= 248:
            p = p + 5
        #11110xxx
        elif ch >= 240:
            p = p + 4
        #1110xxxx
        elif ch >= 224:
            p = p + 3
        #110xxxxx
        elif ch >= 192:
            p = p + 2
        else:
            p = p + 1
        if p >= length:
            break;
        else:
            i = p
    return string[i:] 

def getIPCountry(strip): 
    country = ''
    url = 'http://ip138.com/ips138.asp?ip=%s&action=2'%strip
    user_agent ='"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 Safari/537.36"' 
    headers = { 'User-Agent' : user_agent } 
    maxTryNum=10 
    for tries in range(maxTryNum): 
        try: 
            req = urllib2.Request(url, headers = headers)  
            html=urllib2.urlopen(req).read() 
            break 
        except: 
            if tries <(maxTryNum-1): 
                continue 
            else:
                g_logisnt.logger.error("Has tried %d times to access url %s, all failed!"%(maxTryNum,url))
                break
    html = unicode(html,"gbk").encode('utf-8')
    pattern = '<ul class="ul1">(.*?)</ul>'
    pattern1 = '<li>(.*?)</li>'
    regx = re.compile(pattern)
    regx1 = re.compile(pattern1)
    result = regx.findall(html)
    if len(result) == 1:
        parts = regx1.findall(result[0])
        li = parts[0].split(' ')[0]
    else:
        li="bad ip"
    ret = subString(li,16)
    return ret

def updateIPmapping(destip,mysql): # Interface
    global  g_logisnt
    querysql = 'select ipcountry from ipmapping where ipDomain=%s' 
    insertsql = 'insert into ipmapping(ipDomain,ipcountry,des) values(%s,%s,%s)'
    result =  mysql.getAll(querysql,param=[destip])
    if result == False: # get such ip info from ip138.cn
        ipcountry = getIPCountry(destip)
        country = ipcheck(ipcountry)
        result = mysql.insertOne(insertsql,(destip,country,''))
        mysql.end()
        if result == 0: #insert is ok
           return country	
    else:
         return result[0]['ipcountry']

def ipcheck(strIPCountry):
	strcountry = ""
	bInCHN = False	
	g_logisnt.logger.info("ipcheck:%s",strIPCountry)
	if strIPCountry.find(r"省") != -1:# 匹配到省字
		strcountry = strIPCountry[:strIPCountry.find(r"省")]
	elif strIPCountry.find(r"自治区") !=-1:# 匹配到自治区
		strcountry = strIPCountry[:strIPCountry.find(r"自治区")]
	elif strIPCountry.find(r"市") != -1:# 匹配到直辖市
		strcountry = strIPCountry[:strIPCountry.find(r"市")]
	elif strIPCountry.find(r"特别行政区") != - 1:#匹配到特别行政区
		strcountry = strIPCountry[:strIPCountry.find(r"特别行政区")]
	else:
		strcountry = strIPCountry
	if strcountry in CHN_CITY.keys(): # 在 中国列表中
		bInCHN = True
        if strcountry == "台湾":
            return "台湾"
        elif strcountry == "香港":
            return "香港"
        elif strcountry == "澳门":
            return "澳门"
        else:
            return "中国"
	return strIPCountry



if __name__ == '__main__':
    key = '37.13.12.12'
	#mysql = Mysql()
    mysql=Mysql()
    ret = initIpMapping(mysql)
    print ret
	#print updateIPmapping(key,mysql)
	#print len(CHN_CITY)
