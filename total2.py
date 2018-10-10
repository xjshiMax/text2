# !/usr/bin/python
# -*- coding: utf-8 -*-
'''
	100 个数据 帧 格式
	
	CM_Index 'tid:','from:','to:','domain:','lid:','size:','result:','subject:','Eval:','Score:','ip:','optime:','destip:','bindip:','desc'

	功能： 统计海外接收延时平均值最长的域名TOP10

'''
'''
	传入值：LogInfo(100条数据，日志信息) 
'''
import pdb
import random
import codecs
from file2json import str2Json,str2Full
from send2Es import Deliver2ESWithpath
from EShelplog import getlogInst
g_loginst=getlogInst()
import json
class LogInfo(object):
	"""docstring for LogInfo"""
	def __init__(self):
		super(LogInfo, self).__init__()
		self.CM_Index = 0
		self.tid = ''
		self._from = ''
		self.to = ''
		self.domain = ''
		self.lid = ''
		self.size = 0
		self.result = 0
		self.subject = ''
		self.Eval = ''
		self.Score = 0.0
		self.ip = ''
		self.destip = ''
		self.optime = 0
		self.bindip = ''
		self.desc = ''

	def setAttr(self,strDict):
#		self.CM_Index = strDict['CM_Index']
		self.tid = strDict['tid']
		self._from = strDict['CM_from']
#		self._from = strDict['from']
		self.to = strDict['CM_to']
#		self.to = strDict['to']
		self.domain = strDict['domain']
		self.lid = strDict['lid']
		self.size = strDict['size']
		self.result = strDict['result']
		self.subject = strDict['subject']
		self.Eval = strDict['Eval']
		self.Score = strDict['Score']
		self.ip = strDict['ip']
		self.destip = strDict['destip']
		self.optime = strDict['optime']
		self.bindip = strDict['bindip']
		self.desc = strDict['CM_desc']
#		self.desc = strDict['desc']
	
'''
	
'''
g_index_ID = 0
def genInfo(strTuple,oldFromData,oldToData):#传入元祖数据列表并转成对象列表 并对之前的数据进行融合
	if type(strTuple)==bool:
		return oldToData,oldFromData
	global g_index_ID
	g_index_ID+=1
	logInfos = []
	#print "strTuple:",strTuple
	for dict in strTuple:
		info = LogInfo()
		info.setAttr(dict)
		logInfos.append(info)
	newToData,newFromData = handle(logInfos)
	if len(oldFromData)!=0:
		for new in newFromData:
			bfind = False
			for old in oldFromData:#新老数据的叠加
				# pdb.set_trace()
				if old["from"] == new["from"]:#当前的from相同 数据融合
					bfind = True
					old["cost_time"] += new["cost_time"]#所有时间叠加
					for tid in new["tid"]:#tid 的累加
						old["tid"].append(tid)
					for proxy in new["proxy_chain"]:#路径的累加
						old["proxy_chain"].append(proxy)
			if bfind == False:#在旧数据中未找到与之匹配的From 数据
				oldFromData.append(new)
	else:
		oldFromData = newFromData      
	if len(oldToData) !=0:
		for new in newToData:
			bfind = False
			for old in oldToData:#新老数据的叠加
				if old["to"] == new["to"]:#当前的from相同 数据融合
					bfind = True
					old["cost_time"] += new["cost_time"]#所有时间叠加
					for tid in new["tid"]:#tid 的累加
						old["tid"].append(tid)
					for proxy in new["proxy_chain"]:#路径的累加
						old["proxy_chain"].append(proxy)
			if bfind == False:#在旧数据中未找到与之匹配的From 数据
				oldToData.append(new)
	else:
		oldToData = newToData

	return oldToData,oldFromData

def handle(logInfos):
	retAll = []
	ret = {}#存储一个收件人所有信息量 以收件域名为基础
	retFromAll = []
	retFrom = {} #存储一个发信人所有信息量 以发件域名为基础
	for info in logInfos:#循环 
		flag = False
		flag1 = False
		info.desc = info.desc.replace("\\r\\n","")
		info.desc = info.desc.replace("\\:",":")
		info.desc = info.desc.replace("\"","'")
		retcode,desc = getStatusCode(info.desc,info.result)
		for ret in retAll:# 按照收件人域名统计
			if ret["to"] == info.to.split('@',1)[1]:#找到tid
				ret["tid"].append(info.tid)
				ret["cost_time"] += info.optime
				ret["proxy_chain"].append({"proxyIP":info.bindip,"optime":info.optime,"retcode":retcode,"desc":desc})
				flag = True
		if flag == False:#未找到一个匹配的数据
			temp = {}
			temp["to"] = info.to.split('@',1)[1]
			temp["tid"] = [info.tid]
			temp["cost_time"] = info.optime
			temp["proxy_chain"] = [{"proxyIP":info.bindip,"optime":info.optime,"retcode":retcode,"desc":desc}]
			retAll.append(temp)
		for ret in retFromAll:
			if info._from == "":
				continue
			elif ret["from"] == info._from.split('@',1)[1]:#找到 发信人域名
				ret["tid"].append(info.tid)
				ret["cost_time"] += info.optime
				ret["proxy_chain"].append({"proxyIP":info.bindip,"optime":info.optime,"retcode":retcode,"desc":desc})
				flag1 = True
		if flag1 == False:#未找到一个匹配的数据

			temp = {}
			if info._from != "":
				temp["from"] = info._from.split('@',1)[1]
				temp["tid"] = [info.tid]
				temp["cost_time"] = info.optime
				temp["proxy_chain"] = [{"proxyIP":info.bindip,"optime":info.optime,"retcode":retcode,"desc":desc}]
				retFromAll.append(temp)
	
	return retAll,retFromAll # to from

def DataAnalysis(retAll,retFromAll):
	#统计收件人域名的情况
	OrderByToDomain = []

	for part in retAll:
		temp = {}
		maxtime = 0# 投递最长时间
		mintime = 100000000# 投递最短时间
		averagetime = 0# 投递的平均时长
		temp["toDomain"] = part["to"] #收件人域名
		for proxyInfo in part["proxy_chain"]:
			averagetime += proxyInfo["optime"]
			if maxtime  <= proxyInfo["optime"]:
				maxtime = proxyInfo["optime"]
			if mintime >= proxyInfo["optime"]:
				mintime = proxyInfo["optime"]
		averagetime /= len(part["proxy_chain"])
		temp["averagetime"] = averagetime
		temp["maxtime"] = maxtime
		temp["mintime"] = mintime
		temp["proxycnt"] = len(part["proxy_chain"])
		OrderByToDomain.append(temp)
	#print json.dumps(OrderByToDomain,indent=4)

	#统计发信人域名的情况
	OrderByFromDomain = []

	for part in retFromAll:
		temp = {}
		maxtime = 0# 投递最长时间
		mintime = 100000000# 投递最短时间
		averagetime = 0# 投递的平均时长
		temp["fromDomain"] = part["from"] #收件人域名
		for proxyInfo in part["proxy_chain"]:
			averagetime += proxyInfo["optime"]
			if maxtime  <= proxyInfo["optime"]:
				maxtime = proxyInfo["optime"]
			if mintime >= proxyInfo["optime"]:
				mintime = proxyInfo["optime"]
		averagetime /= len(part["proxy_chain"])
		temp["averagetime"] = averagetime
		temp["maxtime"] = maxtime
		temp["mintime"] = mintime
		temp["proxycnt"] = len(part["proxy_chain"])
		OrderByFromDomain.append(temp)
	# json.dumps(OrderByFromDomain,indent=4)
	i = 0
	for eachitem in OrderByToDomain:
		Deliver2ESWithpath(eachitem,"todomain","test_todmain",i)
		i+=1
	i = 0
	for eachitem in OrderByFromDomain:
		Deliver2ESWithpath(eachitem, "fromdomain","test_domain",i)
		i+=1
	i = 0


	return OrderByToDomain,OrderByFromDomain


def getStatusCode(strstatus,result):
	#pdb.set_trace()
	if strstatus == "":#为空
		if result == 0:
			return 250,"send mail ok"#发送成功
		elif result == 1:
			return 400,"send mail failed"#发送失败
		elif result == 2:
			return 500,"send mail defer"#发送失败
	else:
		bInt = True
		strcode = strstatus[:3]
		for i in range(0,3):
			if strcode[i] >= '0' and strcode[i] <= '9':#判断为数字
				continue
			else:
				bInt =False
				break
		if bInt == False:#字符串
			if result == 0:
				return 250,strstatus #发送成功
			elif result == 1:
				return 400,strstatus#发送失败
			elif result == 2:
				return 500,strstatus#发送失败
		else:
			return int(strstatus[:3]),strstatus[3:]

def Dict2Strjson(strDict):
	sstr = '{'
	i=0
	for key,value in strDict.items():
		if type(value) == int:
			sstr+="\"" + key + "\":" + str(value) + ""
		else:
			sstr+="\"" + key + "\":\"" + value + "\""
		if i != len(strDict.keys())-1:
			sstr += ","
		i+=1
	sstr+='}'
	return str(sstr)

def readDataFromFile(logfilename):
	logInfos = []
	i = 0
	try:
		f = codecs.open(logfilename,'r',encoding='gbk')
		for line in f.readlines():
			tj = str2Json(line[line.find('[')+1:len(line)-2])
			i+=1
			logInfos.append(tj)
			if i == 2000:
				break	
			#info = LogInfo()
			#info.setAttr(tj)
			
		f.close()
		
		ret,ret1 = genInfo(tuple(logInfos),[],[])# to from
		ret,ret1 = DataAnalysis(ret,ret1)
		for i in range(0,len(ret)):
			Deliver2ESWithpath(Dict2Strjson(ret[i]),"to1domain","table1",i)
		for i in range(0,len(ret)):
			Deliver2ESWithpath(Dict2Strjson(ret[i]),"from1domain","table2",i)

		#handle(logInfos)

	except IOError:
		g_loginst.logger.error("can't open such %s file"%logfilename)

#handle(logInfos)
#readDataFromFile('/mnt/file/proxy2018_09_11/proxy_00_00_00.log')



# a = [1,2]

# print (a)

# print "123@abc.com".split("@",1)[1]
