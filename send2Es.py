# !/usr/bin/python
# -*- coding: utf-8 -*-
'''
	该模块封装了elasticsearch库在 python客户端的API
	提供与elasticsearch交互的接口。
'''
from EShelplog import getlogInst
import json
from elasticsearch5 import Elasticsearch as els
from elasticsearch5 import ElasticsearchException as elsexception
from ESconfig import ESconfig
from config import ES_HOST
import pdb
#ES_HOST ={"host":"192.168.202.44",
#	 "port" 	:"9200"
#	 }
es = els([ES_HOST])
TestJson = {"tid":"IwENCgA36y3NlJZbg7RUAA--.21062S2","from":"minipay@znu.com","to":"86975622@qq.com","domain":"corpease.net","lid":"icm-hosting","size":40077,"result":0,"subject":"Heloworld","Eval":"BAYES_99;BM_PASS;CMD_CNT_00_10;CUR_CONN_00_01;DKIM_SUCCESS;DMARC_NON_ALIGNED;DOMAIN_QUARTER_CNT_20_40;DOMAIN_QUARTER_RCPT_CNT_10_50;DOMAIN_TODAY_CNT_1K_XX;DOMAIN_TODAY_RCPT_CNT_1K_XX;FP___MIME_BASE64_MIME_BASE64_NO_NAME_PTR_YES;GET_ERROR_HEADER_FIELD;HTML_BADTAG_00_10;HTML_HAS_COMMENT;HTML_MAX_CONJOINT_IMG_TAG_CNT_08;HTML_NONELEMENT_00_10;HTML_SET_STYLE;HTML_TAG_ATTRIBUTE_COLOR_BAD;HTML_TAG_EXIST_TBODY;HTML_TEXT_DISPLAY_NONE;HTML_TOTAL_IMG_TAG_CNT_32;IP_QUARTER_CNT_04_08;IP_TODAY_CNT_1K_2K;JPG_SVM_PROB_00_10;MIME_BASE64_NO_NAME;MIME_BASE64_TEXT;MIME_HTML_ONLY;NO_PLAIN_CONTENT_TYPE;PTR_YES;RCVD_IN_SORBS_SPAM;REGION_US_23;REPUTATION_NULL;RUSER_QUARTER_CNT_20_40;RUSER_QUARTER_RCPT_CNT_10_50;RUSER_TODAY_CNT_1K_XX;RUSER_TODAY_RCPT_CNT_1K_XX;SENDERREP_NULL;SPF_PASS;STEXT_SVM_PROB_00_10;SUBJECT_CNT_3000_XXXX;TEXT_HTML_CNT_01_03;TEXT_PLAIN_CNT_00_01;TOTAL_DISPLAY_NONE_TAG_CNT_01_03;TO_CC_BCC_CNT_00_02;URLREP_NULL;USER_SEND_INTERVAL_10_60;__MIME_BASE64","Score":12.59,"ip":"223.252.214.175","optime":1344,"destip":"183.57.48.35","bindip":"106.2.96.53","desc":"250 OK"}
g_Scalesize=0
g_GetInESTimestamp=""
g_esconfig = ESconfig()
g_logobj = getlogInst()# 日志对象
#g_logobj.setlevel(0)
def SetScaleSize(size):
	global g_esconfig
	#esconfig = ESconfig()
	g_esconfig.setsize(size)
def CreateIndex(index):
	try:
		iret = es.indices.create(index=index, ignore=400)
	except:
		pass
def Deliver2ESWithpath(strJson,t_index,t_type,t_id):
	tjson = ""
	if type(strJson) == dict:
		tjson = json.dumps(strJson)
	elif type(strJson) == str:
		tjson = strJson
	else:
		g_logobj.getLogger(Loglevel[2]).log( "unhandler error")
	try:
		es.index(index=t_index, doc_type=t_type, id=t_id, body=tjson)
		pass
	except Exception as e:
		g_logobj.logger.log("index failed%s"%(e) )
		raise
def Deliver2ES(strJson):
	if type(strJson) == str or 1==1:
		tjson = strJson
		tjson = json.dumps(strJson)
		try:
			es.index(index='plog',doc_type='data2ES',id=strJson["CM_Index"],body=tjson)
			pass
		except Exception as e:
			#g_logobj.getLogger(Loglevel[2]).log( "index failed%s"%(e))
			raise
			#g_logobj.getLogger(Loglevel[4]).log("ok")
	else:
		g_logobj.logger.log("unhandler error")

#查询Elasticsearch 中 index为index的数据，使用过滤器query_args
#index:索引名称   str
#query_args:过滤器参数 json
#bFirstQuery  bool  True:第一次获取 使用search  False 非第一次获取 使用 scroll 滚动
#size 指定每一次查询的大小 int

sid = ""
scroll_size = 0
bFirstQuery = True
Justforsearch=False
def GetScaleSize():
	global Justforsearch
	global g_esconfig
	#esconfig=ESconfig()
	tempsize=g_esconfig.getsize()
	return tempsize

#Getlastscrollid 通过当前已经取到lastsize,寻准找scroll_id，以便调用scroll函数，size为分片大小
#返回 scrollid,以及还有作用的数据
def Getlastscrollidanddata(index,lastsize,size):
	g_logobj.logger.info("断线重连以后获取ES开始扫描点...lastsize=%d,size=%d"%(lastsize,size))
	query_args = {
		"query": {
			"match_all": {}
		}
	}
	numofdata=0
	finalresult={}
	#pdb.set_trace()
	if(lastsize<=size):
		result = es.search(index=index,body=query_args,scroll='2m',size=size)
		scroll_id=result["_scroll_id"]
		data = result["hits"]["hits"]
		scroll_size = result["hits"]["total"]
		finalresult[scroll_id] =data[lastsize:]
		finalresult["scroll_size"]=scroll_size
		return finalresult
	else:
		result = es.search(index=index, body=query_args, scroll='2m', size=size)
		sid = result["_scroll_id"]
		data = result["hits"]["hits"]
		scroll_size = result["hits"]["total"]  # 获得总数
		numofdata+=len(data)
		while numofdata<lastsize:
			scrolldata=es.scroll(scroll_id=sid,scroll='2m')
			sid=scrolldata["_scroll_id"]
			data = scrolldata["hits"]["hits"]
			scroll_size = scrolldata["hits"]["total"]
			if lastsize-numofdata<len(data):
				newdata=data[lastsize-number:]
				finalresult[sid]=newdata
				finalresult["scroll_size"]=scroll_size
				return finalresult
			if lastsize-numofdata==len(data):
				finalresult[sid]=[]
				finalresult["scroll_size"] = scroll_size
				return finalresult
			else:
				numofdata+=len(data)

def GetlastscrollfromTimestamp(index,timestamp,size):
	g_logobj.logger.info("断线重连以后获取ES开始扫描点...timestamp=%s,size=%d" % (timestamp, size))
	bodystru = {
		"sort": {
			"logWritedate": {"order": "asc"}
		},
		"query": {
			"bool": {
				"must": [
					{
						"range": {"logWritedate": {"gt": u'2018-09-30T08:27:52.108Z'}}
					}
				],
				"must_not": []

			}  # bool
		}
	}
	bodystru['query']['bool']['must'][0]['range']['logWritedate']['gt']=timestamp
	finalresult = {}
	result = es.search(index=index, body=bodystru, scroll='2m', size=size)
	scroll_id = result["_scroll_id"]
	data = result["hits"]["hits"]
	scroll_size = result["hits"]["total"]
	finalresult[scroll_id] = data
	finalresult["scroll_size"] = scroll_size
	return finalresult
#返回值：
# False index 为空 或者index 的type类型错误
# False 查询的结果为空
# [list] 返回一个结果list [{},{}] 其中结构 {"id":"","timestamp":"","message":""}


def getIndexDataFromEs(index,query_args,size):
	global scroll_size,sid,bFirstQuery
	#global g_Scalesize
	global g_logobj
	global g_GetInESTimestamp
	ret = []	
	if type(index) !=	str or index == "":# index 表示為所選索引的值(str) query_args 为所要查询的参数(json 格式)
		return False
	else:
		try:
			#pdb.set_trace()
			if bFirstQuery == True:#第一次查询数据
				#print "first query"
				#查询数据
				lastsize = GetScaleSize()
				g_logobj.logger.info("第一次查询 index=%s,分片size=%d,配置里lastsize=%s"%(index,size,lastsize))
				if lastsize=="": #表示确实时第一次启动，没出现过服务突然死亡
					data = es.search(index=index,body=query_args,scroll='2m',size=size)#一个列表
					sid = data["_scroll_id"]
					scroll_size = data["hits"]["total"]# 获得总数
					data = data["hits"]["hits"]# 获得关键数据
					if len(data) == 0:# 为空
						return False
					#g_Scalesize += len(data)
				else: #标识曾经重启过服务，配置里有上次已经获取过的数据size.
					lastdata=GetlastscrollfromTimestamp(index,lastsize,size)
					#pdb.set_trace()
					scroll_size=lastdata["scroll_size"]
					lastdata.pop("scroll_size")
					sid=lastdata.keys()[0]
					data=lastdata.values()[0]
					#g_Scalesize+=lastsize+len(lastdata.values()[0])
				for li in data:  # 列表读取
					eachItem = {}
					_source = li["_source"]  # 具体信息
					eachItem["id"] = li["_id"]  # 得到_id
					eachItem["timestamp"] = _source["logWritedate"]  # 时间戳
					eachItem["message"] = _source["message"]
					#eachItem["filename"] = _source["filename"]
					g_GetInESTimestamp = _source["logWritedate"]
					print "g_GetInESTimestamp=%s"%(g_GetInESTimestamp)
					ret.append(eachItem)
				bFirstQuery = False
			else:#非第一次查取数据 采用 scroll
				g_logobj.logger.info("scroll... sid=%s"%(sid))
				if scroll_size > 0:# 当前查询条数大于0
					#print "scorlling..."
					data = es.scroll(scroll_id= sid,scroll='2m')
					sid = data['_scroll_id']
					scroll_size = len(data['hits']['hits'])
					
					data = data['hits']['hits']
					if len(data) == 0:# 为空
						return False
					for li in data:#列表读取
						eachItem = {}
						_source = li["_source"]#具体信息
						eachItem["id"] = li["_id"]#得到_id
						eachItem["timestamp"] = _source["logWritedate"]#时间戳
						eachItem["message"] = _source["message"]
						#eachItem["filename"] = _source["filename"]
						g_GetInESTimestamp = _source["logWritedate"]
						print " else g_GetInESTimestamp=%s" % (g_GetInESTimestamp)
						ret.append(eachItem)
					#g_Scalesize += len(data)
		except Exception as e:
			g_logobj.logger.error( "search failed%s"%e)
			raise
			#g_logobj.logger.error("search finished")
		SetScaleSize(g_GetInESTimestamp)
		return ret

#查询Elasticsearch 中 index为index的数据，使用过滤器query_args
def getIndexData(index,query_args):# index 表示為所選索引的值(str) query_args 为所要查询的参数(json 格式)
	ret = []
	sid = ""
	scroll_size = 0
	if type(index) !=str or index == "":#判断index为空 或者类型不匹配 则返回失败
		return False
	else:
		try:
			#查询数据
			data = es.search(index=index,body=query_args,scroll='2m',size=1000)#一个列表
			sid = data["_scroll_id"]
			scroll_size =  data['hits']['total']
			data = data["hits"]["hits"]
			eachtime = {}
			#获取时间戳
			for li in data:#循环读取
				_source = li["_source"] # 得到数据
				_id = li["_id"]#得到 _id
				_timestamp = _source["@timestamp"]#时间戳
				_message = _source["message"]

				eachtime["id"] = _id
				eachtime["timestamp"] = _timestamp
				eachtime["message"] = _message
				ret.append(eachtime)
			
			i = 0
			while (scroll_size > 0):
				data = es.scroll(scroll_id= sid,scroll='2m')
				sid = data['_scroll_id']
				scroll_size = len(data['hits']['hits'])
				i+=scroll_size
		except Exception as e:
			g_logobj.logger.error("search failed%s" % e)
			raise
			g_logobj.logger.error("search finished")
