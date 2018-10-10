#-*-coding:utf-8 -*-
'''
    python 编写的辅助服务
    功能：负责从Elasticsearch取数据，聚合，分析，将结果
          重新存入Elasticsearch.
'''
import pdb
import threading
from ParseLogNew import ParseLog2sql
from Clogdata import Clogdata
from ipaddr import updateIPmapping
from dbmange import Mysql
from statEveryLog import StatEveryLog
from statMailInfo import StatmailInfo
from send2Es import Deliver2ESWithpath
import config as cf
import time
from send2Es import getIndexDataFromEs
from ESconfig import Dictcache
from EShelplog import getlogInst
from EShelplog import getloggerPrint
from config import index_name
g_mysqlInst = Mysql()
g_searchSize=0
g_logdict ={}
g_dictMaxCapacitySize = 6000 #字典最大容量
g_logkeepTimeCountmax=1200  #字典里信息的存储阈值 ，时间就是120s
g_OntimeCount =0  # 记录定时器运行次数
g_dealOnelogTIme=0
g_logcount=0
g_logobj = getlogInst()# 日志对象
g_logobj.setlevel(0)
#缓存dict数据
g_Dictcache = Dictcache()
def DataAggregateWork(sourcedata):
    global g_dealOnelogTIme
    timed = int(round(time.time()*1000))-g_dealOnelogTIme
    g_dealOnelogTIme=int(round(time.time()*1000))
    global  g_logcount

    g_logobj.logger.info("deal  proxy log-------------time=%d-----------count=%d"%(timed,g_logcount))
    print "------log-------------time=%d-----------count=%d"%(timed,g_logcount)
    g_logcount+=1
    Onelog = Clogdata()
    Onelog.SetAttr(sourcedata)
    Onelog.country = updateIPmapping(Onelog.destip, g_mysqlInst)
    #//--在整合之前，添加业务接口，统计业务信息-//
    StatEveryLog(Onelog)
    Uniqueident=Onelog.tid+Onelog.CM_to
    if Uniqueident in g_logdict.keys():
        g_logdict[Uniqueident].reFleshAtrr(Onelog)
        return 0
    else:
        if len(g_logdict)<g_dictMaxCapacitySize:
            g_logdict[Uniqueident]=Onelog
            return 0
        else:
            return -1
mailCount=0
def refreshTimeCount():
    global g_logobj
    g_logobj.logger.info("the g_logdict size=%d"%(len(g_logdict)))
    for dkey,dvalue in g_logdict.items():
        dvalue.CM_TimeCount+=1
        if(dvalue.CM_status=="succeed" or dvalue.CM_status=="fail" or dvalue.CM_TimeCount>=g_logkeepTimeCountmax):
            #投递给ES
            #从g_logdict删除
            if(dvalue.CM_TimeCount>=g_logkeepTimeCountmax):
                dvalue.CM_status="overtime"
            dvalue.country = updateIPmapping(dvalue.destip,g_mysqlInst)
            Deliver2ESWithpath(dvalue.Data2Json(),"everymail","log",dvalue.CM_Index)
            #业务功能 邮件走势量
            StatmailInfo(dvalue)
            global mailCount
            g_logobj.logger.info("Delieve to everymail succeed------------------------------count=%d"%(mailCount))
            print "Delieve succeed------------------------------count=%d"%(mailCount)
            mailCount+=1
            g_logdict.pop(dkey)
#test time
text_time=0
def Main_Timer():
    global  text_time
    pass
    global g_OntimeCount
    global g_Dictcache
    global g_logdict
    if(g_Dictcache.dict!={}):
        g_logdict=g_Dictcache.Getdict()
    #调用接口获取message
    #调用解析模块和数据库入库模块
    #pdb.set_trace()
    print "g_Dictcache.dict"
    args = {"sort": {
			"logWritedate": {"order": "asc"}
		},"query": {"match_all": {}}}
    index=index_name
    datalist=getIndexDataFromEs(index,args,500)
    if type(datalist)!=bool:
        for each in datalist:
          #  pdb.set_trace()
            data = each["message"]
            filedate=each["timestamp"].encode('utf-8')
            #data="10:01:47 [tid:AQAAfwAXAl0KIpdbeSCaAA--.63368S2,from:easyflow2@gsd.net.cn,to:3086105150@qq.com,domain:icoremail.net,lid:OP_ICM_MX,size:2609,result:0,subject:[回函\]合同审定单(HTSD02)\:客户1040418-上鼎工程建设(上海)有限公司合同编号R0106190的合同审定单 (1809100024),Eval:,Score:1.00,ip:223.252.214.66,optime:1123,destip:183.57.48.35,bindip:106.2.96.53,desc:250 OK]"
            Datadict=ParseLog2sql(data,filedate)
            #对每条dict数据做聚合。这里只有等待聚合，不需要主动查询
            # 并对g_logdict里面的数据做更新处理，满足条件的数据直接放入
            global g_OntimeCount
            g_OntimeCount += 1
            DataAggregateWork(Datadict)
            refreshTimeCount()
    Onerow = int(round(time.time() * 1000)) - text_time
    g_Dictcache.Setdict(g_logdict)
    print "----after g_Dictcache.Setdict Onerow=%s"%(Onerow)
    text_time = int(round(time.time() * 1000))
    timer = threading.Timer(0.1, Main_Timer)
    timer.start()
if __name__ == '__main__':
    g_Dictcache.Createconf()
    timer = threading.Timer(0.1, Main_Timer)
    timer.start()
