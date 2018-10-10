#-*- coding:utf-8 -*-
'''
    解决每台proxy的国家投递性能
    DA投递给proxy的性能分析
    通邮问题占比
'''
import pdb
from total2 import getStatusCode
from ipaddr import updateIPmapping
from dbmange import Mysql
from send2Es import Deliver2ESWithpath
import datetime
import time
g_SumofDAFailure=0
g_SumofDADeliver=0
g_proxydict={}
g_forCtyProxydict={}
g_Erroecodedict={}
mysqlInst=Mysql()
g_Deliverid=0

Error_CHNdescribe={250:"投递成功",400:"垃圾邮件",422:"用户状态错误",450:"连接断开",451:"重新投递",500:"服务器错误",550:"退信",551:"收信人不存在", 552:"发送方超过每日邮件限额",553:"需要认证，不能匿名发信",554:"拒收"}
def SetCHNdescribe(errorcode):
    if errorcode in Error_CHNdescribe.keys():
        return Error_CHNdescribe[errorcode]
    else:
        return "未遇见过的错误"
class ProformanceTable:
    def __init__(self,sourceRecord):
        global g_Deliverid
        self.ErrorCode = getStatusCode(sourceRecord.Originaldesc, sourceRecord.result)[0]
        self.detail = getStatusCode(sourceRecord.Originaldesc, sourceRecord.result)[1]
        self.time = sourceRecord.logCreatedate
        print self.time
        self.time = self.time[:self.time.find(".")]
        self.timestamp = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.timestamp1 = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.timestamp2 = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.NumofCode=1
        self.Proxyip=sourceRecord.bindip
        self.DAFailtoDeliver=0
        self.DAsucceedDelv=0
        self.Proxydelay = sourceRecord.optime #每台proxy的国家投递性能
        self.DAproxydelay=sourceRecord.optime
        if(sourceRecord.result==1):
            self.DAFailtoDeliver=1 #当前失败次数
            self.DAsucceedDelv=0
        elif (sourceRecord.result==0):
            self.DAsucceedDelv = 1
            self.DAFailtoDeliver =0
        self.SumofDASucceed=self.DAsucceedDelv
        self.SumofDAFailure=self.DAFailtoDeliver
        self.SumofDADeliver=1
        self.FailrateofDeliver=0.0
        self.Countrydict={}
        self.proxyCountry=updateIPmapping(sourceRecord.bindip,mysqlInst)
        self.ForCtySumofproxysucceedDeliver=self.DAsucceedDelv
        self.ForCtySumofproxyDelv=1
        self.ForCtyproxyDeliverRate=1-float(self.DAsucceedDelv)/float(self.ForCtySumofproxyDelv)
        self.did = g_Deliverid
        self.did1 = g_Deliverid
        self.did2 = g_Deliverid
        self.size=""
        forCtyProxydict={}
        g_Deliverid+=1
    def Data2Json(self):
        jsonData={};
        jsonData['ErrorCode'] = self.ErrorCode
        jsonData['detail'] = self.detail
        jsonData['time'] = self.time
        jsonData['NumofCode'] = self.NumofCode
        jsonData['Proxyip']=self.Proxyip
        jsonData['SumofDAFailure']=self.SumofDAFailure
        jsonData['SumofDADeliver'] = self.SumofDADeliver
        jsonData['FailrateofDeliver'] = self.FailrateofDeliver

        jsonData['ForCtySumofproxysucceedDeliver'] = self.ForCtySumofproxysucceedDeliver
        jsonData['ForCtySumofproxyDelv'] = self.ForCtySumofproxyDelv
        jsonData['ForCtyproxyDeliverRate'] = self.ForCtyproxyDeliverRate
        jsonData['proxyCountry'] = self.proxyCountry
        #jsonData['country']=self.country
        return jsonData
    def Data2FiveJson(self):
        jsonData = {};
        jsonData['ErrorCode'] = self.ErrorCode
        jsonData['detail'] = self.detail
        jsonData['CHNdescribe']=SetCHNdescribe(self.ErrorCode)
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp1))
        jsonData['NumofCode'] = self.NumofCode
        jsonData['@timestamp']=time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp1-28800))
        return jsonData
    def Data2SixJson(self):
        jsonData = {};
        jsonData['Proxyip'] = self.Proxyip
        jsonData['SumofDAFailure'] = self.SumofDAFailure
        jsonData['SumofDADeliver'] = self.SumofDADeliver
        jsonData['FailrateofDeliver'] = self.FailrateofDeliver
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp-28800))
        jsonData['DAproxydelay'] = self.DAproxydelay
        return jsonData
    def Data2SevenJson(self):
        jsonData = {};
        jsonData['ForCtySumofproxysucceedDeliver'] = self.ForCtySumofproxysucceedDeliver
        jsonData['ForCtySumofproxyDelv'] = self.ForCtySumofproxyDelv
        jsonData['ForCtyproxyDeliverRate'] = self.ForCtyproxyDeliverRate
        jsonData['proxyCountry'] = self.proxyCountry
        jsonData['Proxyip'] = self.Proxyip
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp2))
        jsonData['Proxydelay'] = self.Proxydelay
        jsonData['size'] = self.size
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp2-28800))
        # jsonData['country']=self.country
        return jsonData
def StatEveryLog(sourceRecord):
    global g_forCtyProxydict
    global g_Erroecodedict
    global g_proxydict

    t_table = ProformanceTable(sourceRecord)
    if g_proxydict.has_key(sourceRecord.bindip):
        temp = g_proxydict[sourceRecord.bindip]
        print "t_table.time=%s,temp.timestamp=%s"%(time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(t_table.timestamp)),time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(temp.timestamp)))
        if(t_table.timestamp-temp.timestamp<60):
            t_table.did=temp.did
            t_table.timestamp=temp.timestamp
            t_table.SumofDAFailure += temp.SumofDAFailure
            t_table.SumofDASucceed+= temp.SumofDASucceed
            t_table.SumofDADeliver+=temp.SumofDADeliver
            t_table.FailrateofDeliver = 1.0-float(t_table.SumofDASucceed)/float(t_table.SumofDADeliver)
            t_table.DAproxydelay =temp.DAproxydelay*temp.SumofDADeliver/t_table.SumofDADeliver
        else:
            pass
    g_proxydict[sourceRecord.bindip] = t_table
    Deliver2ESWithpath(t_table.Data2SixJson(), "newpreformance", "doc", t_table.did)
    if(g_Erroecodedict.has_key(t_table.ErrorCode)):
        temp = g_Erroecodedict[t_table.ErrorCode]
        if(t_table.timestamp1-temp.timestamp1<60):
            t_table.did1 = temp.did1
            t_table.timestamp1 = temp.timestamp1
            t_table.NumofCode+=temp.NumofCode
            if(t_table.ErrorCode!=250):
                t_table.detail+=temp.detail
        else:
            pass
    g_Erroecodedict[t_table.ErrorCode]=t_table
    Deliver2ESWithpath(t_table.Data2FiveJson(), "errorcode", "doc", t_table.did1)
    strsize=""
    if(sourceRecord.size<10*1024*1024):
        strsize="under10M"
        t_table.size = "under10M"
    else:
        strsize="up10M"
        t_table.size = "up10M"
    if(g_forCtyProxydict.has_key(t_table.proxyCountry+t_table.Proxyip+strsize)):
        temp = g_forCtyProxydict[t_table.proxyCountry+t_table.Proxyip+strsize]
        if (t_table.timestamp2 - temp.timestamp2 < 60):
            t_table.did2 = temp.did2
            t_table.timestamp2 = temp.timestamp2
            t_table.ForCtySumofproxysucceedDeliver+=temp.ForCtySumofproxysucceedDeliver
            t_table.ForCtySumofproxyDelv+=temp.ForCtySumofproxyDelv
            t_table.ForCtyproxyDeliverRate = 1.0-float(t_table.ForCtySumofproxysucceedDeliver)/float(t_table.ForCtySumofproxyDelv)
            t_table.Proxydelay = (temp.Proxydelay*temp.ForCtySumofproxyDelv+t_table.Proxydelay)/(t_table.ForCtySumofproxyDelv)
        else:
            pass
    g_forCtyProxydict[t_table.proxyCountry+t_table.Proxyip+strsize] = t_table
    Deliver2ESWithpath(t_table.Data2SevenJson(),"preformancepercountry","doc",t_table.did2)