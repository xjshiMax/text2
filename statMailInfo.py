# -*- coding:utf-8 -*-
'''
    解决根据收件人域名统计退信率的问题
    根据收件人，发件人 统计延时
    统计发信量
    根据国家统计发信量
'''

import datetime
import time
from Clogdata import Clogdata
from send2Es import Deliver2ESWithpath
from ipaddr import updateIPmapping
from dbmange import Mysql
rejectdict={}
g_rejectrateid=0
mysqlInst=Mysql()
g_recipdomaindict={}
g_SenderDomaindict={}
g_Countrydict={}
class structreject:
    ZeroTime = time.time() - time.time() % 86400
    def __init__(self,sourceRecord):
        global g_rejectrateid
        self.recipDomain=sourceRecord.CM_to[sourceRecord.CM_to.find('@')+1:]
        self.Forrecipreturn = 0
        self.ForCtyreturn = 0
        self.Sumofreturn = 0
        self.SumofSucceed = 0
        self.SumofThridError = 0
        self.ForCtyNumofsucceed = 0
        if(sourceRecord.result==1):
            self.Forrecipreturn=1
            self.ForCtyreturn = 1
            self.Sumofreturn = 1
            self.SumofSucceed = 0
            self.SumofThridError = 0
            self.ForCtyNumofsucceed = 0
        elif(sourceRecord.result==0):
            self.Forrecipreturn = 0
            self.ForCtyreturn = 0
            self.Sumofreturn = 0
            self.SumofSucceed = 1
            self.SumofThridError = 0
            self.ForCtyNumofsucceed = 1
        else:
            self.Forrecipreturn = 0
            self.ForCtyreturn = 0
            self.Sumofreturn = 0
            self.SumofSucceed = 0
            self.SumofThridError = 1
            self.ForCtyNumofsucceed = 0
        self.ForrecipSendmail=1
        self.ForrecipRejectrate=float(self.Forrecipreturn)/float(self.ForrecipSendmail)

        self.time =sourceRecord.logCreatedate #datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.time = self.time[:self.time.find(".")]
        self.timestamp = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.timestamp1 = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.timestamp2 = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))
        self.timestamp3 = int(time.mktime(time.strptime(self.time, "%Y-%m-%dT%H:%M:%S")))

        self.sendid=g_rejectrateid
        self.sendid1 = g_rejectrateid
        self.sendid2 = g_rejectrateid
        self.sendid3 = g_rejectrateid
        g_rejectrateid+=1
        self.SumofDeliver=1


        self.SumofFailrate=1.0-float(self.SumofSucceed)/float(self.SumofDeliver)

        self.ToCountry=updateIPmapping(sourceRecord.bindip,mysqlInst)
        self.ForCtyNumofdlv=1
        self.ForCtyFailrate=1.0-float(self.ForCtyNumofsucceed)/float(self.ForCtyNumofdlv)

        self.ForrecipaveDelay=sourceRecord.CM_AllTime
        self.Forrecipmaxdelay=sourceRecord.CM_AllTime
        self.ForrecipThisdelay=sourceRecord.CM_AllTime

        self.SenderDomain=sourceRecord.CM_from[sourceRecord.CM_from.find('@')+1:]
        self.ForSenderaveDelay = sourceRecord.CM_AllTime
        self.ForSendermaxdelay = sourceRecord.CM_AllTime
        self.ForSenderThisdelay = sourceRecord.CM_AllTime
        self.ForSenderDelvier=1

    def Data2Json(self):
        jsonData = {};
        jsonData['recipDomain'] = self.recipDomain
        jsonData['Forrecipreturn'] = self.Forrecipreturn
        jsonData['ForrecipSendmail'] = self.ForrecipSendmail
        jsonData['ForrecipRejectrate'] = self.ForrecipRejectrate
        jsonData['date'] = self.date
        jsonData['SumofDeliver'] = self.SumofDeliver
        jsonData['Sumofreturn'] = self.Sumofreturn
        jsonData['SumofSucceed'] = self.SumofSucceed
        jsonData['SumofFailrate'] = self.SumofFailrate
        jsonData['ToCountry'] = self.ToCountry
        jsonData['ForCtyNumofdlv'] = self.ForCtyNumofdlv
        jsonData['ForCtyreturn'] = self.ForCtyreturn
        jsonData['ForCtyFailrate'] = self.ForCtyFailrate
        jsonData['ForrecipaveDelay'] = self.ForrecipaveDelay
        jsonData['Forrecipmaxdelay'] = self.Forrecipmaxdelay
        jsonData['ForrecipThisdelay'] = self.ForrecipThisdelay
        jsonData['SenderDomain'] = self.SenderDomain
        jsonData['ForSenderaveDelay'] = self.ForSenderaveDelay
        jsonData['ForSendermaxdelay'] = self.ForSendermaxdelay
        jsonData['ForSenderThisdelay'] = self.ForSenderThisdelay
        return jsonData
    def Data2Onejson(self):
        jsonData = {};
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp-28800))
        jsonData['recipDomain'] = self.recipDomain
        jsonData['ForrecipaveDelay'] = self.ForrecipaveDelay
        jsonData['Forrecipmaxdelay'] = self.Forrecipmaxdelay
        jsonData['ForrecipThisdelay'] = self.ForrecipThisdelay
        jsonData['Forrecipreturn'] = self.Forrecipreturn
        jsonData['ForrecipSendmail'] = self.ForrecipSendmail
        jsonData['ForrecipRejectrate'] = self.ForrecipRejectrate
        #jsonData['SumofDeliver'] = self.SumofDeliver
        #jsonData['Sumofreturn'] = self.Sumofreturn
        #jsonData['SumofSucceed'] = self.SumofSucceed
        #jsonData['SumofFailrate'] = self.SumofFailrate
        return jsonData
    def Data2Twojson(self):
        jsonData = {};
        jsonData['time'] =time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp1))
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp1-28800))
        jsonData['SenderDomain'] = self.SenderDomain
        jsonData['ForSenderaveDelay'] = self.ForSenderaveDelay
        jsonData['ForSendermaxdelay'] = self.ForSendermaxdelay
        jsonData['ForSenderThisdelay'] = self.ForSenderThisdelay
        return jsonData
    def Data2Fourjson(self):
        jsonData = {};
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp3))
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp3-28800))
        jsonData['SumofDeliver'] = self.SumofDeliver
        jsonData['Sumofreturn'] = self.Sumofreturn
        jsonData['SumofSucceed'] = self.SumofSucceed
        jsonData['SumofFailrate'] = self.SumofFailrate
        return jsonData
    def Data2Eightjson(self):
        jsonData = {};
        jsonData['time'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp2))
        jsonData['@timestamp'] = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.localtime(self.timestamp2-28800))
        print "sdn jsonData['@timestamp']=%s"%(jsonData['@timestamp'])
        jsonData['ToCountry'] = self.ToCountry
        jsonData['ForCtyNumofdlv'] = self.ForCtyNumofdlv
        jsonData['ForCtyreturn'] = self.ForCtyreturn
        jsonData['ForCtyFailrate'] = self.ForCtyFailrate
        return jsonData

    def UpdatePa(self):
        global g_rejectrateid
        #if(self.timestamp - self.ZeroTime>86400):
            #self.ZeroTime = time.time() - time.time() % 86400
        #g_rejectrateid += 1
        #self.sendid = g_rejectrateid
def StatmailInfo(sourceRecord):
    global  g_rejectrateid
    structdata=structreject(sourceRecord)
    if g_recipdomaindict.has_key(structdata.recipDomain):
        #if structdata.timestamp-rejectdict[structdata.SenderDomain].ZeroTime>86400:# 如果过了12点重置数据，重新计算该域名对应的收件量，发件量
        temp = g_recipdomaindict[structdata.recipDomain]
        if(structdata.timestamp-temp.timestamp<60):
            structdata.sendid = temp.sendid
            structdata.timestamp=temp.timestamp
            #temp=g_recipdomaindict[structdata.recipDomain]
            structdata.ForrecipSendmail+=temp.ForrecipSendmail
            structdata.ForrecipaveDelay=(temp.ForrecipSendmail*temp.ForrecipaveDelay+structdata.ForrecipThisdelay)/structdata.ForrecipSendmail
            if(structdata.Forrecipmaxdelay<temp.Forrecipmaxdelay):
                structdata.Forrecipmaxdelay=temp.Forrecipmaxdelay
            structdata.Forrecipreturn+=temp.Forrecipreturn
            structdata.ForrecipRejectrate = float(structdata.Forrecipreturn)/float(structdata.ForrecipSendmail)
        else:
            pass
    else:
        pass
    g_recipdomaindict[structdata.recipDomain] = structdata
    Deliver2ESWithpath(structdata.Data2Onejson(), "recipdomainstat", "doc", structdata.sendid)
    if(g_recipdomaindict.has_key("GetTrendobj")):
        temp=g_recipdomaindict["GetTrendobj"]
        if(structdata.timestamp3-temp.timestamp3<60):
            structdata.timestamp3=temp.timestamp3
            structdata.sendid3=temp.sendid3
            structdata.Sumofreturn+=temp.Sumofreturn
            structdata.SumofDeliver += temp.SumofDeliver
            structdata.SumofSucceed += temp.SumofSucceed
            structdata.SumofFailrate = 1.0 - float(temp.SumofSucceed) / float(temp.SumofDeliver)
    else:
        pass
    g_recipdomaindict["GetTrendobj"]=structdata
    Deliver2ESWithpath(structdata.Data2Fourjson(), "mailtrendstat", "doc", structdata.sendid3)
    if g_SenderDomaindict.has_key(structdata.SenderDomain):
        temp = g_SenderDomaindict[structdata.SenderDomain]
        if (structdata.timestamp1 - temp.timestamp1 < 60):
            structdata.sendid1 = temp.sendid1
            structdata.timestamp1 =temp.timestamp1
        #temp=g_SenderDomaindict[structdata.SenderDomain]
            structdata.ForSenderDelvier+=temp.ForSenderDelvier
            if(structdata.ForSendermaxdelay<temp.ForSendermaxdelay):
                structdata.ForSendermaxdelay=temp.ForSendermaxdelay
    else:
        pass
    g_SenderDomaindict[structdata.SenderDomain] = structdata
    Deliver2ESWithpath(structdata.Data2Twojson(), "senderdomainstat", "doc", structdata.sendid1)
    if(g_Countrydict.has_key(structdata.ToCountry)):
        temp=g_Countrydict[structdata.ToCountry]
        if (structdata.timestamp2 - temp.timestamp2 < 60):
            structdata.sendid2 = temp.sendid2
            structdata.timestamp2=temp.timestamp2
            structdata.ForCtyNumofdlv+=temp.ForCtyNumofdlv
            structdata.ForCtyreturn+=temp.ForCtyreturn
            structdata.ForCtyNumofsucceed+=temp.ForCtyNumofsucceed
            structdata.ForCtyFailrate=1.0-float(structdata.ForCtyNumofsucceed)/float(structdata.ForCtyNumofdlv)
    else:
        pass
    g_Countrydict[structdata.ToCountry]=structdata
    Deliver2ESWithpath(structdata.Data2Eightjson(), "sdndeliverstat", "doc", structdata.sendid2)

if __name__ =='__main__':
    #pdb.set_trace()
    data = {'lid': '1', 'domain': '1', 'optime': 1L, 'ip': '1', 'CM_desc': '1', 'CM_from': '1', 'CM_Index': 3L, 'CM_to': '1','destip': '1', 'Score': 1.0, 'result': 1L, 'Eval': '1', 'tid': '1', 'subject': '1', 'bindip': '1'}
    structlogdata = Clogdata()
    structlogdata.SetAttr(data)
    Rejectmailrate(structlogdata)