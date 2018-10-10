#-*- coding:utf-8 -*-
'''
    定义原始数据类，解析原始proxy日志
'''
import pdb
class Clogdata:
    def __init__(self):
        self.tid=""
        self.CM_from=""
        self.CM_to=""
        self.domain=""
        self.lid=""
        self.size=0
        self.result=0;
        self.subject=""
        self.Eval=""
        self.Score=0.0
        self.ip=""
        self.optime=0
        self.destip=""
        self.bindip=""
        self.CM_desc=[]
        self.Originaldesc=""
        #结果字段
        self.CM_chain = []
        self.CM_status="" # succed,fail, Reject
        self.CM_AllTime=0
        self.CM_TimeCount=0
        self.country = ''
        self.CM_Index = 0
        self.NumofPassproxy=1
        #补增
        self.logCreatedate=""
        self.CreateTimestamp=0
    def SetAttr(self,sourcedata={}):
        self.tid = sourcedata["tid"]
        self.CM_from = sourcedata["CM_from"]
        self.CM_to = sourcedata["CM_to"]
        self.domain =sourcedata["domain"]
        self.lid = sourcedata["lid"]
        self.size=sourcedata["size"]
        self.result =sourcedata["result"]
        self.subject = sourcedata["subject"]
        self.Eval = sourcedata["Eval"]
        self.Score = sourcedata["Score"]
        self.ip = sourcedata["ip"]
        self.optime = sourcedata["optime"]
        self.destip = sourcedata["destip"]
        self.bindip = sourcedata["bindip"]
        self.Originaldesc=sourcedata["CM_desc"]
        tempdesc={}
        tempdesc[sourcedata["bindip"]]=sourcedata["CM_desc"]
        self.CM_desc.append(tempdesc)
        self.CM_Index = sourcedata["CM_Index"]
        # 结果字段
        li = {}
        li[sourcedata["bindip"]] = sourcedata["optime"]
        self.CM_chain.append(li)
        #self.CM_chain.append(self.CM_desc)
        if(self.result==0):
            self.CM_status="succeed"
        elif self.result==1:
            self.CM_status="fail"
        elif  self.result==2:
            self.CM_status="defer"
        self.CM_AllTime = sourcedata["optime"]
        self.logCreatedate = sourcedata["logCreatedate"]
    def copyAttr(self,sourcedata):
        self.tid = sourcedata["tid"].encode('utf-8')
        self.CM_from = sourcedata["CM_from"].encode('utf-8')
        self.CM_to = sourcedata["CM_to"].encode('utf-8')
        self.domain = sourcedata["domain"].encode('utf-8')
        self.lid = sourcedata["lid"].encode('utf-8')
        self.size = sourcedata["size"]
        self.result = sourcedata["result"]
        self.subject = sourcedata["subject"].encode('utf-8')
        self.Eval = sourcedata["Eval"].encode('utf-8')
        self.Score = sourcedata["Score"]
        self.ip = sourcedata["ip"].encode('utf-8')
        self.optime = sourcedata["optime"]
        self.destip = sourcedata["destip"].encode('utf-8')
        self.bindip = sourcedata["bindip"].encode('utf-8')
        self.CM_desc= sourcedata["CM_desc"]
        self.CM_chain = sourcedata["CM_chain"]
        self.CM_status = sourcedata["CM_status"].encode('utf-8')
        self.CM_AllTime = sourcedata["CM_AllTime"]
        self.country = sourcedata["country"].encode('utf-8')
        self.CM_Index=sourcedata["CM_Index"]
        self.NumofPassproxy=sourcedata["NumofPassproxy"]
        self.logCreatedate = sourcedata["logCreatedate"]
    def reFleshAtrr(self,SameTidLog):
        # 在合并同一封信的日志时，logCreatedate不做修改，以第一条log为准
        self.CM_chain += SameTidLog.CM_chain
        if(SameTidLog.CM_status=="succeed"):
            self.CM_status = "succeed"
        self.CM_AllTime += SameTidLog.optime
        self.CM_desc+=SameTidLog.CM_desc
        self.NumofPassproxy += 1
    def Data2Json(self):
        sdjson={}
        sdjson["tid"]=self.tid
        sdjson["CM_from"]=self.CM_from
        sdjson["CM_to"]=self.CM_to
        sdjson["domain"] =self.domain
        sdjson["lid"]=self.lid
        sdjson["size"]=self.size
        sdjson["result"]= self.result
        sdjson["subject"]=self.subject
        sdjson["Eval"]=self.Eval
        sdjson["Score"]= self.Score
        sdjson["ip"]=self.ip
        sdjson["optime"]=self.optime
        sdjson["destip"]=self.destip
        sdjson["bindip"]=self.bindip
        sdjson["CM_desc"]=self.CM_desc
        sdjson["CM_chain"]=self.CM_chain
        sdjson["CM_status"]=self.CM_status
        sdjson["CM_AllTime"]=self.CM_AllTime
        sdjson["country"] = self.country
        sdjson["CM_Index"] = self.CM_Index
        sdjson["NumofPassproxy"] = self.NumofPassproxy
        sdjson["logCreatedate"] = self.logCreatedate
        return sdjson
    def Data2JsonforOneTwo(self):
        sdjson={}
        sdjson["tid"]=self.tid
        sdjson["CM_from"]=self.CM_from
        sdjson["CM_to"]=self.CM_to
        sdjson["domain"] =self.domain
        sdjson["lid"]=self.lid
        sdjson["size"]=self.size
        sdjson["result"]= self.result
        sdjson["subject"]=self.subject
        sdjson["Eval"]=self.Eval
        sdjson["Score"]= self.Score
        sdjson["ip"]=self.ip
        sdjson["optime"]=self.optime
        sdjson["destip"]=self.destip
        sdjson["bindip"]=self.bindip
        sdjson["CM_desc"]=self.Originaldesc
        sdjson["CM_chain"]=""
        sdjson["CM_status"]=self.CM_status
        sdjson["CM_AllTime"]=self.CM_AllTime
        sdjson["country"] = self.country
        sdjson["CM_Index"] = self.CM_Index
        return sdjson
if __name__ =="__main__":
    teststr = {'lid': '1', 'domain': '1', 'optime': 1L, 'ip': '1', 'CM_desc': '1', 'CM_from': '1', 'CM_Index': 3L, 'CM_to': '1', 'destip': '1', 'Score': 1.0, 'result': 1L, 'Eval': '1', 'tid': '1', 'subject': '1', 'bindip': '1', 'size': 1L}
#{'lid': '1', 'domain': '1', 'optime': 1L, 'ip': '1', 'CM_desc': '1', 'CM_from': '1', 'CM_Index': 4L, 'CM_to': '1', 'destip': '1', 'Score': 1.0, 'result': 1L, 'Eval': '1', 'tid': '1', 'subject': '1', 'bindip': '1', 'size': 1L}
#{'lid': '1', 'domain': '1', 'optime': 1L, 'ip': '1', 'CM_desc': '1', 'CM_from': '1', 'CM_Index': 5L, 'CM_to': '1', 'destip': '1', 'Score': 1.0, 'result': 1L, 'Eval': '1', 'tid': '1', 'subject': '1', 'bindip': '1', 'size': 1L}
#{'lid': '1', 'domain': '1', 'optime': 1L, 'ip': '1', 'CM_desc': '1', 'CM_from': '1', 'CM_Index': 6L, 'CM_to': '1', 'destip': '1', 'Score': 1.0, 'result': 1L, 'Eval': '1', 'tid': '1', 'subject': '1', 'bindip': '1', 'size': 1L} '''
    pdb.set_trace()
    loginst = Clogdata()
    loginst.SetAttr(teststr)
    strjson = loginst.Data2Json()
    print strjson