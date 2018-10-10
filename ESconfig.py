#-*-coding:utf8 -*-
'''
    缓存配置，缓存服务挂掉时，程序里的数据，以及elasticsearch的搜索点
'''
import ConfigParser
import os
import pdb
import json
from Clogdata import Clogdata
from EShelplog import getlogInst
import os
g_loginst=getlogInst()
cf = ConfigParser.ConfigParser()
cf.read('config/ess.conf')
class ESconfig:
    def __init__(self):
        global cf
        secs = cf.sections()
        self.Scalesize=0
    def setsize(self,size):
        self.Scalesize=size
        ret=cf.set("elasticsearchpara","scalsize",size)
        with open("config/ess.conf", "w+") as f:
            cf.write(f)

    def getsize(self):
        temp = cf.get("elasticsearchpara", "scalsize")
        self.Scalesize =cf.get("elasticsearchpara", "scalsize")
        return self.Scalesize

class Dictcache:
    def __init__(self):
        self.dict={}
    def Getdict(self):
        global cf
        secs = cf.sections()
        str = cf.get("dictdata", "g_dict")
        self.dict = self.deserialize(str)
        return self.dict
    def Setdict(self,tempdict):
        self.dict = tempdict
        ret = cf.set("dictdata", "g_dict", self.serialize())
        with open("config/ess.conf", "w+") as f:
            cf.write(f)
    def serialize(self):
        strdict=""
        for (key,value) in self.dict.items():
            strdict+=key
            strdict+="@@@"
            valuedata=json.dumps(value.Data2Json())
            strdict+=str(valuedata)
            strdict+="&&"
        strdict=strdict[:-2] #去掉最后一个”&&“
        return strdict

    def deserialize(self,strdata):
        tempdict={}
        strdata=strdata.encode('utf-8')
        filedlist = strdata.split('&&')
        for item in filedlist:
            keyvalue=item.split("@@@",1)
            strobj=keyvalue[1]
            jsonobj=json.loads(strobj.encode('utf-8'))
            logobj=Clogdata()
            logobj.copyAttr(jsonobj)
            tempdict[keyvalue[0]]=logobj
        self.dict=tempdict
        return self.dict
        pass
    def Createconf(self):# 不存在就创建一个
        global  g_loginst
        flag=os.path.exists('config/ess.conf')
        if flag==True:
            pass
            g_loginst.logger.info("配置文件存在，不需要创建")
        else:
            g_loginst.logger.info("配置文件不存在，创建文件")
            cf.add_section('elasticsearchpara')
            cf.set('elasticsearchpara','scalsize',0)
            cf.add_section('dictdata')
            cf.set('dictdata','g_dict',{})
            pdb.set_trace()
            with open('config/ess.conf', 'w') as configfile:
                cf.write(configfile)
if __name__ =='__main__':
    pass
    t=Dictcache()
    pdb.set_trace()
    t.Createconf()
