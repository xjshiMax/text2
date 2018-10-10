#-*-coding:utf-8-*-
'''
    日志
'''
import time
import datetime
import logging
import pdb
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
g_timestamp=0
g_debug=0
g_info=1
g_warning=2
g_error=3
class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,'_instance'):
            orig=super(Singleton,cls)
            cls._instance=orig.__new__(cls,*args,**kwargs)
        return cls._instance
class eslog(Singleton):
    def __init__(self):
        self.logger = logging.getLogger("coremail_eslog")
        self.filename=""
        self.Formatter=logging.Formatter(fmt="[%(asctime)s][%(filename)s:%(lineno)d] [%(levelname)s]  %(message)s")
        self.CheckFileHandle("pyserverlog/")

    def getfilename(self):
        strdate=datetime.datetime.now().strftime('%Y-%m-%d')+"es.log"
        return strdate
    def CheckFileHandle(self,onlypath=""):
        #pdb.set_trace()
        fullfilename=onlypath+self.getfilename()
        if(self.filename!=fullfilename):
            if(self.filename!=""):
                self.logger.removeHandler(self.FileHandler)
            if self.logger.handlers:
                self.logger.handlers=[]
            temphandle=logging.FileHandler(fullfilename,encoding="utf-8")
            temphandle.setFormatter(self.Formatter)
            self.logger.addHandler(temphandle)
            self.filename = fullfilename
            self.FileHandler=temphandle
    def setlevel(self,level):
        if level==0:
            self.logger.setLevel(logging.DEBUG)
        elif level==1:
            self.logger.setLevel(logging.INFO)
        elif level==2:
            self.logger.setLevel(logging.WARNING)
        elif level==3:
            self.logger.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.INFO)
def getlogInst():
    log = eslog()
    log.logger.info("get log instance %d"%(id(log)))
    return log
def getloggerPrint():
    log = eslog()
    log.setlevel(1)
    log.logger.info("get logger %d" % (id(log)))
    pdb.set_trace()
    logger=log.logger
    return (logger)
def testeslog():
    global g_timestamp
    log=eslog()
    log.setlevel(0)
    str1="fll"
    log.debug("find you %s"%(str1))
    i = 0
    while i < 1000:
        plog = logging.getLogger("d")
        plog.debug("create a obj")
        timed = int(round(time.time() * 1000)) - g_timestamp
        g_timestamp = int(round(time.time() * 1000))
        print "delay=%d---i=%d" % (timed, i)
        i = i + 1


if __name__=="__main__":
    log = getloggerPrint()
    log.info("in test1")
    log.info("in teo")