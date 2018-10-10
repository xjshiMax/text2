#!/usr/bin/python
#! -*- encoding:utf-8  -*-

'''
	This moudule is output the log while the program is running.
	This moudule can help us to find something wrong about the program,and it's helpful to help us to get the programs breakpoint so that we can slove it.
'''

import logging as pylog
import logging.config
import time

Loglevel = (pylog.FATAL,pylog.CRITICAL,pylog.ERROR,pylog.WARNING,pylog.INFO,pylog.DEBUG)

class plog(object):

	def __init__(self,logfileName):
		self.fileName = logfileName
		self.logger = pylog.getLogger(__name__)
		self.formatter = pylog.Formatter('T:(%(thread)d)(%(filename)s %(asctime)s) [%(levelname)s:%(lineno)d] %(message)s','%H:%M:%S')
		self.handler = pylog.FileHandler("pyserverlog/"+logfileName)
		#self.handler = pylog.FileHandler(logfileName)
		self.level = 0
		#pylog.config.fileConfig(" ./pyserverlog/")
	def getLogger(self,level):
		self.level = level
		self.logger.setLevel(level)
		self.handler.setLevel(level)
		self.handler.setFormatter(self.formatter)
		self.logger.addHandler(self.handler)
		return self

	def log(self,message):
		level = self.level
		if level == Loglevel[0] or level == Loglevel[1]:#Fatal
			self.logger.fatal(message)
		elif level == Loglevel[2]:#Error
			self.logger.error(message)
		elif level == Loglevel[3]:#Warning
			self.logger.warn(message)
		elif level == Loglevel[4]:#Info
			self.logger.info(message)
		elif level == Loglevel[5]:#Debug
			self.logger.debug(message)
		self.logger.removeHandler(self.handler)

class gError(object):

	def __init__(self,logpath):
		self.pathname = logpath
		self.filename = self.pathname + time.strftime("%Y_%m_%d",time.localtime(int(time.time()))) + ".log"
		self.level = 0

	def getLogger(self,level):
		self.level = level
		if int(time.time()) % 86400 == 86400:#提示要切換文件了
			self.filename = self.pathname + logpath + time.strftime("%Y_%m_%d",time.localtime(int(time.time()))) + ".log"
		return self
	def log(self,message):
		p = plog(self.filename)
		p.getLogger(self.level).log(message)
		
	 

g_timestamp=0
def main():
	global g_timestamp
	g = gError('proxy')
	i=0
	while i<1000:
		g.getLogger(pylog.ERROR).log("This is make a mistake by errors! %s Don't ignore it!"%("asdas"))
		g.getLogger(pylog.WARNING).log("This is make a mistake by errors! Don't ignore it!")
		g.getLogger(pylog.INFO).log("This is make a mistake by errors! Don't ignore it!")
		g.getLogger(pylog.DEBUG).log("This is make a mistake by errors! Don't ignore it!")
		timed = int(round(time.time() * 1000)) - g_timestamp
		g_timestamp = int(round(time.time() * 1000))
		print "delay=%d---i=%d"%(timed,i)
		i=i+1

if __name__ == '__main__':
	main()
