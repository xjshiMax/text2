#!/usr/bin/python
# -*- coding: utf-8 -*-
import codecs
import json
#from logInfo import logInfo
#属性列表 
 

CM_Index = 0 

names = ['tid:','from:','to:','domain:','lid:','size:','result:','subject:','Eval:','Score:','ip:','optime:','destip:','bindip:','desc']

def readDataFromFile(logfilename):#日志文件路径
	i = 1
	try:
		f = codecs.open(logfilename,'r',encoding='gbk')
		for line in f.readlines():
			tj = str2Json(line[line.find('[')+1:len(line)-2])
			sql = 'insert into plog('
			sqlvalues = ' ) values ( '
					
			for (key,value) in tj.items():
#				print tj["desc"]
				sql+=  key+ ','
				if type(value) == int or type(value) == float:
					sqlvalues += str(value)
#					print key,value
				else:
					sqlvalues += '\"' +  value + '\"'
				sqlvalues+=','
		f.close()
	except IOError:
		print "can't open such %s file"%logfilename
def str2Json(strs):
	strs = strs.replace("\\,","\\")
	strs = strs.replace("\"","\\\'")
	parts = strs.split(',')
	if len(parts)!=len(names):
		strs = str2Full(strs)
	elif len(parts) == len(names):
		strs = strCheckparts(strs)
	parts = strs.split(',')
	temp = {}
	for i in range(0,len(parts)):
		# 去掉 []
		parts[i] = parts[i].replace('[','')
		parts[i] = parts[i].replace(']','')
		li = parts[i].split(':',1)
		if li[0] == 'size' or li[0] == 'result' or li[0] == 'optime':
			temp[li[0]] = int(li[1])
		elif li[0] == 'Score':
			temp[li[0]] = float(li[1])
		else:
			temp[li[0]] = li[1]
	temp["CM_Index"] = 0
	return temp

def strCheckparts(strs):
	bFull = True
	for i in range(0,len(names)):
		if strs.find(names[i]) == -1:#当前属性缺失
			bFull = False
			break
	# print "bFull:",bFull
	if bFull == False:
		return str2Full(strs)
	elif bFull == True:
		return strs

def str2Full(strs):#补齐str
	strfull = ""
	for i in range(0,len(names)):
		if strs.find(names[i]) == -1:#当前属性缺失
			print "name[%d] lost %s"%(i,names[i])
			if i != 5 or i !=6 or i != 9 or i !=11 :
				strfull += ","  + names[i] 
			else:
				strfull += ","  + names[i] + "0 " 
	strs += strfull + ']'
	
	return strs


