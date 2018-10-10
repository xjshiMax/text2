#-*-coding:utf-8 -*-
'''
    解析字符串数据
'''
import pdb
CM_Index=0
def ParseLog2sql(source,filedate):
    global  CM_Index
    ALlfielddict={"tid":"","from":"","to":"","domain":"","lid":"","size": 0, "result":0,"subject":"","Eval":"","Score":0.0,"ip":"0.0.0.0" ,"optime":0,"destip":"0.0.0.0","bindip":"0.0.0.0","desc":"","logCreatedate":""}
    strdata = source
   # filetime = strdata[:8]
    #filedate=filedate.replace("_","-")
    ALlfielddict["logCreatedate"]=filedate
    print "original time =%s"%(ALlfielddict["logCreatedate"])
    strdata = strdata[strdata.find('[')+1:]
    strdata = strdata.replace('\r', "")
    strdata = strdata.replace('\n', "")
    strdata = strdata.replace(']', "")
    strdata = strdata.replace("\,",".")
    strdata = strdata.replace("\"","\\\"")

    filedlist = strdata.split(',')
    for each in filedlist:
        keyvalue = each.split(':',1)
        if len(keyvalue)==1:
            if(keyvalue[0]=="bindip" or keyvalue[0]=="destip" or keyvalue[0]=="ip"):
                keyvalue.append("0.0.0.0")
            else:
                keyvalue.append(" ")
        if(keyvalue[0] in ALlfielddict.keys()):
            if (keyvalue[0]=="size" or keyvalue[0]=="result" or keyvalue[0]=="optime" ):
                ALlfielddict[keyvalue[0]] = int(keyvalue[1])
            elif (keyvalue[0]=="Score"):
                ALlfielddict[keyvalue[0]] = float(keyvalue[1])
            else:
                ALlfielddict[keyvalue[0]]=keyvalue[1]
    ALlfielddict["CM_to"] = ALlfielddict["to"]
    ALlfielddict.pop("to")
    ALlfielddict["CM_from"] = ALlfielddict["from"]
    ALlfielddict.pop("from")
    ALlfielddict["CM_desc"] = ALlfielddict["desc"]
    ALlfielddict.pop("desc")
    ALlfielddict["CM_Index"]=CM_Index
    CM_Index+=1
    return ALlfielddict
    pass
if __name__ =="__main__":
    pdb.set_trace()
    data = "00:08:24 [tid:GgENCgB3fRWBcZZb0VkPAQ--.42246S2,from:hermes@pactl.com,to:,domain:corpease.net,lid:icm-hosting,size:,result:2,subject:IMPORT TONNAGE CSV T3 -,Eval:BAYES_50;BM_PASS;CMD_CNT_00_10;CREATE_TO_SEND_INTERVAL_30_XX;CUR_CONN_00_01;DKIM_NEUTRAL;DMARC_NON_ALIGNED;DOMAIN_QUARTER_CNT_20_40;DOMAIN_QUARTER_RCPT_CNT_50_100;DOMAIN_TODAY_CNT_1K_XX;DOMAIN_TODAY_RCPT_CNT_1K_XX;GET_ERROR_HEADER_FIELD;GOT_ATTACHMENT;GOT_EMPTY_CONTENT;HTML_MIME_NO_HTML_TAG;IP_QUARTER_CNT_08_32;IP_TODAY_CNT_2K_4K;JPG_SVM_PROB_00_10;MAILERREP_NULL;NO_REAL_NAME;ONLY_ATTACHMENT;REGION_CN_21;SENDERREP_NULL;STEXT_SVM_PROB_00_10;SUBJECT_CNT_32_64;TEXT_HTML_CNT_00_01;TEXT_PLAIN_CNT_01_03;TO_CC_BCC_CNT_00_02;URLREP_NULL;USER_QUARTER_CNT_20_40;USER_QUARTER_RCPT_CNT_10_50;USER_SEND_INTERVAL_60_300;USER_TODAY_CNT_1K_XX;USER_TODAY_RCPT_CNT_1K_XX;\
    __MIME_BASE64,Score:5.73,ip:223.252.214.166,optime:10,destip:116.236.254.238,bindip:106.2.96.56,desc:450 Broken pipe(CONNECT)]"
    ParseLog2sql(data,1,"tabletest")