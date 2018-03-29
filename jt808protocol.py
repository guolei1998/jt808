# -*- coding: utf8 -*-
import struct, logging, sys, StringIO, binascii
import datetime, md5
from bitstring import BitArray

def to_packed_bcd(number):
    numtest = '%012d'% int(number)
    return [(ord(numtest[x])-ord('0'))<<4 | (ord(numtest[x+1])-ord('0')) for x in range(0, len(numtest), 2)]
def to_unpack_bcd(bcdarray):
    return ''.join(['%x'% c for c in bcdarray])
def get_check(msg):
    rst = 0
    mlen = len(msg)
    for x in range(mlen):
        rst ^= ord(msg[x])
    return rst
def a_to_b(msg):
    rst = msg.replace('\x7d','\x7d\x01')
    rst = rst.replace('\x7e','\x7d\x02')
    return rst
def b_to_a(msg):
    rst = msg.replace('\x7d\x02','\x7e')
    rst = rst.replace('\x7d\x01','\x7d')
    return rst

class Message(object): 
    flagBit =  struct.pack('>B',0x7e)
    termPhone=''
    commandId = None
    fenbaoLen = 1000 #最大值1023
    baocount = 1
    baoid = 1
    structParams = []
    def __init__(self, termPhone=None, seqNum=None, **kwargs):
        self.body_Length = 0
        self.seqNum = seqNum
        self.termPhone = termPhone
        for sParam in self.structParams:
            setattr(self, sParam[0], sParam[2])
        for key, value in kwargs.items():
            setattr(self, key, value)
    def generate(self):
        rst = []
        body = ''
        for sParam in self.structParams:
            fmt = sParam[1]
         
            if type(sParam[2]) == str and sParam[1].find('*') > -1:
                msglen = len(getattr(self,sParam[0],''))
                fmt = fmt.replace('*',str(msglen+1))
                body = body + mypack(fmt, getattr(self, sParam[0])+'\0')
                continue
            body = body + mypack(fmt, getattr(self, sParam[0]))
        self.body_Length = len(body)
        bcd = struct.pack('>6B',*to_packed_bcd(self.termPhone) )
        jiami = 0 
        fenbao = 0
        baocount = 1
        if (self.body_Length > self.fenbaoLen): # 分包
            fenbao = 1
            baocount = self.body_Length / self.fenbaoLen + 1
        for bao in xrange(baocount):
            bodylen = self.fenbaoLen if (bao+1) < baocount else  self.body_Length % self.fenbaoLen
            msgprop = struct.pack('>H',BitArray('0b00%s00%s, uint:10=%s' % (fenbao,jiami,bodylen)).uint)
            baoadd = struct.pack('>HH',baocount, bao+1) if fenbao else ''
            header = struct.pack('>H', self.commandId) + msgprop + bcd + struct.pack('>H', self.seqNum) + baoadd
            msgbody = body[self.fenbaoLen*bao:self.fenbaoLen*(bao+1)]
            check = struct.pack('>B',get_check(header+msgbody))
            rst.append( self.flagBit + a_to_b(header+msgbody+check) + self.flagBit)
        return ''.join(rst)


    @staticmethod
    def parseBuild(data):
        pos = 0
        dlen = len(data)
        content = data[1:dlen-1]
        msg = None
        content = b_to_a(content)
        commandId = struct.unpack('>H',content[:2])[0]

        msg = getMsgClass(commandId)()
        if msg:
            msg.commandId = commandId
            msg.termPhone = str(int(to_unpack_bcd(struct.unpack('>6B',content[4:10]))))
            msg.seqNum = int(struct.unpack('>H',content[10:12])[0])
            msgprop = BitArray('uint:16=%s' % struct.unpack('>H',content[2:4])[0]).bin
            fenbao = int(msgprop[2:3])
            jiami = int(msgprop[5:6])
            bodylen = int(msgprop[7:],2)
            pos=12
            if fenbao:
                msg.baocount,msg.baoid = struct.unpack('>HH',content[12:14])
                pos=14
            
            for sParam in msg.structParams:
                fmt = sParam[1]
                if sParam[2] == 'BCD':
                    setattr(msg, sParam[0], to_unpack_bcd(struct.unpack(fmt,content[pos:pos+struct.calcsize(fmt)])))
                    continue
                if type(sParam[2]) == str:
                    if sParam[1].find('*') > -1:
                        msglen = content[pos:].find('\0')+1
                        fmt = fmt.replace('*',str(msglen))
                    tmpdata = content[pos:pos+struct.calcsize(fmt)]
                    setattr(msg, sParam[0], str(tmpdata).strip('\0'))
                    continue

                setattr(msg, sParam[0], struct.unpack(sParam[1],content[pos:pos+struct.calcsize(fmt)])[0])

                pos = pos + struct.calcsize(fmt)

        return msg
    def __repr__(self):
        r = "MSG [command: %x, sequence_number: %s" % (self.commandId,self.seqNum)
        for sParam in self.structParams:
            if sParam[2] == 'r':
                r += "\n%s: %r" % (sParam[0], getattr(self, sParam[0]).encode('hex'))
            elif sParam[0] == 'MsgID':
                r += "\n%s: %x" % (sParam[0], getattr(self, sParam[0]))
            else:
                tmpdata = getattr(self, sParam[0])
                r += "\n%s: %s" % (sParam[0], tmpdata)
        r += '\n]'
        return r

class MessageRequest(Message):
    requireAck = None
    
class MessageResponse(Message):
    pass
    

#终端通用应答
class JT808_term_resp(MessageResponse):
    commandId = 0x0001
    structParams = [
        ('Seq','>H',0),
        ('MsgID','>H',0),
        ('Result','>B',0),
    ]

#平台通用应答
class JT808_plat_resp(MessageResponse):
    commandId = 0x8001
    structParams = [
        ('Seq','>H',0),
        ('MsgID','>H',0),
        ('Result','>B',0),
    ]
    
#终端心跳
class JT808_term_heart(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0002    
 
#终端注册应答
class JT808_termreg_resp(MessageResponse):
    commandId = 0x8100
    structParams = [
        ('Seq','>H',0),
        ('Result','>B',0),
        ('Auth','>*B',''),
    ]

#终端注册
class JT808_term_reg(MessageRequest):
    requireAck = JT808_termreg_resp
    commandId = 0x0002    
    structParams = [
        ('Prov','>H',0),
        ('City','>H',0),
        ('Man','>5B',''),
        ('Termtype','>8B',''),
        ('TermID','>7B',''),
        ('Platecolor', '>B', 0),
        ('Plateno','>*B',''),
    ]

#终端注销
class JT808_term_unreg(MessageRequest):
    #requireAck = JT808_plat_resp
    commandId = 0x0003
    
#终端鉴权
class JT808_term_auth(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0102
    structParams = [
        ('Auth','>*B',''),
    ]
    
#设置终端参数
class JT808_plat_setpara(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8103
    structParams = [
        ('ParaCount','>B',0),
        ('ParaArray','>*B',0),
    ]
    
#终端参数应答
class JT808_platqterm_resp(MessageResponse):
    commandId = 0x0104
    structParams = [
        ('Seq','>H',0),
        ('ParaCount','>B',0),
        ('ParaArray','>*B',''),
    ]

#查询终端参数
class JT808_plat_qterm(MessageRequest):
    requireAck = JT808_platqterm_resp
    commandId = 0x8104
    

    
#控制终端
class JT808_plat_cterm(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8105
    structParams = [
        ('Command','>B',0),
        ('CommandString','>*B',''),
    ]
    
#位置信息汇报
class JT808_term_loc(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0200
    structParams = [
        ('SOS','>L',0),
        ('Status','>L',0),
        ('Lat','>L',0),
        ('Lng','>L',0),
        ('Height','>H',0),
        ('Speed','>H',0),
        ('Direction','>H',0),
        ('Time','>6B','BCD'),
        ('AddMsg','>*B',''),
    ]
    

#位置信息查询应答
class JT808_platqloc_resp(MessageResponse):
    commandId = 0x0201
    structParams = [
        ('Seq','>H',0),
        ('SOS','>L',0),
        ('Status','>L',0),
        ('Lat','>L',0),
        ('Lng','>L',0),
        ('Height','>H',0),
        ('Speed','>H',0),
        ('Direction','>H',0),
        ('Time','>6B','BCD'),
        ('AddMsg','>*B',''),
    ]

#位置信息查询
class JT808_plat_qloc(MessageRequest):
    requireAck = JT808_platqloc_resp
    commandId = 0x8201

    
#临时位置跟踪
class JT808_plat_tmptrace(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8202
    structParams = [
        ('Interval','>H',0),
        ('Validity','>L',0),
    ]
    
#文本信息下发
class JT808_plat_sendtxt(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8300
    structParams = [
        ('Flag','>B',0),
        ('Content','>*B',''),
    ]

#事件设置
class JT808_plat_setevent(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8301
    structParams = [
        ('EventType','>B',0),
        ('EventCount','>B',0),
        ('EventArray','>*B',''),
    ]

#事件报告
class JT808_term_eventreport(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0301
    structParams = [
        ('EventID','>B',0),
    ]
    
#提问应答
class JT808_platquest_resp(MessageResponse):
    commandId = 0x0302
    structParams = [
        ('Seq','>H',0),
        ('AnswerID','>B',0),
    ]

#提问下发
class JT808_plat_question(MessageRequest):
    requireAck = JT808_platquest_resp
    commandId = 0x0301
    structParams = [
        ('Flag','>B',0),
        ('QuestLen','>B',0),
        ('Quest','>*B',''),
        ('AnswerArray','>*B',''),
    ]




#信息点播菜单设置
class JT808_plat_infomenu(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8303
    structParams = [
        ('SetType','>B',0),
        ('InfoCount','>B',0),
        ('InfoArray','>*B',''),
    ]

#信息点播/取消
class JT808_term_infooper(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0303
    structParams = [
        ('InfoType','>B',0),
        ('Operate','>B',0),
    ]

#信息服务
class JT808_plat_infoserv(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8304
    structParams = [
        ('InfoType','>B',0),
        ('InfoCount','>H',0),
        ('InfoContent','>*B',''),
    ]
    
#电话回拨
class JT808_plat_phonecallback(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8400
    structParams = [
        ('Flag','>B',0),
        ('PhoneNo','>*B',''),
    ]  

#设置电话本
class JT808_plat_setphonebook(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8401
    structParams = [
        ('SetType','>B',0),
        ('ManCount','>B',0),
        ('ManArray','>*B',''),
    ]  
    
#车辆控制应答
class JT808_platvehctrl_resp(MessageResponse):
    commandId = 0x0500
    structParams = [
        ('Seq','>H',0),
        ('SOS','>L',0),
        ('Status','>L',0),
        ('Lat','>L',0),
        ('Lng','>L',0),
        ('Height','>H',0),
        ('Speed','>H',0),
        ('Direction','>H',0),
        ('Time','>6B','BCD'),
        ('AddMsg','>*B',''),
    ]
    
#车辆控制
class JT808_plat_vehctrl(MessageRequest):
    requireAck = JT808_platvehctrl_resp
    commandId = 0x8500
    structParams = [
        ('Flag','>B',0),
    ] 
    


#设置圆形区域
class JT808_plat_setcircle(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8600
    structParams = [
        ('SetType','>B',0),
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 


#删除圆形区域
class JT808_plat_delcircle(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8601
    structParams = [
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 
    
#设置矩形区域
class JT808_plat_setrectangle(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8602
    structParams = [
        ('SetType','>B',0),
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 


#删除矩形区域
class JT808_plat_delrectangle(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8603
    structParams = [
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 

#设置多边形区域
class JT808_plat_setpolygon(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8604
    structParams = [
        ('SetType','>B',0),
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 


#删除多边形区域
class JT808_plat_delpolygon(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8605
    structParams = [
        ('AreaCount','>B',0),
        ('AreaArray','>*B',''),
    ] 
    
    
#设置路线
class JT808_plat_setroute(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8606
    structParams = [
        ('RouteID','>L',0),
        ('RouteType','>H',0),
        ('StartTime','>6B','BCD'),
        ('EndTime','>6B','BCD'),
        ('PointCount','>H',0),
        ('PointArray','>*B',''),
    ] 


#删除路线
class JT808_plat_delroute(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8607
    structParams = [
        ('RouteCount','>B',0),
        ('RouteArray','>*B',''),
    ] 


#行驶记录数据采集
class JT808_plat_collectdriving(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8700
    structParams = [
        ('Command','>B',0),
    ] 


#行驶记录上传
class JT808_term_drivingupload(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x0700
    structParams = [
        ('Seq','>H',0),
        ('Command','>B',0),
        ('DrivingData','>*B',''),
    ] 

#行驶记录参数下传
class JT808_plat_setdrivingpara(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8701
    structParams = [
        ('Command','>B',0),
        ('Content','>*B',''),
    ] 
    
#电子运单上报
class JT808_term_reportsheet(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x0701
    structParams = [
        ('SheetLen','>L',0),
        ('SheetContent','>*B',''),
    ] 
    
#驾驶员身份信息上报
class JT808_term_reportdriver(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x0702
    structParams = [
        ('NameLen','>B',0),
        ('Name','>*B',''),
        ('IDcard','>20B',''),
        ('Qualification','>40B',''),
        ('OrgNameLen','>B',0),
        ('OrgName','>*B',''),
    ] 
    
#多媒体事件信息上传
class JT808_term_reportmedia(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x0800
    structParams = [
        ('MediaID','>L',0),
        ('MediaType','>B',0),
        ('MediaCode','>B',0),
        ('EventCode','>B',0),
        ('ChannelID','>B',0),
    ] 
    
#多媒体数据上传应答
class JT808_termmediadata_resp(MessageResponse):
    commandId = 0x8800
    structParams = [
        ('MediaID','>H',0),
        ('ReSend','>B',0),
        ('ReSendArray','>*B',''),
    ]
    
#多媒体数据上传
class JT808_term_mediadata(MessageRequest):
    requireAck = JT808_termmediadata_resp
    commandId = 0x0801
    structParams = [
        ('MediaID','>L',0),
        ('MediaType','>B',0),
        ('MediaCode','>B',0),
        ('EventCode','>B',0),
        ('ChannelID','>B',0),
        ('MediaData','>*B',''),
    ]



#摄像头立即拍摄命令
class JT808_plat_camera(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8801
    structParams = [
        ('ChannelID','>B',0),
        ('Command','>H',0),
        ('Interval','>H',0),
        ('SaveFlag','>B',0),        
        ('Resolution','>B',0),
        ('Quantity','>B',0),
        ('Brightness','>B',0),
        ('Contrast','>B',0),
        ('Saturation','>B',0),
        ('Color','>B',0),
    ] 
    

#多媒体数据检索应答
class JT808_platqmedia_resp(MessageResponse):
    commandId = 0x0802
    structParams = [
        ('Seq','>H',0),
        ('MediaCount','>H',0),
        ('MediaArray','>*B',''),
    ]
    

#存储多媒体数据检索
class JT808_plat_qmedia(MessageRequest):
    requireAck = JT808_platqmedia_resp
    commandId = 0x8802
    structParams = [
        ('MediaType','>B',0),
        ('ChannelID','>B',0),
        ('EventCode','>B',0),        
        ('StartTime','>6B','BCD'),
        ('EndTime','>6B','BCD'),
    ] 



#存储多媒体数据上传
class JT808_plat_mediaupload(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8803
    structParams = [
        ('MediaType','>B',0),
        ('ChannelID','>B',0),
        ('EventCode','>B',0),        
        ('StartTime','>6B','BCD'),
        ('EndTime','>6B','BCD'),
        ('DelFlag','>B',0),  
    ] 


#录音开始
class JT808_plat_startrecord(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8804
    structParams = [
        ('Command','>B',0),
        ('RecordLength','>H',0),
        ('SaveFlag','>B',0),        
        ('SamplingRate','>B',0),  
    ] 
    
#数据下行透传
class JT808_plat_datadown(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8900
    structParams = [
        ('DataType','>B',0),       
        ('Data','>*B',''),  
    ] 
    
#数据上行透传
class JT808_term_dataup(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0900
    structParams = [
        ('DataType','>B',0),       
        ('Data','>*B',''),  
    ] 
    
#数据压缩上报
class JT808_term_datazipup(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0901
    structParams = [
        ('ZipDataLen','>L',0),       
        ('ZipData','>*B',''),  
    ] 
    
#平台RSA公钥
class JT808_plat_rsapublic(MessageRequest):
    requireAck = JT808_term_resp
    commandId = 0x8A00
    structParams = [
        ('e','>L',0),       
        ('n','>128B',''),  
    ] 

#终端RSA公钥
class JT808_term_rsapublic(MessageRequest):
    requireAck = JT808_plat_resp
    commandId = 0x0A00
    structParams = [
        ('e','>L',0),       
        ('n','>128B',''),  
    ] 


        
MSGS = {}

def _register():
    for msgClass in globals().values():
        try:
            if issubclass(msgClass, Message):
                MSGS[msgClass.commandId] = msgClass
        except TypeError:
            pass

_register()

def getMsgClass(commandId):
    if MSGS.has_key(commandId):
        return MSGS[commandId]
    else:
        return None
    
def mypack(fmt, data):
    if type(data) == str:   
        dlen = struct.calcsize(fmt)
        cdata=[0]*dlen
        for i in range(dlen):
            cdata[i] = 0 if not data[i:i+1] else ord(data[i:i+1])
        return struct.pack(fmt, *cdata)    
    else:
        return struct.pack(fmt, data)    