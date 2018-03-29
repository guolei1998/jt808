# -*- coding: utf8 -*-
import sys
if sys.platform.find('linux') > -1:
    from twisted.internet import pollreactor
    pollreactor.install()
import logging, struct, binascii, itertools
from twisted.internet.protocol import Protocol, ClientFactory, Factory
from twisted.internet import defer, reactor
from twisted.internet.threads import deferToThread
from collections import namedtuple
import Queue, threading, time, datetime
import redis

from enum import Enum
from jt808protocol import *
from jt808error import *

REDISHOST='127.0.0.1'
REDISHOST='222.169.228.116'

JT808SessionStates = Enum('NONE','OPEN',)

JT808OutboundTxn = namedtuple('JT808OutboundTxn', 'request, timer, ackDeferred')
JT808OutboundTxnResult = namedtuple('JT808OutboundTxnResult', 'JT808, request, response')

sys.setrecursionlimit(500000)


class RedisQueue(object):  
    """Simple Queue with Redis Backend"""  
    def __init__(self, name, namespace='queue', **redis_kwargs):  
        """The default connection parameters are: host='localhost', port=6379, db=0"""  
        self.__db= redis.Redis(**redis_kwargs)  
        self.key = '%s:%s' %(namespace, name)  
  
    def qsize(self):  
        """Return the approximate size of the queue."""  
        return self.__db.llen(self.key)  
  
    def empty(self):  
        """Return True if the queue is empty, False otherwise."""  
        return self.qsize() == 0  
  
    def put(self, item):  
        """Put item into the queue."""  
        self.__db.rpush(self.key, item)  
  
    def get(self, block=True, timeout=None):  
        """Remove and return an item from the queue.  
 
        If optional args block is true and timeout is None (the default), block 
        if necessary until an item is available."""  
        if block:  
            item = self.__db.blpop(self.key, timeout=timeout)  
        else:  
            item = self.__db.lrpop(elf.key)  
  
        if item:  
            item = item[1]  
        return item  
  
    def get_nowait(self):  
        """Equivalent to get(False)."""  
        return self.get(False) 
    


class JT808(Protocol):
    def __init__( self ):
        self.recvBuffer = ""
        self.termPhone = ''
        self.log = logging.getLogger("JT808")
        self.disconnectedDeferred = defer.Deferred()
        self.connectionCorrupted = False
        self.dataRequestHandler =  None #self.factory.dataRequestHandler
        self.MSGReadTimer = None
        #self.enquireLinkTimer = None
        #self.inactivityTimer = None
        #self.enquireLinkTimerSecs = 10
        #self.inactivityTimerSecs = 120        
        self.MSGReadTimerSecs = 10
        self.responseTimerSecs = 60
        self.inTxns = {}
        self.outTxns = {}
        self.lastSeqNum = 0
        self.sessionState = JT808SessionStates.NONE
        
    def connectionMade(self):
        self.__buffer = ''
        self.dataRequestHandler = self.factory.dataRequestHandler
        Protocol.connectionMade(self)
        self.sessionState = JT808SessionStates.OPEN
        self.log.warning("Connection established")
        
    def connectionLost( self, reason ):
        Protocol.connectionLost( self, reason )
        self.log.warning("Disconnected: %s" % reason)
        #self.sessionState = JT808SessionStates.NONE
        if self.termPhone in self.factory.clients:
            self.factory.clients.pop(self.termPhone)
        self.disconnectedDeferred.callback(None)
        
    def onJT808Operation(self):
        """Called whenever an JT808MSG is sent or received
        """
        pass

    def dataReceived( self, data ):
        self.recvBuffer = self.recvBuffer + data
        while True:
            if self.connectionCorrupted:
                return
            msg = self.readMessage()
            if msg is None:
                break
            self.endMSGRead()
            self.rawMessageReceived(msg)
            
        if len(self.recvBuffer) > 0:
            self.incompleteMSGRead()

    def incompleteMSGRead(self):
        if self.MSGReadTimer and self.MSGReadTimer.active():
            return
        self.MSGReadTimer = reactor.callLater(self.MSGReadTimerSecs, self.onMSGReadTimeout)

    def endMSGRead(self):
        if self.MSGReadTimer and self.MSGReadTimer.active():
            self.MSGReadTimer.cancel()

    def readMessage(self):
        pos = self.recvBuffer.find('~')
        if pos > -1:
            pos2 = self.recvBuffer[pos+1:].find('~') 
            if pos2 > -1:
                message = self.recvBuffer[pos:pos2+pos+1+1]
                self.recvBuffer = self.recvBuffer[pos2+pos+1+1+1:]
                return message
        return None


    def onMSGReadTimeout(self):
        self.log.critical('MSG read timed out. Buffer is now considered corrupt')
        self.corruptDataRecvd()

    def corruptDataRecvd(self):
        self.log.critical("Connection is corrupt!!! Shutting down...")
        self.connectionCorrupted = True
        self.cancelOutboundTransactions(JT808ServerConnectionCorruptedError())
        #self.shutdown()

    def cancelOutboundTransactions(self, error):
        for txn in self.outTxns.values():
            self.endOutboundTransactionErr(txn.request, error)

    def rawMessageReceived( self, data ):
        msg = None
        try:
            msg = Message.parseBuild(data)
        except MSGCorruptError, e:
            self.log.exception(e)
            self.log.critical("Received corrupt MSG %s" % binascii.b2a_hex(data))
            self.corruptDataRecvd()
        else:
            self.MSGReceived(msg)

    def MSGReceived( self, msg ):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Received MSG: %s" % msg)
        
        self.onJT808Operation()
        if not self.termPhone:
            self.termPhone = msg.termPhone
            self.factory.clients[self.termPhone] = self

        if isinstance(msg, MessageRequest):
            self.MSGRequestReceived(msg)
        elif isinstance(msg, MessageResponse):
            self.MSGResponseReceived(msg)
        else:
            getattr(self, "onMSG_%s" % type(msg).__name__)(msg)

    def MSGResponseReceived(self, msg):
        if msg.seqNum not in self.outTxns:
            self.log.critical('Response MSG received with unknown outbound transaction sequence number %s' % msg)
            return
        self.endOutboundTransaction(msg)

    def MSGRequestReceived(self, reqmsg):
        if hasattr(self,"onMSGRequest_%s" % type(reqmsg).__name__) : # 请求我的内容，我做
            getattr(self, "onMSGRequest_%s" % type(reqmsg).__name__)(reqmsg)
            return
        if self.dataRequestHandler is None:
            return self.fatalErrorOnRequest(datamsg, 'Missing dataRequestHandler')
        
        self.doMSGRequest(reqmsg, self.dataRequestHandler)

    def onMSGRequest_JT808_term_heart(self,reqmsg):
        self.sendResponse(reqmsg,MsgID=reqmsg.commandId,Seq = reqmsg.seqNum)

    def sendResponse(self, reqmsg, **params):
        self.sendMessage(reqmsg.requireAck(reqmsg.termPhone,reqmsg.seqNum, **params))
    
    def setDataRequestHandler(self, handler):
        self.dataRequestHandler = handler

    def getDisconnectedDeferred(self):
        return self.disconnectedDeferred


    def sendRequest(self, msg, timeout):
        return defer.maybeDeferred(self.doSendRequest, msg, timeout)
        
    def doSendRequest(self, msg, timeout):
        if self.connectionCorrupted:
            raise JT808ServerConnectionCorruptedError()

        if not isinstance( msg, MessageRequest ) or msg.requireAck is None:
            raise Exception("Invalid msg to send: %s" % msg)

        msg.seqNum = self.claimSeqNum()
        self.sendMessage(msg)
        return self.startOutboundTransaction(msg, timeout)
    def doMSGRequest(self, reqmsg, handler): #给我的内容，我做处理
        self.startInboundTransaction(reqmsg)
        
        handlerCall = defer.maybeDeferred(handler, self, reqmsg)
        handlerCall.addCallback(self.MSGRequestSucceeded, reqmsg)
        handlerCall.addErrback(self.MSGRequestFailed, reqmsg)
        handlerCall.addBoth(self.MSGRequestFinished, reqmsg)
    def MSGRequestSucceeded(self, result, reqmsg, **params):
        if reqmsg.requireAck:
            self.sendResponse(reqmsg, MsgID=reqmsg.commandId,Seq = reqmsg.seqNum, **params)
        
    def MSGRequestFailed(self, error, reqmsg):
        self.log.critical('Exception raised handling inbound MSG [%s] hex[%s]: %s' % (reqmsg, binascii.b2a_hex(reqmsg.generate()), error))
        if reqmsg.requireAck:
            self.sendResponse(reqmsg, MsgID=reqmsg.commandId,Seq = reqmsg.seqNum,Result=1)
        #self.shutdown()

    def MSGRequestFinished(self, result, reqmsg):
        self.endInboundTransaction(reqmsg)                    
        return result   
    def finishTxns(self):
        return defer.DeferredList([self.finishInboundTxns(), self.finishOutboundTxns()])
    
    def finishInboundTxns(self):
        return defer.DeferredList(self.inTxns.values())
        
    def finishOutboundTxns(self):
        return defer.DeferredList([txn.ackDeferred for txn in self.outTxns.values()])
    def claimSeqNum(self):
        self.lastSeqNum += 1
        return self.lastSeqNum
    def disconnect(self):
        self.log.warning("Disconnecting...")
        self.sessionState = JT808SessionStates.NONE
        self.transport.loseConnection()

    def shutdown(self):
        """ Unbind if appropriate and disconnect """

        if self.sessionState in (JT808SessionStates.OPEN,):
            self.log.warning("Shutdown requested...disconnecting")
            self.disconnect()
        else:
            self.log.debug("Shutdown already in progress")

    def startInboundTransaction(self, reqmsg):
        if reqmsg.seqNum in self.inTxns:
            raise JT808ProtocolError('Duplicate message id [%s] received.  Already in progess.' % reqmsg.seqNum)
        txnDeferred = defer.Deferred()
        self.inTxns[reqmsg.seqNum] = txnDeferred
        self.log.debug("Inbound transaction started with message id %s" % reqmsg.seqNum)
        return txnDeferred
    
    def endInboundTransaction(self, reqmsg):
        if not reqmsg.seqNum in self.inTxns:
            raise ValueError('Unknown inbound sequence number in transaction for request MSG %s' % reqmsg)
            
        self.log.debug("Inbound transaction finished with message id %s" % reqmsg.seqNum)
        self.inTxns[reqmsg.seqNum].callback(reqmsg)
        del self.inTxns[reqmsg.seqNum]


    def sendMessage(self, msg):

        self.transport.write( msg.generate() )
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Sending MSG: %s" % msg)
        self.onJT808Operation()
        
    def sendPlatRequest( self, datamsg ):
        """Send a JT808 Request Message

        Result is a Deferred object
        """
        return self.sendRequest(datamsg, self.responseTimerSecs)
    def startOutboundTransaction(self, reqmsg, timeout):
        if reqmsg.seqNum in self.outTxns:
            raise ValueError('Seq number [%s] is already in progess.' % reqmsg.seqNum)
        
        ackDeferred = defer.Deferred()
        timer = reactor.callLater(timeout, self.onResponseTimeout, reqmsg, timeout)
        self.outTxns[reqmsg.seqNum] = JT808OutboundTxn(reqmsg, timer, ackDeferred)
        self.log.debug("Outbound transaction started with message id %s" % reqmsg.seqNum)
        return ackDeferred

  
    def fatalErrorOnRequest(self, reqmsg, errMsg):
        self.log.critical(errMsg)
        self.sendResponse(reqmsg, MsgID=reqmsg.commandId,Seq = reqmsg.seqNum,Result=1)
        #self.shutdown()

    def onResponseTimeout(self, reqmsg, timeout):
        pass

    def endOutboundTransaction(self, respmsg):
        txn = self.closeOutboundTransaction(respmsg.seqNum)
        if not isinstance(respmsg, txn.request.requireAck):
            txn.ackDeferred.errback(SMPPProtocolError("Invalid MSG response type [%s] returned for request type [%s]" % (type(respmsg), type(txn.request))))
            return

        txn.ackDeferred.callback(JT808OutboundTxnResult(self, txn.request, respmsg))
        return
        
    def endOutboundTransactionErr(self, reqmsg, error):
        self.log.exception(error)
        txn = self.closeOutboundTransaction(reqmsg.seqNum)
        #Do errback
        txn.ackDeferred.errback(error)
    def closeOutboundTransaction(self, seqNum):        
        self.log.debug("Outbound transaction finished with message id %s" % seqNum)        
        txn = self.outTxns[seqNum]
        #Remove txn
        del self.outTxns[seqNum]
        #Cancel response timer
        if txn.timer.active():
            txn.timer.cancel()
        return txn
recvQueue = RedisQueue('recvQueue',host=REDISHOST)   
def msgHandler(jt808, msg):
    #print jt808.factory.clients
    #print msg
    
    pass

class JT808Factory(Factory):

    protocol = JT808
    clients={}

    def __init__(self):
        self.buildProtocolDeferred = defer.Deferred()   
        self.log = logging.getLogger("JT808")
        self.dataRequestHandler = msgHandler
        self.sendQueue = RedisQueue('sendQueue',host=REDISHOST)
        self.sendcommand()
        
    def sendcommand(self,towho=None,data=None):
        if data is not None and towho and towho in self.clients:
            self.clients[towho].transport.write(data)
        deferToThread(self.sendQueue.get).addCallback(self.sendcommand)   
        


def main():
    #logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(levelname)s:%(message)s',datefmt='%Y-%m-%d %H:%M:%S')

    log = logging.getLogger("JT808")
    log.setLevel(logging.INFO)
    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler('JT808.log')
    fh.setLevel(logging.INFO)
    # 再创建一个handler，用于输出到控制台
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # 给logger添加handler
    log.addHandler(fh)
    log.addHandler(ch)
    
    host = reactor.listenTCP(8562, JT808Factory(),interface='0.0.0.0')

    log.info('JT808 Serving on %s.' % ( host.getHost(),)) 

    reactor.run()
    
if __name__ == '__main__':
    main()