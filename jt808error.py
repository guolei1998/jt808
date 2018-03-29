# -*- coding: utf8 -*-

class JT808Error(Exception):
    """Base class for JT808 errors
    """

class JT808ServerError(JT808Error):
    """Raised for client-side errors
    """

class JT808ServerConnectionCorruptedError(JT808ServerError):
    """Raised when operations are attempted after the client has received corrupt data
    """

class JT808ServerSessionStateError(JT808ServerError):
    """Raised when illegal operations are attempted for the client's session state
    """
    
class JT808TransactionError(JT808Error):
    """Raised for transaction errors
    """
    def __init__(self, response, request=None):
        self.response = response
        self.request = request
        JT808Error.__init__(self, self.getErrorStr())
        
    def getErrorStr(self):
        errCodeName = str(self.response.status)
        errCodeVal = constants.command_status_name_map[errCodeName]
        errCodeDesc = constants.command_status_value_map[errCodeVal]
        return '%s (%s)' % (errCodeName, errCodeDesc)

class JT808GenericNackTransactionError(JT808TransactionError):
    """Raised for transaction errors that return generic_nack
    """

class JT808RequestTimoutError(JT808Error):
    """Raised for timeout waiting waiting for response
    """

class JT808SessionInitTimoutError(JT808RequestTimoutError):
    """Raised for timeout waiting waiting for response
    """

class JT808ProtocolError(JT808Error):
    """Raised for JT808 protocol errors
    """

class SessionStateError(JT808ProtocolError):
    """Raise when illegal operations are received for the given session state
    """

class MSGParseError(JT808ProtocolError):
    """Parent class for MSG parsing errors
    """

class MSGCorruptError(MSGParseError):
    """Raised when a complete MSG cannot be read from the network
    """

class JT808BindError(JT808Error):
    """Raised for JT808 bind errors
    """