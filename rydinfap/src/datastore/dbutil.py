#------------------------------------------------------------
# Version 0.1 20101105
#
# dbutils.py
#
# Creation Date:     2012/01/03
# Modification Date:
# Description: This module contains all DB database related classes/methods.
# '20120918' : Added pyodbc.Error Exception Catching
# New Class Style.
# 20130507   : Added pyodbc.Error Exception Catching for exeQry
# 20131007   : Added MSSQL Native Driver
# 20131114   : Added ELSE in exception clauses & repalced sys.exc_type
# 20150529   : Added the exeManyQry Method . Be aware that DB returns -1 for rowcount
# 20151215   : Added connect string for DB2
#-------------------------------------------------------------
__version__ = '20151215'

import sys
import string as ST
import mainmsg as mm

#===============================================================================
# ConnectString
#===============================================================================
# t DS type currently 'oracle', 'SQLLite'. Mandatory by Contract (not nullor blank)
# u username
# p password
# dsn data source name or server name
# db database
# Entries defined on the odbcinst.ini for UX
#[ODBC Drivers]
#NetezzaSQL = Installed
#[NetezzaSQL]
#Driver           = /infa/Informatica/ODBC6.1/Netezza/lib64/libnzodbc.so
#Setup            = /infa/Informatica/ODBC6.1/Netezza/lib64/libnzodbc.so
#
#SQL Server = Installed
#[SQL Server]
#Driver=/infa/Informatica/ODBC6.1/lib/DWsqls25.so

def getDSConnStr(t,u,p,dsn,db=None):
    if  ST.upper(t) == 'ORADB'  and u is not None and p is not None: 
        return _getOracleOCIStr(u,p,dsn)
    if  ST.upper(t) == 'SQLLITE' and dsn is not None                : 
        return _getSQLITEStr(dsn)
    if  ST.upper(t) == 'NZODBC'  and u is not None and p is not None and dsn and db is not None: 
        return _getNetODBCStr(u,p,dsn,db)
    if  ST.upper(t) == 'MSSQLODBC'  and u is not None and p is not None and dsn and db is not None: 
        return _getMSSQLODBCStr(u,p,dsn,db)   
    if  ST.upper(t) == 'MSSQLNAT'  and u is not None and p is not None and dsn and db is not None: 
        return _getMSSQLNatStr(u,p,dsn,db)   
    if  ST.upper(t) == 'DB2CLI' and u is not None and p is not None and dsn and db is not None:
        return _getDB2Cli(u,p,dsn,db)
    return None

def _getOracleOCIStr(u,p,dsn)    : return '%s/%s@%s' % (u,p,dsn)
def _getSQLITEStr(dsn)           : return dsn
def _getNetODBCStr(u,p,srv,db)   : return "DRIVER={NetezzaSQL};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (srv,db,u,p)
def _getMSSQLODBCStr(u,p,srv,db) : return "DRIVER={SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (srv,db,u,p)
def _getMSSQLNatStr(u,p,srv,db)  : return "DRIVER={SQL Server Native Client 11.0};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (srv,db,u,p)
def _getDB2Cli(u,p,dsn,db)       : return "DSN=%s;DATABASE=%s;UID=%s;PWD=%s" % (dsn,db,u,p)
#===============================================================================
# Enumeration
#===============================================================================
# This class does not trim for blanks (performance) , make sure the names parameter
# is properly trim !
class Enumerate(object):
    def __init__(self, names, sep):
        for number, name in enumerate(names.split(sep)):
            setattr(self, name, number)

def getIDX(idxStr, sep = ','):
    en = Enumerate(idxStr,sep)
    return (en)

#===============================================================================
# SQLLITE Driver 
#===============================================================================
import sqlite3  
class DBSQLLite(object): 

    QRY = 0     # Query objects
    IDX = 1     # Column idx objects
   
    def __init__(self, cs, logger,encode=str):
        self.connStr  = cs
        self.dbConn   = None
        self.logger   = logger
        self.encode   = encode
        self.sep      = ','
    
    def connToDB(self):
        retVal = 0
        try:
            #self.dbConn = sqlite3.connect(self.connStr,isolation_level=None)
            self.dbConn = sqlite3.connect(self.connStr)
            self.dbConn.text_factory = self.encode
            self.logger.debug("Connecting to %s" % self.connStr)
            
        except:

            self.logger.error("==EXCEP Connect String << %s >>"   % self.connStr )
            self.logger.error("==EXCEP %s %s "  % (sys.exc_type, sys.exc_info()[1]))
            retVal = mm.DB_CON_ERR
        
        finally:  return retVal

    # Use this query for select options
    # s     : list of BINDING VARIABLES, that need to be passed to the SQL Engine
#    def runQry(self,qryStr,s=[]):
#
#        if(type(s) != list) : 
#            s = [s,]
#
#        resLst =[]
#        if (self.dbConn == None):
#            self.logger.error("self.dbConn has not been set")
#            return resLst
#       
#        try:   
#            cursor = self.dbConn.cursor()      
#            res = cursor.execute(qryStr,s)
#            for row in res:
#                   resLst.append(row)
#        except sqlite3.OperationalError, msg:
#        #except:
#            print("==>> EO OPEr EXCEP IN RUNQRY %s " % msg)
#            raise sqlite3.OperationalError, msg
#            
#        
#        finally:return resLst
#        

    # Use this query for select options
    # s     : list of BINDING VARIABLES, that need to be passed to the SQL Engine
    def runQry(self,qryStr,s=[]):
        print "qryStr + %s " % qryStr  
        if(type(s) != list) : 
            s = [s,]

        resLst = None
        if (self.dbConn == None):
            self.logger.error("self.dbConn has not been set")
            return resLst
       
        try:   
            tmp = []
            cursor = self.dbConn.cursor()      
            res = cursor.execute(qryStr,s)
            for row in res:
                tmp.append(row)
                          
        except sqlite3.OperationalError, msg:
            self.logger.error("==>> sqlite3.OperationalError : %s " % msg)
            #raise sqlite3.OperationalError, msg
                
        else    : resLst = tmp
        finally : return resLst

#   DEBUG ONLY            
#    def runQry(self,qryStr,s=[]):
#
#        if(type(s) != list) : 
#            s = [s,]
#
#        resLst =[]
#        if (self.dbConn == None):
#            self.logger.error("self.dbConn has not been set")
#            return resLst
#        cursor = self.dbConn.cursor()      
#        res = cursor.execute(qryStr,s)
#        for row in res:
#            resLst.append(row)
#            
#        return resLst


    # Use this method for DML insert, update, delete
    # qryStr: SQL command to execute
    # s     : list of BINDING VARIABLES, that need to be passed to the SQL Engine

    def exeQry(self,qryStr,s = []) :
        #print "qryStr + %s " % qryStr                #EO Remove after test
        rc = -1

        if(type(s) != list or type(s) != tuple) : 
            s = list(s)

        if (self.dbConn == None):
            self.logger.error("self.dbConn has not been set")
            return mm.DB_CON_HDL_ERR
        
        try:    
            cursor = self.dbConn.cursor()                
            cursor.execute(qryStr,s)
            rc = cursor.rowcount
            self.dbConn.commit()
            self.logger.debug("rc is %s " % rc) 
            return rc
        
        # IntegrityError columns fac_name, addr1 are not unique
        # ProgrammingError: Incorrect number of bindings supplied
        except sqlite3.OperationalError, msg:  
            #self.dbConn.rollback()
            #raise sqlite3.OperationalError, msg
            self.logger.error("==>> sqlite3.OperationalError : %s " % msg)
            
        except sqlite3.IntegrityError, msg:  
            #self.dbConn.rollback()              # EO might help in DB lock issue.
            #raise sqlite3.IntegrityError, msg
            self.logger.error("==>> sqlite3.IntegrityError : %s " % msg)
                   
        except:
            self.logger.error("Gral Excp  %s %s\n" % (sys.exc_type,sys.exc_info()[1]))
                  
        finally:  
            self.dbConn.rollback()         # WIll get in here only 
            return rc
                 
##   DEBUG ONLY     
#    def exeQry(self,qryStr,s = []) :
#            print "\n\n==============Qry = %s " % qryStr
#            rc = -1
#            cursor = self.dbConn.cursor()                
#            cursor.execute(qryStr,s)
#            rc = cursor.rowcount
#            self.dbConn.commit()
#            self.logger.debug("exeQry: rc is %s " % rc) 
#            return rc

    def getIDX(self,idxStr):
        self.logger.debug("%s " % idxStr)
        en = Enumerate(idxStr, self.sep)
        return (en)
    
    def closeDBConn(self):
        if(self.dbConn != None):
            self.dbConn.commit()
            self.logger.debug( "Closing Conn %s " % self.connStr)
            self.dbConn.close()
            
#===============================================================================
# CX_ORACLE Driver 
#===============================================================================
import cx_Oracle   
class DBOracle(object):  
    
    def __init__(self, cs, logger, encode= str):
        self.connStr  = cs
        self.dbConn   = None
        self.logger   = logger
  
##DEBUG ONLY    
#    def connToDB(self):
#        retVal = -1
#
#        self.dbConn = cx_Oracle.connect(self.connStr)
#        print("connToDB:Connecting to %s" % self.connStr)
#         
#        return 0 
#    
#    def runQry(self,qryStr,s=[]):
#        resLst=[]
#        if (self.dbConn == None):
#            return mm.DB_NO_HANDLER
#        
#        cursor = self.dbConn.cursor()
#        res=cursor.execute(qryStr,s)
#        for row in res:
#                   #print row
#            resLst.append(row)
#        return resLst    
#    
# For oracle val is a dictionary
#    def exeQry(self,qryStr,val) :
#            print "\n\n==============Qry = %s \n\nLST=%s " % (qryStr,val)
#            
#            cursor = self.dbConn.cursor()                
#            cursor.execute(qryStr,val)
#            rc = cursor.rowcount
#            self.dbConn.commit()
#            cursor.close()
#            self.logger.debug("exeQry: count is %s rc is %s " % (111,rc)) 
#            return rc
#
#    def closeDBConn(self):
#        if(self.dbConn != None):
#            print( "closeDBConn:Closing Conn %s " % self.connStr)
#            self.dbConn.close()
#            self.dbConn = None

#END DEBUG ONLY   

    def connToDB(self):
        retVal = -1
        try:
            self.dbConn = cx_Oracle.connect(self.connStr)
            self.logger.debug("connToDB:Connecting to %s" % self.connStr)
         
        except cx_Oracle.DatabaseError, exc:
            error, = exc.args
            
            self.logger.debug("connToDB:==EXCEP Connect String << %s >>"   % self.connStr )
            self.logger.debug("connToDB:==EXCEP Error-Code:%s"             % error.message )
            self.logger.debug("connToDB:==EXCEP Error-Code:%s"             % error.code )
            retVal = mm.DB_CON_ERR
        
        else   : retVal = 0
                
        finally: return retVal
        
    def runQry(self,qryStr,s=[]):
        resLst = None
        if (self.dbConn == None):
            return resLst
        
        cursor = self.dbConn.cursor()
        try:   
            tmp = [] 
            res=cursor.execute(qryStr,s)
            for row in res:
                tmp.append(row)    
                    
        except cx_Oracle.DatabaseError, exc:
            error, = exc.args
            self.logger.error("Oracle-Error-Code:%s"      % error.code)
            self.logger.error("Oracle-Error-Message:%s"   % error.message)
        
        else : resLst = tmp
            
        finally: return resLst
     
    #For oracle val is a dictionary
    
    def exeQry(self,qryStr,val) :
        self.logger.debug( "qryStr = %s val=%s " % (qryStr,val))
        retVal = -1
        try:    
            cursor = self.dbConn.cursor()                
            cursor.execute(qryStr,val)
            retVal = cursor.rowcount
            self.logger.debug("rc = %s for qry %s val = %s " % (retVal,qryStr,val))
            self.dbConn.commit()
            cursor.close()
            
        except cx_Oracle.IntegrityError, exc:
            error, = exc.args
            self.logger.error("Oracle-Error-Code:%s"      % error.code)
            self.logger.error("Oracle-Error-Message:%s"   % error.message)
            retVal = mm.DB_CONSTR_ERR
              
        except cx_Oracle.DatabaseError, exc:
            error, = exc.args      
            self.logger.error("Oracle-Error-Code:%s"      % error.code)
            self.logger.error("Oracle-Error-Message:%s"   % error.message)
            retVal = mm.DB_GRAL_ERR  
            
        finally: return retVal 

    def closeDBConn(self):
        if(self.dbConn != None):
            #self.logger.debug("Closing Conn %s " % self.connStr)
            #self.logger.debug( "closeDBConn:Closing Conn %s " % self.connStr)
            self.dbConn.close()
            self.dbConn = None         


#===============================================================================
# PYODBC Driver 
#===============================================================================
#except pyodbc.Error:
#     print 'Error exc'
#except pyodbc.DataError:
#     print 'Data Error'
#except pyodbc.DatabaseError:
#     print 'Database error'
#..... catch rest of the errors......
#except:
#     print 'Why did I get here!!!!! %s' % sys.exc_info()
# Do not Instantiate directly use a subclass for the engine !

import pyodbc
class _DBPYODBC(object):  
    
    def __init__(self, cs, logger):
        self.connStr  = cs
        self.dbConn   = None
        self.logger   = logger
  
    def connToDB(self):
        retVal = -1
        try:           
            self.dbConn = pyodbc.connect(self.connStr)
            self.logger.debug("connToDB:Connecting to %s" % self.connStr)
                   
        except pyodbc.Error, exc:
                    
            err,errMsg = exc.args
            self.logger.error("err %s " % err)
            self.logger.error("errMsg= %s " % errMsg)

        except:
            self.logger.error(" EXCEP -- %s %s\n" % (sys.exc_type,sys.exc_info()[1]))

        else    : retVal = 0
        finally: return retVal 
    
    def runQryd(self,qryStr):
            resLst=[]
            cursor = self.dbConn.cursor()
            print "QRYSTR = %s " %  qryStr       
            c=cursor.execute(qryStr)
            res = c.fetchall()
            for row in res:
                print row
                resLst.append(row)
            return resLst   
           
    def runQry(self,qryStr):
        resLst = None
        if (self.dbConn == None):
            return None
        
        try:
            tmp = []
            cursor = self.dbConn.cursor()
            self.logger.debug("QRYSTR = %s " %  qryStr)       
            c=cursor.execute(qryStr)
            res = c.fetchall()
            for row in res:
                tmp.append(row)              
        
        except pyodbc.DataError,exc:
            err,errMsg = exc.args
            self.logger.error("err %s  " % err)
            self.logger.error("errMsg= %s  " % errMsg)  
            
        except  pyodbc.ProgrammingError,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)     
        
        except  pyodbc.Error,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)   
        
        except:
            self.logger.error("GRAL EXCEPT : %s %s\n" % (sys.exc_type,sys.exc_info()[1]))
            
        else  : resLst = tmp 
    
        finally: return  resLst      
    
    def exeQry(self,qryStr,val) :
        #print "\n\n==============Qry = %s \n\nLST=%s " % (qryStr,val)
        rc = -1
        if (self.dbConn == None):
            return rc
        try:    
            cursor = self.dbConn.cursor()                
            cursor.execute(qryStr,val)
            rc = cursor.rowcount
            self.dbConn.commit()
            cursor.close()
            self.logger.debug("exeQry: count rc is %s " % (rc))            
        
        except pyodbc.DataError,exc:
            err,errMsg = exc.args
            self.logger.error("err %s  " % err)
            self.logger.error("errMsg= %s  " % errMsg)  
            
        except  pyodbc.ProgrammingError,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)     
        
        except  pyodbc.Error,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)   
        
        except:
            self.logger.error("GRAL EXCEPT : %s %s\n" % (sys.exc_type,sys.exc_info()[1]))
    
        finally: return  rc
    
    # rc = cursor.rowcount returns -1 even for succesful insertions. 
    def exeManyQry(self,qryStr,val) :
        rc = -1
        if (self.dbConn == None):
            return rc
        try:    
            cursor = self.dbConn.cursor()                
            cursor.executemany(qryStr,val)
            rc = cursor.rowcount
            self.dbConn.commit()
            cursor.close()
            self.logger.debug("exeManyQry: cursor.rowcount is %s " % (rc)) 
            rc = 0   # At this point no errors exceptions. Need to test with conatrints !
        
        except pyodbc.DataError,exc:
            err,errMsg = exc.args
            self.logger.error("err %s  " % err)
            self.logger.error("errMsg= %s  " % errMsg)  
            
        except  pyodbc.ProgrammingError,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)     
        
        except  pyodbc.Error,exc:
            err,errMsg = exc.args
            self.logger.error ("err %s  " % err)
            self.logger.error("errMsg= %s " % errMsg)   
        
        except:
            self.logger.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
    
        finally: return  rc

                   
    def closeDBConn(self):
        if(self.dbConn != None):
            self.logger.debug( "closeDBConn:Closing Conn %s " % self.connStr)
            self.dbConn.close()
            self.dbConn = None

class NZODBC (_DBPYODBC): 
    def __init__(self,cs,log):
        #_DBPYODBC.__init__(self, cs, log)
        super(NZODBC,self).__init__(cs, log)
        
        
class MSSQLODBC (_DBPYODBC): 
    def __init__(self,cs,log):
        #_DBPYODBC.__init__(self, cs, log)    
        super(MSSQLODBC,self).__init__(cs, log)
        
        
class MSSQLNat (_DBPYODBC): 
    def __init__(self,cs,log):
        #_DBPYODBC.__init__(self, cs, log)    
        super(MSSQLNat,self).__init__(cs, log)
        
        
class DB2Cli (_DBPYODBC): 
    def __init__(self,cs,log):
        #_DBPYODBC.__init__(self, cs, log)    
        super(DB2Cli,self).__init__(cs, log)
