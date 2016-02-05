'''
Created on Mar 21, 2012

@author: eocampo

Use this module for DB Commands. 

20150825 Added support for MS SQL
'''

__version__ = '20150825'

import inspect

import proc.process   as p
import mainglob  as mg
from datastore.dbutil  import NZODBC, DBOracle, MSSQLNat, getDSConnStr
from datetime import datetime
import utils.strutils as su

# Note classes with ODBC string are runign using ODBC drivers. Otherwise will use native !

# This class will run DB Commands. Do not instantiate directly
class _DBCmd:
    
    # ib infaBean    
    def __init__(self, ib, log):
        self.ib     = ib
        self.logger = log
    
    def pingDB(self):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],self.ib.dbType))
        return 100
       
    def connToDB(self):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],self.ib.dbType))
        return 100

    # Main execution driver
#    def _execCmd(self,c):
#        cmd = '%s %s' % (mg.pmcmd, c)
#        self.logger.debug("cmd = %s" % cmd)
#        return p.runSync(cmd,self.logger)

class ORADBCmd(_DBCmd) :
    def __init__(self, ib, log):
        _DBCmd.__init__(self, ib, log)
        
    def pingDB(self) :
        rc = 1    
        cmd = 'tnsping %s ' % self.ib.db_name 
        rc, msg = p.runSync(cmd, self.logger)
        if rc == 0 :
            rc = su.findStr(msg,'OK ')
        return rc
    
    def connToDB(self):
        
        r = 1
        sql = 'select SYSDATE from dual'
        cs = getDSConnStr('ORADB',self.ib.db_user,self.ib.db_pwd,self.ib.db_name)
        self.logger.debug('qry = %s\nCS =%s ' % (sql,cs))
        dbh = DBOracle(cs,self.logger)
        if dbh.connToDB() != 0 : return r
        
        rs = dbh.runQry(sql)
        if rs is not None and type(rs) == list :  
            r = 0
            self.logger.debug("rs = %s" % rs[0][0])
        
        dbh.closeDBConn()
        return r
    
    
    
class MSSQLDBCmd(_DBCmd) :
    def __init__(self, ib, log):
        _DBCmd.__init__(self, ib, log)
        
     
    def connToDB(self):
        r = 1
        sql = 'SELECT GETDATE() AS [CurrentDate]'
        self.logger.debug('qry = %s' % sql)
        cs = getDSConnStr('MSSQLNAT',self.ib.db_user,self.ib.db_pwd,self.ib.db_server,self.ib.db_name)
        self.logger.debug('ConnStr %s' % cs)
        dbh = MSSQLNat(cs,self.logger)
        if dbh.connToDB() != 0 : return r
        
        rs = dbh.runQry(sql)
        if rs is not None and len(rs) > 0 :  
            r = 0
            self.logger.debug("rs = %s" % rs[0][0])
        
        dbh.closeDBConn()
        return r   
        
class NZODDBCmd(_DBCmd):
    
    def __init__(self, ib, log):
        _DBCmd.__init__(self, ib, log)
    
    # Note if ib atributes not present will throw an exception, that inovker needs to catch !
    def connToDB(self):
        r = 1
        sql = 'select current_timestamp'
        self.logger.debug('qry = %s' % sql)
        cs = getDSConnStr('NZODBC',self.ib.db_user,self.ib.db_pwd,self.ib.db_server,self.ib.db_name)
        self.logger.debug('ConnStr %s' % cs)
        dbh = NZODBC(cs,self.logger)
        if dbh.connToDB() != 0 : return r
        
        rs = dbh.runQry(sql)
        if rs is not None and len(rs) > 0 :  
            r = 0
            self.logger.debug("rs = %s" % rs[0][0])
        
        dbh.closeDBConn()
        return r

# Note each script should focus on one DB Type only !
def getDBClass():
    pass