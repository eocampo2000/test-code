'''
Created on Sep 8, 2015

@author: eocampo

General Function to query DB's

'''
__version__ = '20150908'


import os      #, os.path
import sys
import socket
import time


import common.log4py    as log4py 
import common.simpmail  as sm

import utils.fileutils  as fu
import utils.strutils   as su
import datastore.dbapp  as ds
import mainglob         as mg    # Do not remove.

from datastore.dbutil  import NZODBC, MSSQLODBC,MSSQLNat, DBOracle, getDSConnStr

from common.loghdl     import getLogHandler

class _QueryBaseDB(object):
    '''
    classdocs
    '''
    # Set of diagnostics commands to run
    hostname = socket.gethostname()
    exitOnError = False   # default.
    
    def __init__(self):
        self.appName = self.__class__.__name__.lower()
        self.log = log4py.Logger().get_instance()       
        self.infaEnvVar   = {}                       # Empty for base class

        # Db handler
        self.dbh = None   
        
        self.infaEnvVar   = {
                'DB_USER'        : 'self.ib.user'      ,
                'DB_PWD'         : 'self.ib.pwd'       ,
                'DB_SERVER'      : 'self.ib.dbserver'  ,
                'DB_NAME'        : 'self.ib.db'        ,
                'DB_ENG '        : 'self.ib.dbeng'     ,             
               }  
   
    # Sets self.dbh 
    # dbEng : Database Engine connect Strings
    # CAUTION : Cannot have 2 open connections. Use this method for 1 open connection otherwise override in child class.
    # Valid DB Engines:
    #      'ORADB'     
    #      'SQLLITE'   
    #      'NZODBC'    
    #      'MSSQLODBC' 
    #      'MSSQLNAT'  
    
    def _connToDB(self):
        cs = getDSConnStr(self.ib.dbeng, self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        self.dbh = NZODBC(cs, self.log)
        rc = self.dbh.connToDB () 
        if rc != 0 : 
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return 1
        return rc
    
    
        # Disconnects from DB 
    def _disFromDB(self): self.dbh.closeDBConn()
    
    #  This method is used for DB Connections. 
    #  sql statement to execute.
    #  bv bind variables
    #  po sql operation. SEL otherwise INS, UPD, DEL
    #  This method returns 
    #     SUCCESS : a list (runQry) or a positive number (exeQry)
    #     ERROR   : None . 
    def _getNZDS(self, sql, bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('NZODBC', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = NZODBC(cs, self.log)
        rc  = dbh.connToDB () 
        if rc != 0 : 
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
        
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)

        dbh.closeDBConn()
        return r
     
    def _getMSSQLDS(self, sql,bv=[], po='SEL'): 
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('MSSQLODBC', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = MSSQLODBC(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
    def _getMSSQLNatDS(self, sql,bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('MSSQLNAT', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = MSSQLNat(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
    def _getOracleDS(self, sql,bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('ORADB', self.ib.user, self.ib.pwd, self.ib.db)
        dbh = DBOracle(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
        # Environment Variables that need to be set. Key is the ENV and ELE the name of var.
    # Below are global variables. Env variables should be set based on env settings.    
    def _getEnvVars(self):
        ret = 0   
        for ev, v in  self.infaEnvVar.items():
            self.log.debug("ev =%s v = %s" % (ev, v))
            
        try:     
            for ev, v in  self.infaEnvVar.items():
                x = os.environ[ev]
                exec  "%s='%s'" % (v, x) 
                self.log.debug("%s='%s'" % (v, x))  
                     
        except:
                ret = 2
                self.log.error("ENV var not set %s %s\n" % (sys.exc_type, sys.exc_info()[1]))
    
        finally : return ret
    
    # If arguments define  in child class.
    def setArgs(self,Argv): return 0 
        
    def main(self, Argv):
        rc = 1  # Failed
        logFile = getLogHandler(self.appName, self.log)
        self.log.info('logFile= %s' % logFile)
        
        # should NEVER get this programmatic error !!!!
        rc = self.setArgs(Argv)    
        if rc != 0 : return rc
        
        rc = self._getEnvVars()
        if rc != 0 :
            self.log.error("Need to set all env vars:\n %s" % self.infaEnvVar.keys())
            return rc
        
        sys.exit(rc)
    
    def __del___(self):
        self.log.debug('Base class cleaning')         

