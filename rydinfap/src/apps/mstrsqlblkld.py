'''
Created on Sep 18, 2012

@author: eocampo

This application will be used for BIGDATA Movement
New Style class 
'''
__version__ = '20120923'

import sys

import utils.fileutils   as fu
import utils.filetransf  as ft
import procdata.procinfa as pi 

from apps.infbaseapp import _InfaBaseApp, hostname

# mandatory to define self.cmdStep

class MstrSQLBulkLd(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(MstrSQLBulkLd,self).__init__()
        self.landDir    = 'SrcFiles/invoice'
        
        self.rc       = 1     # Need for notify user
        self.files    = []

        self.notifUsersFlg = False

        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock        ,
                         'N' : self.notifyUsersFlg ,  # Set instance Flags first
                         'B' : self.getTrigFiles   ,  # ChkTriggerFiles
                         'C' : self.wflMstrSQLBulkLd,
                         'D' : self.chkErrFile     ,  
                         'E' : self.putFiles       ,                        
                         'F' : self.remTrigFiles   ,                   
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = { 'PMCMD'            : 'mg.pmcmd'           ,
                              'INFA_USER'        : 'self.ib.rep_user'   ,
                              'INFA_PWD'         : 'self.ib.rep_pwd'    ,
                              'DOMAIN'           : 'self.ib.dom_name'   ,
                              'INT_SERV'         : 'self.ib.IS'         , 
                              'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                              'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                              'INFA_APP_LCK'     : 'self.ib.lckDir'     ,
                              'INFA_TGT'         : 'self.ib.tgtDir'     ,
                            }

    def _setDataDir(self) : 
        self.ib.landDir = '%s/%s'    % (self.ib.shareDir,self.landDir )
        self.ib.archDir = '%s/%s/%s' % (self.ib.shareDir,self.landDir, self.ib.archDirName)
        
    def wflMstrSQLBulkLd(self):
        self.ib.fld     = 'Invoice'
        self.ib.wkf     = 'wkf_mstr_sql_bulk_load'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def chkErrFile(self):
        rc = 1
        fnp = '%s/%s' % (self.ib.tgtDir,self.ib.errorFileName)
        self.log.info('Checking for file  %s' % (fnp))

        if fu.fileExists(fnp):
            fileSz = fu.getFilseSize(fnp)
            if fileSz == 0 : return 0
        else:
            self.log.error('filename %s does not exist!' % (fnp))
            if self.notifUsersFlg is True:  self._notifyUsers()

        # At this point file exist and size > 0. Let's Check for blank lines!
        
        rc = fu.remBlankLines(fnp)
        self.log.info('Found File %s size = %d(bytes). Checked for non blank lines = %d.' % (fnp,fileSz,rc))
        if rc != 0 :
           self.log.error ('Error File created : filename %s. Lines = %d ' % (fnp,rc))
           if self.notifUsersFlg is True:  
               r = self._notifyUsers(rc)
               self.log.info ('Notifying Users rc= %s' % (r) )
              
        return rc
    
    # This method will FTP the files to target server for Bulk Load.
    def putFiles(self):
        rc= ft.put(self.appName,self.log)
        return rc  
    
    def notifyUsersFlg(self):
        self.notifUsersFlg = True
        self.log.debug('Setting Notify users FLag = TRUE')
        return 0
        
    def _notifyUsers(self,fileSz=1):

        try: 
            m = -1 ; p = -1
            m = eval(self.ib.mailOnErr) ; p = eval(self.ib.pgOnErr)
        except:
            #('Error with configuration value(s) self.ib.mailOnErr = %s self.ib.pgOnErr = %s' % (m,p) )
            self.log.error ('Error with configuration value(s) self.ib.mailOnErr = %s self.ib.pgOnErr = %s' % (m,p) )
        
        finally:  
            self.ib.mailOnErr = m
            self.ib.pgOnErr   = p
          
        if fileSz > 0 :
            self.subj = 'SQL query error (%s)' % hostname 
            self.text = 'SQL query error. Please see %s/%s ' % (self.ib.tgtDir,self.ib.errorFileName)
                   
        else:
            self.subj = 'Error Creating File (%s) !'    % hostname 
            self.text = 'Error Creating File %s o%s/%s ' % (self.ib.tgtDir,self.ib.errorFileName)

        rc = self._notifyAppUsers()
        
        self.log.debug('Notify rc = %s SUBJ %s \t MSG %s' % (rc,self.subj, self.text))    
        
        return 1

def main(Args):
    a = MstrSQLBulkLd()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                      # Remove in UX 
    rc=  main(sys.argv)
    print "RC IS", rc
