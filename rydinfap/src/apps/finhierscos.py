'''
Created on Sep 10, 2013

@author: eocampo

This code waits 

'''

__version__ = '20130910'

import sys
import os
from apps.infbaseapp import _InfaBaseApp
import utils.fileutils as fu
import utils.strutils  as su
import procdata.procinfa as pi 
import utils.filetransf  as ft

# mandatory to define self.cmdStep


class FinHierSCOS(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(FinHierSCOS,self).__init__()
        self.landDir    = 'SrcFiles/finance'
        self.outDir     = 'TgtFiles/finance'
        self.ib.outFile = 'SCS_Org_Hierarchy.csv'   
        self.outFile    = ''      # Place Holder for final File Name SCS_Org_Hierarchy130910.csv
        self.outArchDir = 'archive'
        self.trigFiles  = []      # Incoming Trigger File.
        
        self.ts         =  su.getTimeSTamp()
       
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.getTrigFilesDate    ,
                         'C' : self.verTrigFileDate     ,
                         'D' : self.mvTrigFileToArchDir ,   # Move files after dates within are verified.
                         'E' : self.wfinHierSCOS         ,
                         'F' : self.procHierSCOS         ,
                         'G' : self.putSCOSftpFile      ,               
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = {
                'PMCMD'            : 'mg.pmcmd'           ,
                'INFA_USER'        : 'self.ib.rep_user'   ,
                'INFA_XPWD'        : 'self.ib.rep_xpwd'   ,
                'DOMAIN'           : 'self.ib.dom_name'   ,
                'INT_SERV'         : 'self.ib.IS'         , 
                'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
               }
         
    def wfinHierSCOS(self):
        self.ib.fld = 'Financial'
        self.ib.wkf = 'wkf_org_hierarchy_scs_tmw'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    
    # This method renames the created file, places it  in the output dir for transmission and archives it. 
    # SE Sets self.outFile
    def procHierSCOS(self):
        # Check for file existence
        src = '%s/%s/%s' % (self.ib.shareDir,self.outDir,self.ib.outFile)
        
        r = fu.fileExists(src)
        if r == False : 
            self.log.error('File %s does not exist' % src)
            return 1
        
        # Move File to output dir
        dt  = su.getTodayDtStr('%y%m%d')
        self.outFile  = 'SCS_Org_Hierarchy%s.csv' % dt
        outFile = '%s/%s/%s/%s' % (self.ib.shareDir,self.outDir,self.ib.outDirname,self.outFile)
        r = fu.moveFile(src,outFile)
        if r != 0 : 
            self.log.error("Could not move %s to %s " % (src,outFile))
        self.log.debug('Moved  src = %s to self.outFile = %s RC = %s' % (src,outFile,r))
        
        # Archive File .
        arcfn = '%s/%s/%s/%s-%s' % (self.ib.shareDir,self.outDir,self.outArchDir,self.ts,self.outFile)
        r     = fu.copyFile(outFile,arcfn)
        if r != 0 : 
            self.log.error("Could not copy %s to %s " % (outFile,arcfn))
        
        self.log.debug('Copied src = %s to racfn = %s RC = %s' % (outFile,arcfn,r))
        return 0
        
    
    # ftp a file to a remote machine. (self.outFile)
    def putSCOSftpFile(self):
        # FTP is expecting the following env variables, which should not be in a config file.
        os.environ['FILE'     ] =  self.outFile
        os.environ['RXFILE'   ] =  'SCS_Org_HierarchyYYMMDD.csv'   
       
        ftp = ft.Ftp("FinHierSCOS",self.log)
        rc  = ftp.connect() 
        self.log.info("Connect rc = %s" % rc)
        if rc != 0 : return rc
        rc  = ftp.login() 
        self.log.info("Login  rc = %s" % rc)
        if rc != 0 : return rc
        ftp.ftpSetPasv(False)
        self.log.info("Setting to Active Mode")    
        rc=ftp.ftpServCwd()       
        self.log.info('Trying to Change Remote Dir')
        if rc != 0 : return rc    
        rc = ftp.ftpPut()
        self.log.info("Put File  rc = %s " % rc)
        if rc != 0 : return rc
        ftp.disconnect() 
        self.log.info("Disconnecting = %s" % rc)
        return rc
     
def main(Args):
        a = FinHierSCOS()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
