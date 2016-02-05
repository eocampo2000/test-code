'''
Created on Jan 13, 2015

@author: eocampo
'''
__version__ = '20150129'

import sys
import os
#import time
from datetime import datetime
import procjobs.procsched as psc
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
RUN_PER_DAY = 1  # Daily runs.
DP_LEN      = len('YYYYMM')  
   
# Schedules
SCH_FREQ = 'Mthly'
sch = ()
cur_dayr   = su.getTodayDtStr('%Y%m')

class EmpBenefitMthly(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(EmpBenefitMthly,self).__init__()
        self.landDir    = 'SrcFiles/employee'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile     = ('hp400jnm_sap.dat','hr503ril.dat','hrcoord5.csv')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.HP400JNM.SAP.DW.EXTRACT','P.HR503RIL',hrcoord5.csv"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.getTrigFiles        ,
                         'C' : self.mvTrigFileToArchDir ,
                         'D' : self.getFtpFiles         ,  # MF edwetl
                         'E' : self.getFtpFiles2        ,  # MF INFAHR
                         'F' : self.getFtpFiles3        ,  # sscbpbiamsql05
                         'G' : self.getIncSetFiles      ,
                         'H' : self.copyFilesWorkDir    ,
                         'I' : self.archFilesTS         ,
                         'J' : self.chkNextRunFlg       ,
                         'K' : self.procEmpBenefit      , 
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
                'INFA_APP_CTL'     : 'self.ib.ctlDir'     ,          
               }
        
        # FTP is expecting the following env variables, which should not be in a config file.
        # First Set of files from Mainframe. 
        # 
        os.environ['RXFILE'   ] =  ('None')    # 
        # Sets a flag, to check for next run.
    
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0   
             
    def getFtpFiles(self):
        os.environ['FILE'     ] =  (r"\'P.HP400JNM.SAP.DW.EXTRACT\'")   
        
        return ft.getn('Employee',self.log)
    
    def getFtpFiles2(self):
        os.environ['FILE'     ] =  (r"\'P.HR503RIL\'")
        envVars = {'REMOTE_HOST':'REMOTE_HOST1',    
                   'USER'       :'USER1',    
                   'PWD'        :'PWD1',        
                   'REMOTE_DIR' :'REMOTE_DIR1',    
                   'FTP_MODE'   :'FTP_MODE1',   
                  }
        
        rc = self.setEnvVars(envVars)    
        if rc != 0 : return rc
        
        return ft.getn('Employee2',self.log)    


    def getFtpFiles3(self):
        os.environ['FILE'     ] =  ('hrcoord5.csv')
        envVars = {'REMOTE_HOST':'REMOTE_HOST2',    
                   'USER'       :'USER2',    
                   'PWD'        :'PWD2',        
                   'REMOTE_DIR' :'REMOTE_DIR2',    
                   'FTP_MODE'   :'FTP_MODE2',   
                  }
        
        rc = self.setEnvVars(envVars)    
        if rc != 0 : return rc
        
        return ft.get('Employee3',self.log)    

    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
       
    def _wkfEmpBenefitMthly(self):
        self.ib.fld = 'Employee'
        self.ib.wkf = 'wkf_employee_benefits_monthly' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

      
    def procEmpBenefit(self):
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)                
        self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
        prev_dayr = self._getCtlFile()
        
        if self.checkNextRunFlg is True:
            
            if prev_dayr is None or prev_dayr.strip() == '': 
                self.log.error("Could not find control file or No Data")
                return -1
            
            rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log,sch)
            if rc != 0 : 
                self.log.error("self._chkNextRun rc = %s" % rc)
                return rc
 
        # Run workflows
        if self._wkfEmpBenefitMthly()      != 0 : return 1
     
        # Loading Staging Succeeded. Update the control file.
        rc = fu.updFile(ctlFile,cur_dayr)               
        if rc == 0 :
            if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s'     % (prev_dayr,cur_dayr, ctlFile))
            else                   : self.log.info('Overwriting Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr, ctlFile))
        else       : 
            self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
            return rc 
        
        for i in range (len(self.srcFile)):   
            t   = '%s/%s' % (self.ib.workDir,self.srcFile[i]) 
            r = fu.delFile(t) 
            self.log.info('Removing %s rc = %s ' % (t,r))     
        return rc
            
def main(Args):
    a = EmpBenefitMthly()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
