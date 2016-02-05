'''
Created on Jul 17, 2015

@author: eocampo
'''

__version__ = '20150717'

import sys
import os
#import time
from datetime import datetime

import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
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

class EmpBenefitonDem(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(EmpBenefitonDem,self).__init__()
        self.landDir    = 'SrcFiles/employee'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile     = ('hp400jnm_sap_dwext.dat','hrcoord5.csv')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.HP400JNM.SAP.DW.EXTRACT',hrcoord5.csv"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.getFtpFiles         ,  # MF edwetl
                         'C' : self.getFtpFiles2        ,  # MF INFAHR
                         'D' : self.getIncSetFiles      ,
                         'E' : self.copyFilesWorkDir    ,
                         'F' : self.archFilesTS         ,
                         'G' : self.procEmpBenefit      , 
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
                 
    def getFtpFiles(self):
        os.environ['FILE'     ] =  (r"\'P.HP400JNM.SAP.DW.EXTRACT\'")   
        
        return ft.getn('Employee',self.log)
    
    def getFtpFiles2(self):
        os.environ['FILE'     ] =  ('hrcoord5.csv')
        envVars = {'REMOTE_HOST':'REMOTE_HOST1',    
                   'USER'       :'USER1',    
                   'PWD'        :'PWD1',        
                   'REMOTE_DIR' :'REMOTE_DIR1',    
                   'FTP_MODE'   :'FTP_MODE1',   
                  }
        
        rc = self.setEnvVars(envVars)    
        if rc != 0 : return rc
        
        return ft.get('Employee2',self.log)    

    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
       
    def _wkfEmpBenefitonDem(self):
        self.ib.fld = 'Employee'
        self.ib.wkf = 'wkf_employee_benefits_on_demand' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
 
    def procEmpBenefit(self):
        # Run workflows
        if self._wkfEmpBenefitonDem()      != 0 : return 1
     
        for i in range (len(self.srcFile)):   
            t   = '%s/%s' % (self.ib.workDir,self.srcFile[i]) 
            r = fu.delFile(t) 
            self.log.info('Removing %s rc = %s ' % (t,r))     
        return 0
            
def main(Args):
    a = EmpBenefitonDem()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
