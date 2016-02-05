'''
Created on Jan 8, 2014

@author: eocampo

Main driver for SAP Employee.
20151119 Disable Trigger File creation.
'''

__version__ = '20151119'

import sys
import os
#import time
from datetime import datetime
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import procdata.procinfa as pi 
import procjobs.procsched as psc

from apps.infbaseapp       import _InfaBaseApp
from infbaseutil      import _SAPEmployeeFile

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

#RUN_PER_DAY = 1  # Daily runs.
#DP_LEN      = len('YYYYMMDD')  
FLD_SEP  = '~'
SCH_FREQ = 'Dly'

class SAPEmployee(_InfaBaseApp,_SAPEmployeeFile):  
    exitOnError = True
    
    def __init__(self):
        super(SAPEmployee,self).__init__()
        self.landDir    = 'SrcFiles/SAP'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
        
        self.RowCnt     = -1
        self.ib.fld     = 'SAP'
        self.ib.wkf     = 'wkf_SDL_SAP_DLY'    
        self.ib.srcFile = ( 'employee_company.txt','employee_group.txt','employee_job.txt','employee_status.txt','employee_subgroup.txt','zhpij510.zhpip510.edwactives.txt',)    # File that Informatica expects. Alphabetical.
             
        self.ib.FileColCnt = {'employee_company.txt':2,
                              'employee_group.txt':2,
                              'employee_job.txt':2,
                              'employee_status.txt':2,
                              'employee_subgroup.txt':2,
                              'zhpij510.zhpip510.edwactives.txt':23,
                               }
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 4   
        self.procCort  = {}
        self.procBuReg = {}
        
        self.ts        =  su.getTimeSTamp()
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.getFtpFiles      ,  # FTP Files based on fixed list.
                         'C' : self.getIncSetFiles   ,  # Populates self.incSetFiles. Incoming Files.  
                         'D' : self.setIncFilePvt    ,  # Run after setting self.incFiles
                         'E' : self.cpFileToWorkDir  ,  # Copies FileSet and sets self.workFiles (full path)
                         'F' : self.archFiles        ,
                         'G' : self.chkFileColsFlg   ,
                         'H' : self.rWkfFlowFlg      ,
                         'I' : self.chkNextRunFlg    ,   
                         'J' : self.procSAPEmpFile   ,  # Check that headers/record count, number of fields.                        'G' : self.procCortFiles    ,
                       
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
        os.environ['FILE'     ] =  ('employee_company.txt,employee_group.txt,employee_job.txt,employee_status.txt,employee_subgroup.txt,zhpij510.zhpip510.edwactives.txt')  # Should match, this are expected files.
        os.environ['RXFILE'   ] =  ''

    def archFiles(self):
        return self.archGenFiles(self.incFiles, su.getTimeSTamp())

    def getFtpFiles(self):
        return ft.get('SQP Employee',self.log)
    
    def procSAPEmpFile(self):
        
        # Get  trailer date
        cur_dayr = self.getFileProcDate(self.workFiles[0])
        self.log.debug('Current Run day = %s from file = %s' % (cur_dayr,self.workFiles[0]))
        
        #Verify Trailers
        rc = self.chkTrailer(self.workFiles,cur_dayr)
        if rc != 0:
            self.log.error("Issue with Trailers")
            return rc
        
        #Verify Column Numbers:
        if self.checkFileColsFlag is True:
            rc = self.checkFileCols(self.workFiles,FLD_SEP)
            if rc != 0:
                self.log.error('Issue with column number. PLease check bad directory under %s' % self.ib.badDir)
                return rc
        
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)                
        self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
        prev_dayr = self._getCtlFile()
        
                      
        if self.checkNextRunFlg is True:
            
            if prev_dayr is None or prev_dayr.strip() == '': 
                self.log.error("Could not find control file or No Data")
                return -1
            
            rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log)
            if rc != 0 : 
                self.log.error("self._chkNextRun rc = %s" % rc)
                return rc
        
        # Create trigger file:
        #rc = self.crtTrigFile()
        #if rc != 0 : return rc  
            
        if self.runWkfFlowFlg is True: 
            # Run workflows
            if self._wkfSAPEmp()       != 0 : return 1
         
            # Loading Staging Succeeded. Update the control file.
            rc = fu.updFile(ctlFile,cur_dayr)               
            if rc == 0 :
                if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s'     % (prev_dayr,cur_dayr, ctlFile))
                else                   : self.log.info('Overwriting Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr, ctlFile))
            else       : 
                self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
                return rc 
            
            for fn in self.workFiles:
                r = fu.delFile(fn) 
                self.log.info('Removing %s rc = %s ' % (fn,r))     
        
        return rc
                    
    # Get the files from the remote host (source system).
    # Placed in the self.landDir in local host
    # Move files in remote host from . to inprocess/processed dir.

    # Workflow execution.
    def _wkfSAPEmp(self):
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc


        
def main(Args):
    a = SAPEmployee()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()        
    rc = main(sys.argv)
    sys.exit(rc)
        

