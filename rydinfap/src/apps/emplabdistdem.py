'''
Created on Jul 17, 2015

@author: eocampo

On demand version for EmpLaborDistMthly

'''
__version__ = '20150717'

import sys
import os

import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
class  EmpLaborDistOnDem(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super( EmpLaborDistOnDem,self).__init__()
        self.landDir    = 'SrcFiles/employee'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile     = ('hr_lbr_distr.csv','hr_payroll_jobs_pos.csv')                # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"HR_Lbr_Distr.csv,Sales HR Payroll Jobs and Positions.csv"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.getFtpFiles      ,
                         'C' : self.getIncSetFiles   ,
                         'D' : self.copyFilesWorkDir ,
                         'E' : self.archFilesTS      ,
                         'F' : self.procEmpLaborDist , 
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
        os.environ['FILE'     ] =  (r'HR_Lbr_Distr.csv,Sales HR Payroll Jobs and Positions.csv')
        # Sets a flag, to check for next run.
             
    def getFtpFiles(self):   
        return ft.get('EmployeeDem',self.log)

    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
       
    def _wkfEmpLaborDistOnDem(self):
        self.ib.fld = 'Employee'
        self.ib.wkf = 'wkf_labor_distribution_on_demand' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def procEmpLaborDist(self):
        # Run workflows
        if self._wkfEmpLaborDistOnDem()      != 0 : return 1
     
        for i in range (len(self.srcFile)):   
            t   = '%s/%s' % (self.ib.workDir,self.srcFile[i]) 
            r = fu.delFile(t) 
            self.log.info('Removing %s rc = %s ' % (t,r))     
        return 0
            
def main(Args):
    a =  EmpLaborDistOnDem()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
