'''
Created on Aug 4, 2014

This 
Vehicle Component Daily.

FTP File from Mainframe.
Rename and place in working dir
Invoke wkf

@author: eocampo
'''

__version__ = '20140804'

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
DP_LEN      = len('YYYYMMDD')  
   
   
# Schedules
#SCH_FREQ = 'WDay'
SCH_FREQ = 'Cust'
sch = ('Tue','Wed','Thu','Fri','Sat')

cur_dayr   = su.getTodayDtStr('%Y%m%d')

class VehPOdly(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehPOdly,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile    = ('PO225D15.dat','PO875D30.dat')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.PO225D15.UMPK(0)','P.PO875D30.WHOUSE.CURR.STD'"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock           ,
                         'B' : self.getFtpFiles       ,  # Sets self.incFileSet
                         'C' : self.getIncSetFiles    , 
                         'D' : self.copyFilesWorkDir  ,
                         'E' : self.archFilesTS       ,
                         'F' : self.chkNextRunFlg     ,   
                         'G' : self.procVehPO         ,     
                         'H' : self.wkfVehOrdStatWkly ,     

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
        #os.environ['FILE'     ] =  (r"\'P.PO225D15.UMPK\(0\)\',\'P.PO875D30.WHOUSE.CURR.STD\'")    # 
        os.environ['FILE'     ] =  (r"\'P.PO225D15.UMPK(0)\',\'P.PO875D30.WHOUSE.CURR.STD\'")    # 
        os.environ['RXFILE'   ] =  ('None')    # 
        # Sets a flag, to check for next run.
    
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0   
             
    def getFtpFiles(self):
        return ft.getn('Vehicle',self.log)
    
    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
       
    def _wkfVehPODly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_veh_purchase_order_daily' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
      
    def procVehPO(self):
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
        if self._wkfVehPODly()       != 0 : return 1
     
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
  
    def wkfVehOrdStatWkly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_vehicle_order_status_weekly' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
              
def main(Args):
    a = VehPOdly()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
