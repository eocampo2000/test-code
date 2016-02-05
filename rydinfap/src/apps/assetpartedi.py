'''
Created on Jan 6, 2015

@author: eocampo

Added 2 more mainframe file as per Srinath request on 20160606.

'''

__version__ = '20150206'


import sys
import os
#import time
from datetime import datetime

import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
#mport datastore.dbapp   as da
import procdata.procinfa as pi 
import procjobs.procsched as psc

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
RUN_PER_DAY = 1  # Daily runs.
DP_LEN      = len('YYYYMM')  
   
# Schedules
SCH_FREQ = 'Mthly'
sch = ()
cur_dayr   = su.getTodayDtStr('%Y%m')


class AssetPartEdi(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(AssetPartEdi,self).__init__()
        self.landDir    = 'SrcFiles/asset'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
  
        self.srcFile    = ('ei111d03.dat', 'ei112d02.dat','ei112d02_frtl_can.dat','ei112d02_mvprefrd.dat','ei112d02_mvprf_can.dat','ei112d02_nav_can.dat',)  # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.EI111D03.DATAW','P.EI114JBM.EI112D02.DATAW','P.EI114JBM.EI112D02.DATAW.FRTL.CAN','P.EI114JBM.EI112D02.DATAW.MVPREFRD','P.EI114JBM.EI112D02.DATAW.MVPRF.CAN','P.EI114JBM.EI112D02.DATAW.NAV.CAN'"
        
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.isWorkDayWarn    ,
                         'C' : self.getFtpFiles      ,  # Sets self.incFileSet
                         'D' : self.getIncSetFiles   , 
                         'E' : self.copyFilesWorkDir ,
                         'F' : self.archFilesTS      ,
                         'G' : self.chkNextRunFlg    ,   
                         'H' : self.procAssetPartEDI ,           
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
        #os.environ['FILE'     ] =  (r"\'P.PO870D30.WHOUSE.COMP.STD\'")    # 
        os.environ['FILE'     ] =  (r"\'P.EI111D03.DATAW\',\'P.EI114JBM.EI112D02.DATAW\',\'P.EI114JBM.EI112D02.DATAW.FRTL.CAN\',\'P.EI114JBM.EI112D02.DATAW.MVPREFRD\',\'P.EI114JBM.EI112D02.DATAW.MVPRF.CAN\',\'P.EI114JBM.EI112D02.DATAW.NAV.CAN\'")    # 
        os.environ['RXFILE'   ] =  ('None')    # 
        
        # Sets a flag, to check for next run.
             
    def getFtpFiles(self):
        return ft.getn('AssetPartEdi',self.log)
    
    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)

    def _wkfAssetPartEDI(self):
        self.ib.fld     = 'Asset'
        self.ib.wkf     = 'wkf_parts_edi_monthly'   
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def procAssetPartEDI(self):
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
        if self._wkfAssetPartEDI()      != 0 : return 1
     
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
    a = AssetPartEdi()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 

    rc=  main(sys.argv)
