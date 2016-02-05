'''
Created on Dec 13, 2013

@author: eocampo
This program checks for a Trigger file/

'''

__version__ = '20140103'

import sys
import procjobs.procsched as psc
import utils.fileutils   as fu
import utils.strutils    as su
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
  
# Schedules
SCH_FREQ = 'Wkly'

cur_dayr   = su.getTodayDtStr('%Y%m%d')

class VehOrdStatWkly(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehOrdStatWkly,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        self.trigFiles  = []    # Incoming Trigger File.
        
        self.RowCnt     = -1
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.chkNextRunFlg    ,   
                         'C' : self.getTrigFiles     ,
                         'D' : self.verTrigFileDate  ,
                         'E' : self.procVehOSWkly    ,           
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
            
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0   
                 
    def _wkfVehOrdStatWkly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_vehicle_order_status_weekly' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
      
    def procVehOSWkly(self):
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)
        prev_dayr = self._getCtlFile()                
        self.log.debug('Ctl File is %s self.checkNextRunFlg is %s prev_dayr=%s' %  (ctlFile,self.checkNextRunFlg,prev_dayr))
        
        
        if self.checkNextRunFlg is True:
            
            if prev_dayr is None or prev_dayr.strip() == '': 
                self.log.error("Could not find control file or No Data")
                return -1
            
            rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log)
            self.log.debug(" getNextRunDate rc = %s " % rc)
            if rc != 0 : 
                self.log.error("self._chkNextRun rc = %s" % rc)
                return rc
 
        # Run workflow
        if self._wkfVehOrdStatWkly()       != 0 : return 1
     
        # Loading Staging Succeeded. Update the control file.
        rc = fu.updFile(ctlFile,cur_dayr)               
        if rc == 0 :
            if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s'     % (prev_dayr,cur_dayr, ctlFile))
            else                   : self.log.info('Overwriting Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr, ctlFile))
        else       : 
            self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
            return rc 
        
        return rc
            
def main(Args):
    a = VehOrdStatWkly()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
