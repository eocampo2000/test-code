'''
Created on Aug 22, 2014

@author: eocampo
'''
__version__ = '20150114'

import sys
import utils.fileutils    as fu
import utils.strutils     as su
import procdata.procinfa  as pi 
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

class ClaimMonthly(_InfaBaseApp):
    exitOnError = True

    def __init__(self):
        super(ClaimMonthly,self).__init__()
        self.landDir    = ''
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile).
        self.trigFiles  = []    # Incoming Trigger File.


        self.fileDate   = ''
        self.FILE_SET_LEN = 1

        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set
        self.cmdStep = { 'A' : self.getLock           ,
                         'B' : self.isLastWorkDayWarn ,
                         'C' : self.chkNextRunFlg     ,   
                         'D' : self.procClaims        ,
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

    def _setDataDir(self) : return  0

    def _wkf_claims_mthly(self):
        self.ib.fld = 'Accident'
        self.ib.wkf = 'wkf_claims_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def procClaims(self):
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
        if self._wkf_claims_mthly()      != 0 : return 1
     
        # Loading Staging Succeeded. Update the control file.
        rc = fu.updFile(ctlFile,cur_dayr)               
        if rc == 0 :
            if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s'     % (prev_dayr,cur_dayr, ctlFile))
            else                   : self.log.info('Overwriting Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr, ctlFile))
        else       : 
            self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
        
        return rc 

def main(Args):
    a = ClaimMonthly()
    rc = a.main(Args)
    return rc

if __name__ == '__main__':
    rc=  main(sys.argv)
