'''
Created on Jan 6, 2014

@author: eocampo

'''

__version__ = '20140106'

import sys
import utils.strutils    as su
import procdata.procinfa as pi 
import procjobs.jobpred  as jp

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
  
class SrvCalls(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(SrvCalls,self).__init__()
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
                         'B' : self.isWorkDayWarn    ,   
                         'C' : self.getPredStatus    ,
                         'D' : self.wkfSrvCalls      ,       
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
            
    def getPredStatus(self):      
        plst = jp.getPred(self.ib.pred, self.log)
        self.log.debug("plst= ", plst)
        return self.chkPred(plst)
            
    def wkfSrvCalls(self):
        self.ib.fld = 'OEM'
        self.ib.wkf = 'wkf_oem_ride_monthly' 
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
              
def main(Args):
    a = SrvCalls()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
