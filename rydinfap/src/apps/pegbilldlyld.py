'''
Created on Jan 9, 2014

@author: eocampo

RPR_PEG_Billing_Daily Loads

'''

__version__ = '20140109'

import sys

from apps.infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class pegBillDlyLd(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(pegBillDlyLd,self).__init__()
        self.landDir    = ''
        self.trigFiles  = []                  # Incoming Files
 
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock              ,
                         'B' : self.wPegBillDlyRef       ,
                         'C' : self.wPegBillDlyMainFrame ,
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
 
    def _setDataDir(self) : return 0
    
    def wPegBillDlyRef(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Daily_Reference_Load'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wPegBillDlyMainFrame(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Daily_Mainframe_Load'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
        
def main(Args):
        a = pegBillDlyLd()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)