'''
Created on Sep 6, 2013

@author: eocampo

Code needs to run 4th Workday.
Scheduled.
Predecessor for OEM 

'''


__version__ = '20130906'

import sys

from apps.infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class pegPurchOrd(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(pegPurchOrd,self).__init__()
        self.landDir    = ''
        self.trigFiles  = []                  # Incoming Files
 
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.isWorkDayWarn       ,
                         'C' : self.wPegPurchOrd        ,
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
 

    def wPegPurchOrd(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Purchase_Order'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
        
def main(Args):
        a = pegPurchOrd()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)