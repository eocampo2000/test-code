'''
Created on Sep 4, 2013

@author: eocampo

Customer Cost Analysis. Checks for 2 trigger files and runs on the 8th workday.
'''

__version__ = '20130911'

import sys
from apps.infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 

# mandatory to define self.cmdStep
class CustCostAnl(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(CustCostAnl,self).__init__()
        self.landDir    = 'SrcFiles/cust_cost'
        self.trigFiles  = []                  # Incoming Files
       
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.isWorkDayWarn       ,
                         'C' : self.getTrigFilesDate    ,
                         'D' : self.verTrigFileDate     ,
                         'E' : self.mvTrigFileToArchDir ,   # Move files after dates within are verified.
                         'F' : self.wfactInvChg         ,
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
 

  

    def wfactInvChg(self):
        self.ib.fld = 'CINV_Invoice_Data'
        self.ib.wkf = 'wkf_cinv_fact_invoice_charges'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    

        
def main(Args):
        a = CustCostAnl()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)
