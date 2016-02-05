'''
Created on Jan 30, 2014

@author: eocampo

To schedule FBAP for parallel testing.

'''

__version__ = '20140130'

import sys

import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep

class FBAPTestPar(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(FBAPTestPar,self).__init__()
        self.landDir    = 'SrcFiles'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        self.trigFiles  = []    # Incoming Trigger File.

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.getTrigFiles     ,  
                         'C' : self.wkfTISStgFBAPLoc ,
                         'D' : self.wkfTISTgtFBAP    ,
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
                             
    def wkfTISStgFBAPLoc(self):
        self.ib.fld = 'TIS'
        self.ib.wkf = 'wkf_TIS_STG_FBAP_LOCAL' 
        #rc = pi.runWkflWait(self.ib,self.log)
        rc=0
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
    
    def wkfTISTgtFBAP(self):
        self.ib.fld = 'TIS'
        self.ib.wkf = 'wkf_TIS_TGT_FBAP'
        rc=0 
        #rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
            
def main(Args):
    a = FBAPTestPar()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
