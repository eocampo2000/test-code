'''
Created on Oct 2, 2013

@author: eocampo
'''

__version__ = '20131002'

import sys
import time
from infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 

# mandatory to define self.cmdStep
class RunPJob(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(RunPJob,self).__init__()
        self.landDir    = 'SrcFiles/finance'
        self.trigFiles  = []
       
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.remPrevTrigFiles    ,
                         'C' : self.chkPredComplete     ,
                         'D' : self.wfactInvChg         ,
                         'E' : self.mvTrigFileToArchDir ,
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
  
  
  
    def remPrevTrigFiles(self):
        s = len(self.ib.trigFiles)              
        self.log.debug('Trigger files %s -- Len = %d' %  (self.ib.trigFiles,s))
        if s < 1 : return 1
        
        fns = self.ib.trigFiles.split(',')
        for fn in fns:
            f = '%s/%s' % (self.ib.landDir, fn)
            self.trigFiles.append(f)
        
        if len(self.trigFiles) < 1 : return 2
        
        rc = self.remTrigFiles()
        self.log.debug("remTrigFiles = %s" % rc)
        return rc
        
    # This method will run a wkfl to chk an SQL Table for job completion.
    def chkPredComplete(self):
                     
        i = 1
        wi = int(self.ib.waitFileIter)    
        while (i <= wi):           
            rc = self._wchkSqlTrig()
            if rc != 0 : return rc
              
            rc = self._chkFiles(self.trigFiles)
            if rc != 0   :
                self.log.info('Will check for trigger file(s) in ', self.ib.waitFile, ' secs.')
                time.sleep(float(self.ib.waitFile))
                self.log.info('Iteration %d out of %d ' % (i, wi))
                i += 1
            elif rc == 0 :    
                self.log.info('Trigger file(s) ', self.trigFiles, ' found.' )
                return 0
    
        return 1

        
    def _wchkSqlTrig(self):
        self.ib.fld = 'CINV_Invoice_Data'
        self.ib.wkf = 'wkf_check_job_completion' 
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc        

    def wfactInvChg(self):
        self.ib.fld = 'CINV_Invoice_Data'
        self.ib.wkf = 'wkf_cinv_fact_invoice_charges'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    

        
def main(Args):
        a = RunPJob()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)
