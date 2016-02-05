
'''
Created on Sep 5, 2012

@author: eocampo

This code will execute fin_detail logic using Mianframe Source.
In order to ensure that end to end wkf completed it will check a state file for RUN.
New Style class

'''
__version__ = '20130522'

import sys
#import time
from datetime import datetime
import utils.fileutils   as fu
import utils.strutils    as su
import procdata.procinfa as pi 
import proc.process      as p

from apps.infbaseapp  import _InfaBaseApp

   
class FinDetailMF(_InfaBaseApp):  
    exitOnError = True
    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(FinDetailMF,self).__init__()        
        self.checkRunState = False
        self.runWkfFlowFlg = False
        
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getRunState     ,      # Sets self.incFileSet
                         'C' : self.crtTrigFiles    ,      # Creates trigger files. Not for 4:00am runs !
                         'D' : self.wFinDetail      ,
                         'Z' : self.postCheck       ,
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = {
                'PMCMD'            : 'mg.pmcmd'           ,
                'INFA_USER'        : 'self.ib.rep_user'   ,
                'INFA_XPWD'        : 'self.ib.rep_xpwd'   ,
                'DOMAIN'           : 'self.ib.dom_name'   ,
                'INT_SERV'         : 'self.ib.IS'         , 
                'INFA_APP'         : 'self.ib.appDir'     ,    
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
                'INFA_APP_CTL'     : 'self.ib.ctlDir'     ,    
                'INFA_SRC'         : 'self.ib.srcDir'     , 
             
               }
     
    def _setDataDir(self):   
        pass
    
    
    # This method is used to create trigger files. Should not run at 4:00 am !
    def crtTrigFiles(self):
        print "type(self.ib.trigfiles) %s " % type(self.ib.trigfiles)
        if len(self.ib.trigfiles) < 0:
            self.log.error('Cannot Create Trigger Files')
            return 1 
        self.ib.crtFiles = []
        
        fns = self.ib.trigfiles.split(',')
        for tf in fns:
            self.ib.crtFiles.append('%s/%s' % (self.ib.srcDir,tf))
        
        self.log.debug('TrigFiles', self.ib.crtFiles)    
        return self._touchFiles()
    
    # This method gets the next schedule run date
    # pd is prev run date, from storage.
    
    def getRunState(self):
        s = self.getStateFile()
        if s is None or s == '' : return 1
        state = (s.strip()[-3:]).lower()
        if state == 'run' :
            self.log.info('State is %s' % (state)) 
            return 0
        else:
            self.log.error('State is %s. Needs to be "run" to proceed !' % (s))
            return 2 
    
    
    def wFinDetail(self):
        
        # Update State File, for next run. Wkf has a final command task that wilol UPdate the file to RUN.
        # If cmd task does not ran we know wkf is not successful and should not start.
        
        fn = '%s/%s.state' % (self.ib.ctlDir, self.appName)
        dt = '%s-EXECUTING' % su.getTimeSTamp('%d%02d%02d:%02d:%02d')
        self.log.debug("fn = %s Updating TO ==> dt = %s" % (fn,dt))
        rc = fu.updFile(fn,dt)
        self.log.info('Updating state returned %s' % rc)
        cmd = '%s/pstate.sh findetailmf  EXECUTING' % self.ib.appDir
        p.runSync(cmd,self.log)

        
        # Start WKF execution
        self.ib.fld     = 'Financial'
        self.ib.wkf     = 'wkf_FIN_DETAIL'
        
        rc = pi.runWkflWait(self.ib,self.log) 
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def postCheck(self):
        rc = self.getRunState()
        if rc != 0 :
            self.log.error('wkf_fin_detail had not completed rc = %s' % rc)
        return rc
        
        
def main(Args):
    a = FinDetailMF()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    
