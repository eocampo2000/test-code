'''
Created on Jul 30, 2012

@author: eocampo
'''
'''
Created on Jun 10, 2012

@author: eocampo

Nwe Class Style
'''
__version__ = '20120923'

import sys
  
import utils.filetransf  as ft
import procdata.procinfa as pi 

from apps.infbaseapp import _InfaBaseApp
# mandatory to define self.cmdStep

class TISTotFrt(_InfaBaseApp):  
    exitOnError = True
    def __init__(self):               
#       _InfaBaseApp.__init__(self)
        super(TISTotFrt,self).__init__()
        self.landDir    = 'TgtFiles'
        self.incFiles   = []                  # Incoming Files
        self.procFiles  = []                  # Files to process
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles)
        self.trigFiles  = []                  # Trigger Files
        self.ib.fld     = 'TIS'
        self.ib.wkf     = 'wkf_TOT_FRT'
        #self.verFiles   = []                 # File verification
        #self.ib.srcFile = 'wkf_TOT_FRT.done'  # trigger file
    

        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock     ,
                         'B' : self.getTrigFiles,              
                         'E' : self.runWkfl     ,
                         'P' : self.putFiles    ,
                         'R' : self.remTrigFiles,                    
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = { 
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,  
                'INFA_SHARE'       : 'self.ib.shareDir'   ,    
                'CHK_FILES'        : 'self.ib.chkFiles'   ,
               }

    def _setDataDir(self):
        self.ib.landDir = '%s/%s'    % (self.ib.shareDir,self.landDir )
        self.ib.archDir = '%s/%s/%s' % (self.ib.shareDir,self.landDir, self.ib.archDirName)
        return 0
    
    def runWkfl(self):
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 1
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    # This method will FTP the file into the boeing site.    
    def putFiles(self):
        rc= ft.put(self.appName,self.log)
        return rc  
           
def main(Args):
    a = TISTotFrt()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    rc=  main(sys.argv)
    
