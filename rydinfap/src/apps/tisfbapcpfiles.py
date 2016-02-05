'''
Created on Aug 29, 2012

@author: eocampo
'''

__version__ = '20120915'

import sys
  
import utils.filetransf  as ft
import utils.fileutils   as fu

from apps.infbaseapp import _InfaBaseApp
# mandatory to define self.cmdStep

class TISFBAPCpFiles(_InfaBaseApp):  
    def __init__(self):
        _InfaBaseApp.__init__(self)
        self.landDir    = 'SrcFiles'
        self.incFiles   = []                  # Incoming Files

        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock     ,
                         'B' : self.chkTrigFiles, # ChkTriggerFiles
                         'R' : self.remTrigFiles,
                         'D' : self.remFiles    , # To remove existing src files
                         'G' : self.getFiles    ,
                         # 'C' : self.touchFiles,  Modified to _touchFiles and accepts a list instead of ',' string
                        }


        # Infa Environmental variables/
        self.infaEnvVar   = { 
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,  
                'INFA_SHARE'       : 'self.ib.shareDir'   ,    
               }

    def _setDataDir(self):
        self.ib.landDir = '%s/%s'    % (self.ib.shareDir,self.landDir )
        #self.ib.archDir = '%s/%s/%s' % (self.ib.shareDir,self.landDir, self.ib.archDirName)
        return 0
    
    # This method will FTP the file into the boeing site.    
    def getFiles(self):
        rc= ft.get(self.appName,self.log)
        return rc  
           
def main(Args):
    a = TISFBAPCpFiles()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    setEnvVars()         # Remove in UX 
    print "INVOKED SET_ENV"
    rc=  main(sys.argv)
    
