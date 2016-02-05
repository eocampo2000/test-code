'''
Created on Oct 28, 2015

@author: eocampo
  
To execute a remote command. At this point code runs in UNIX only.  
 '''
__version__ = '20151028'

import sys

from apps.infbaseapp import _InfaBaseApp

import cmd.oscmd         as oc 
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class RemNZMig(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):

        super(RemNZMig,self).__init__()
        self.landDir    = 'SrcFiles'
        self.trigFiles  = []                  # Incoming Files
 
        self.verPIDFlg  = False

        # For App Notification
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock    ,
                         'B' : self.vPidFlg    ,
                         'C' : self.exeRmtCmd ,
                        # 'Z' : self.chkPreq         ,
                        }
       
        self.infaEnvVar   = {
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
                'INFA_APP_CTL'     : 'self.ib.ctlDir'     ,          
               }
 
 
    def _setDataDir(self): pass
       
    def vPidFlg(self):
        self.verPIDFlg = True
        return 0

    def _exeRmtCmd(self):
        cmd  = '%s/%s; echo rc=$?' % (self.ib.rpath,self.ib.rscript)
    #ib.pwd = 'Ryder#123'
        rcmd = oc.CmdLinuxDriver(self.log,self.ib)
        self.log.debug('rcmd = \n\n%s' % cmd)
        rmsg = rcmd.execRemCmd(cmd, 'Linux')
        return rmsg
       
    def exeRmtCmd(self):
        if(sys.platform == 'win32'):
            self.log.error('This code is not implemented to run in Windows Platforms.')
            return 1
        
        rmsg =  self.rshNZDataMigr()
        self.log.debug('rmsg = %s' % rmsg)
        return 0
    
def main(Args):
        a = RemNZMig()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
