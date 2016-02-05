'''
Created on Jan 8, 2014

@author: eocampo

Hyperion.
20151020 EO Added idl worklfow run.

'''

__version__ = '20150120'

import sys


import utils.strutils    as su
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

FLD_SEP  = '|'


class HypSDLDly(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(HypSDLDly,self).__init__()
        self.landDir    = 'SrcFiles'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
        
        self.RowCnt     = -1
    
        self.ib.srcFile = ('FMSGL.TXT',)       # File that Informatica expects. Alphabetical.
             
        self.ib.FileColCnt = {'FMSGL.TXT':4, }  
                
        self.ts        =  su.getTimeSTamp()
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self. getIncFiles    ,  # Populates self.incSetFiles. Incoming Files.  
                         'C' : self.cpFileToWorkDir ,  # Copies FileSet and sets self.workFiles (full path)
                         'D' : self.archFiles       ,
                         'E' : self.procHypSdlFile  ,
                         'F' : self.wkfHypSdlDly    ,  #
                         'G' : self.wkfHypIdlDly    ,                  
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

    def archFiles(self):
        return self.archGenFiles(self.incFiles, su.getTimeSTamp())
    
    def procHypSdlFile(self):        
        rc = self.checkFileCols(self.workFiles,FLD_SEP)
        if rc != 0:
            self.log.error('Issue with column number. PLease check bad directory under %s' % self.ib.badDir)
        return rc
                                                
    # Workflow execution.
    def wkfHypSdlDly (self):
        self.ib.fld     = 'HYP'
        self.ib.wkf     = 'wkf_SDL_HYP_DLY'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    
    # Workflow execution.
    def wkfHypIdlDly (self):
        self.ib.fld     = 'IDL'
        self.ib.wkf     = 'wkf_IDL_HYP_DLY'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
        
def main(Args):
    a = HypSDLDly()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)
        

