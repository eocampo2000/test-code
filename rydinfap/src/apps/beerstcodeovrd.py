'''
Created on Sep 4, 2013

@author: eocampo

File Directory: $PMSourceFileDir\BeerStore
This module check for incoming file and starts  wkf if file is present. Will send email when loaded.

'''

__version__ = '20130904'

import sys

import utils.fileutils    as fu
import utils.strutils     as su
#import procdata.procinfa  as pi 

from apps.infbaseapp       import _InfaBaseApp


# Mandatory to define self.cmdStep

FLDxROW     = 12   # Fields per row in CSV file. 
SEP         = ','
   
class BeerStCodeOvrd(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(BeerStCodeOvrd,self).__init__()
        self.landDir    = 'SrcFiles/BeerStore'
#        self.incFileSet = []    # Incoming Files. Contains full path name.
#        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
        
        self.ib.fld     = 'BeerStore'
        self.ib.wkf     = 'wkf_elctr_inv_trdng_prtnr_maint_cd_ovrrd'    
        self.ib.srcFile = ( 'PMCodeOverride.csv',)    # File that Informatica expects. Alphabetical.
             
        self.checkNextRunFlg  = False
        self.verifyCSVFlg     = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''   
        self.ts        =  su.getTimeSTamp()

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.chkIncFilesWarn  ,  # Wrapper for getIncFiles. Populates self.incSetFiles. Incoming Files.
                         'C' : self.cpFileToWorkDir  ,  # Populates self.workFiles
                         'D' : self._archFiles        ,  # ARCH and Moves files.
                         'F' : self.verCSVFlg        ,  # Verify CSV contents, columns
                         'G' : self.procIncFiles     ,     
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
                
    # Sets a flag, to Verify CVS content.
    def verCSVFlg(self) : 
        self.verifyCSVFlg = True
        return 0    
   
    def _archFiles(self) :
        rc = self.archGenFiles(self.incFiles,  self.ts)
        self.log.info('archCtlFiles rc = %s' %  rc)
        return 0 

    # Workflow execution.
    def _wkfCodeOvrd(self):
        rc = 0
        #rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    # This method will call the informatica command to run the wklf
    # ProcFiles is a set of files to process (complete path). 
    def procIncFiles(self):

        if len(self.workFiles) < 1 : 
            self.log.error('No files to process')
            return 1
        
        self.workFiles.sort()
        rc = 0
        
        # Files in the working directory:
        i = 1
        self.log.debug('Will Process a total of %d file(s) ' % len(self.workFiles))
        for fnp in self.workFiles:
            self.log.info('\nProcessing File (%d) =>  %s ' % (i,fnp))
            fn    = fu.getFileBaseName(fnp)
            
            if self.verifyCSVFlg is True:    
                r,b = fu.readCSV(fnp, FLDxROW, SEP)   
                
                if len(b) > 0 :  
                    fbad = '%s/%s.bad' % (self.ib.badDir,fn)                 
                    rc = fu.createFile(fbad , b)
                    self.log.error("No of %d bad row(s) on %s" % (len(b),fnp))
                    self.log.error("Creating file %s rc = %s" % (fbad,rc))
                    self.log.debug("Bad rows = , ", b)
                    return 5
                
                if len(r) == 0 :
                    self.log.error("No rows to process on file %s" % fnp)
                    return 6 
                               
            t = '%s/%s' % (self.ib.workDir , self.ib.srcFile[0])
            rc = fu.moveFile(fnp, t)
            self.log.info('Renaming %s to %s rc = %s' %  (fnp,t,rc))
            if rc != 0 : 
                self.log.error('Could not move File %s to %s' % (fnp,t))
                return 7
                                    
            # Invoke workflow.   
            rc = self._wkfCodeOvrd()  
            if rc != 0 : 
                self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                if self.exitOnError: 
                    self.log.debug('ExitOnError is TRUE rc = %s' % (rc))
                    return rc
            else : 
                self.log.info('Ran  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                           
                            
            r = fu.delFile(t) 
            self.log.debug('Deleting File %s rc = %s' % (t,r))
            i+=1  
        
        return rc
 
        
def main(Args):
    a = BeerStCodeOvrd()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()
    rc=  main(sys.argv)
