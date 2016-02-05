'''
Created on Jul 8, 2013

@author: eocampo

'''

__version__ = '20130716'

import sys

import utils.fileutils    as fu
import utils.strutils     as su
import procdata.procinfa   as pi 
import procjobs.procsched as psch

from apps.infbaseapp       import _InfaBaseApp
from infbaseutil      import _IMSSftyLocFile

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

RUN_PER_MTH = 1   # Mthly runs.
DP_LEN      = len('YYYYMM') 
FLDxROW     = 4   # Fields per row in CSV file. 
SEP         = '|'
   
class IMSSftyLoc(_InfaBaseApp,_IMSSftyLocFile):  
    exitOnError = True
    
    def __init__(self):
        super(IMSSftyLoc,self).__init__()
        self.landDir    = 'SrcFiles/IMS'
#        self.incFileSet = []    # Incoming Files. Contains full path name.
#        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
        
        self.ib.fld     = 'IMS'
        self.ib.wkf     = 'wkf_IMS_sfty_dim_loc'    
        self.ib.srcFile = ( 'ims_sfty_location.csv',)    # File that Informatica expects. Alphabetical.
             
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
                         'E' : self.chkNextRunFlg    ,   
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
              
    # Sets a flag, to check for next run.
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0    
    
    # Sets a flag, to Verify CVS content.
    def verCSVFlg(self) : 
        self.verifyCSVFlg = True
        return 0    
   
    def _archFiles(self) :
        rc = self.archGenFiles(self.incFiles,  self.ts)
        self.log.info('archCtlFiles rc = %s' %  rc)
        return 0 

    # Workflow execution.
    def _wkfIMSSftyLoc(self):
        rc = pi.runWkflWait(self.ib,self.log)
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
        
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)
        
        self.workFiles.sort()
        rc = 0
        
        # Files in the working directory:
        i = 1
        self.log.debug('Will Process a total of %d file(s) ' % len(self.workFiles))
        for fnp in self.workFiles:
            self.log.info('\nProcessing File (%d) =>  %s ' % (i,fnp))
 
            # Get date run  from 1st filename
            fn    = fu.getFileBaseName(fnp)
            cur_dayr = self._getDateRunStr(fn)
            
            if cur_dayr is None : 
                self.log.error('No  Date String %s ' % cur_dayr) 
                return 1
            
            fmt = '%Y%m'
            date = '%s%s' % (cur_dayr[0:4],cur_dayr[4:6])

            rc = su.isValidDate(date,fmt)
            if rc is False :
                self.log.error('Invalid Date %s on file %s ' % (date,fn))
                return 2
            
            self.fileDate =  date
            self.log.debug('self.fileDate = %s' % (self.fileDate))
            
            if self.checkNextRunFlg is True:
                # Get Previous Run Info. File should contain one line only :  YYYYMM from storage.
                prev_dayr = self._getCtlFile()
                if prev_dayr is None : return 3
                
                pd,pr     = self._getMonth(prev_dayr,DP_LEN)
                if pd is None : return 4
                
                #rc = self._chkNextRun(cur_dayr,prev_dayr,pd,pr,RUN_PER_MTH)
                rc = psch.getNextRunDate(pd,cur_dayr, 'Mthly',self.log)
                if rc != 0 : 
                    self.log.error("self._chkNextRun rc = %s" % rc)
                    return rc  
                 
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
            rc = self._wkfIMSSftyLoc()  
            if rc != 0 : 
                self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                if self.exitOnError: 
                    self.log.debug('ExitOnError is TRUE rc = %s' % (rc))
                    return rc
            else : 
                self.log.info('Ran  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                           
            # Loading Staging Succeeded. Update the control file.
            rc = fu.updFile(ctlFile,cur_dayr)               
            if rc == 0 :
                if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr,   ctlFile))
                else                   : self.log.info('Overwriting Cur Load Date to  %s , Control File %s' % (cur_dayr,   ctlFile))
            else       : 
                self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
                return rc 
                            
            r = fu.delFile(t) 
            self.log.debug('Deleting File %s rc = %s' % (t,r))
            i+=1  
        
        return rc
 
        
def main(Args):
    a = IMSSftyLoc()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()
    rc=  main(sys.argv)
