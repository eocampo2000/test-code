'''
Created on Jan 30, 2015

@author: eocampo
     This program expects a MF src file, when found will invoke Informatica wkf  and will send an email attaching  a file  produced by Infa process. 
'''

__version__ = '20150302'

import sys
from datetime import datetime
  
import utils.fileutils    as fu
import utils.strutils     as su
import procdata.procinfa as pi 
#import datastore.dbapp    as da
import procjobs.procsched as psc

from apps.infbaseapp  import _InfaBaseApp
from infbaseutil import _WalkerFlatFile


# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
RUN_PER_DAY = 1  # Daily runs.
DP_LEN      = len('YYYYMM')  
   
# Schedules
SCH_FREQ = 'Mthly'
sch = ()
cur_dayr   = su.getTodayDtStr('%Y%m')

# mandatory to define self.cmdStep
# EO TODO if the program runs a set of files, row count will only be verified on the latest load date.
#         need to add the capability to check in very loaded file !
class FinVehMissing(_InfaBaseApp,_WalkerFlatFile):  
    
    exitOnError = True   # default.
    
    def __init__(self):
        
        #_InfaBaseApp.__init__(self)
        super(FinVehMissing,self).__init__()
        self.landDir    = 'SrcFiles/finance'
        self.tgtDir     = 'TgtFiles/finance'
        self.incFiles   = []                  # Incoming Files
        self.procFiles  = []                  # Files to process
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles)
        self.RowCnt     = -1
        self.ib.fld     = 'Financial'
        self.ib.wkf     = 'wkf_fin_ap_can_veh_us_mthly'    
        self.ib.srcFile = 'Veh_scg4100.txt'   # File that Informatica expects
        self.srcCount   = { 'Veh_scg4100.txt' : -1,  
                          }
        
        self.tgtFiles  = ('Missing_CAN_Vehicles_Repaired_US_Details.csv',)
        self.FILE_SET_LEN = len([self.ib.srcFile,])
        

        self.checkNextRunFlg = False
        self.postCheckFlg    = False
        
        self.fileDate   = ''                  # 
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getIncFiles     ,     # Populates self.incFiles.
                         'C' : self.cpFileToWorkDir ,     # Populates self.workFiles
                         'D' : self.archFiles       ,     # ARCH and Moves Src files.
                         'E' : self.chkNextRunFlg   ,
                         'F' : self.procIncFiles    ,  
                         'N' : self.notifyUsers     ,
                         'H' : self.archTgtFiles    ,     # ARCH and Moves TGT files.                 
                        }
       
        # Infa Environmental variables.
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
 
            date = '%s%s' % (cur_dayr[:4],cur_dayr[4:6])  

            rc = su.isValidDate(date,'%Y%m')
            if rc is False :
                self.log.error('Invalid Date %s on file %s ' % (date,fn))
                return 2
            
            self.fileDate =  date
            self.log.debug('self.fileDate = %s' % (self.fileDate))
 
            ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)                
            self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
            prev_dayr = self._getCtlFile()
            
            if self.checkNextRunFlg is True:
                
                if prev_dayr is None or prev_dayr.strip() == '': 
                    self.log.error("Could not find control file or No Data")
                    return -1
                
                rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log,sch)
                if rc != 0 : 
                    self.log.error("self._chkNextRun rc = %s" % rc)
                    return rc
     
            procFiles = self.chkTrailer([fnp,],fn,cur_dayr)
            
            if len(procFiles) != self.FILE_SET_LEN : 
                self.log.error("chkTrailer Files that were OK ",  procFiles)
                return 4
                      
            t = '%s/%s' % (self.ib.workDir , self.ib.srcFile)
            self.log.debug('fnp =%s Move to %s ' % (fnp,t))
            rc = fu.moveFile(fnp, t)
            if rc != 0 : 
                self.log.error('Could not move File %s to %s' % (fnp,t))
                return 5
            
            self.log.info('mv src file fnp %s -> t %s' % (fnp,t))                        
            # Invoke workflow.   
            rc = pi.runWkflWait(self.ib,self.log)  
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
            
            i+=1  
        
        return rc
    
    def notifyUsers(self):
        self.files = []
        self.subj = 'Monthly details for Canadian Vehicles repaired in US'
        self.text = 'Please find attached the monthly details for Canadian vehicles repaired in US. Please review and let us know if any issues. Thanks..!!'
        self.rc   = 0
        
        for tf in self.tgtFiles:
            tfp  = '%s/%s/%s' % (self.ib.shareDir,self.tgtDir,tf)
            if fu.fileExists(tfp):
                self.files.append(tfp)
            else :
                self.log.error('File %s does not exist' % tfp)    
        
        self.log.info('Attaching ', self.files)
        
        if len(self.files) < 1:
            self.log.error('No attachments where found !')
            return 1
        
        rc = self._notifyAppUsers('plain')
        self.log.debug('rc = %s SUBJ %s \t MSG %s' % (rc,self.subj, self.text))
                
        return rc
    
    def archTgtFiles(self):
        self.ib.archDir = '%s/%s/%s' % (self.ib.shareDir, self.tgtDir,self.ib.archDirName)
        fls = ('%s/%s/%s' % (self.ib.shareDir,self.tgtDir ,self.tgtFiles[0]),)
        return self.archGenFiles(fls,su.getTimeSTamp())
        
def main(Args):
    a = FinVehMissing()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()       
    rc=  main(sys.argv)  
    sys.exit(rc)