'''
Created on Jun 10, 2012

@author: eocampo

This method will be able to run source files (1 to n) in chronological order, based on filename.
Upon Failure will stop processing.

Add Next Run Check. Jobs run Mon to Sat (no Sundays).
Add _WalkerFlatFile Utility class
20130430 New Class Style.
20131114 Added None check for rs
20150728 Comment input file deletion.

'''

__version__ = '20150728'

import sys
from datetime import datetime
  
import utils.fileutils   as fu
import utils.strutils    as su
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp  import _InfaBaseApp
from infbaseutil import _WalkerFlatFile

RUN_PER_DAY = 1   # Daily runs.
DP_LEN      = len('YYYYMMDD')  

# mandatory to define self.cmdStep
# EO TODO if the program runs a set of files, row count will only be verified on the latest load date.
#         need to add the capability to check in very loaded file !
class InvAppDtl(_InfaBaseApp,_WalkerFlatFile):  
    
    exitOnError = True   # default.
    
    def __init__(self):
        
        #_InfaBaseApp.__init__(self)
        super(InvAppDtl,self).__init__()
        self.landDir    = 'SrcFiles/invoice'
        self.incFiles   = []                  # Incoming Files
        self.procFiles  = []                  # Files to process
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles)
        self.RowCnt     = -1
        self.ib.fld     = 'Invoice'
        self.ib.wkf     = 'wkf_INV_AP_DTL'    # EO UNCOMMENT for PROD RELEASE
        self.ib.srcFile = 'Ap_scuaphis.txt'   # File that Informatica expects
        self.srcCount   = { 'Ap_scuaphis.txt' : -1,  
                          }
        
        self.FILE_SET_LEN = len([self.ib.srcFile,])
    
        self.checkNextRunFlg = False
        self.postCheckFlg    = False
        
        self.fileDate   = ''                  # 
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getIncFiles     ,     # Populates self.incFiles.
                         'C' : self.cpFileToWorkDir ,     # Populates self.workFiles
                         'D' : self.archFiles       ,     # ARCH and Moves files.
                         'F' : self.chkNextRunFlg   ,
                         'G' : self.postChkFlg      ,
                         'H' : self.procIncFiles    ,
                         'Y' : self.getDlyCnt       ,     # Standalone, otherwise invoke self.postCheck
                         
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
    
    # Sets a flag, to do a post check.
    def postChkFlg(self) : 
        self.postCheckFlg  = True
        return 0
    
    # Does Not Run On Sunday. 
    # Files are produced Sunday to Friday. 
    # Sunday file will be for Monday   4:00 am run
    # Friday file will be for Saturday 4:00 am run
    # Saturdays no file is produced.
    # After Friday run, next file creation will be on Sunday ! 
    # pd previous date run
    def _getNextRunDate(self,pd):
        mdate   = datetime.strptime(pd,'%Y%m%d')
        dayOfWk = mdate.weekday()
        nxtDay  = 1
        
        if dayOfWk == 4:  nxtDay = 2  # Friday
        
        nrd = su.getDayPlusStr(nxtDay,pd,'%Y%m%d')
        self.log.debug('pd=%s dayOfWk=%s nxtDay=%s Next Run Date (nrd) =%s ' % (pd,su.DL[dayOfWk],nxtDay,nrd))
        
        return nrd

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
 
            date = '%s/%s/%s' % (cur_dayr[4:6],cur_dayr[6:8],cur_dayr[:4])

            rc = su.isValidDate(date)
            if rc is False :
                self.log.error('Invalid Date %s on file %s ' % (date,fn))
                return 2
            
            self.fileDate =  date
            self.log.debug('self.fileDate = %s' % (self.fileDate))
            
            if self.checkNextRunFlg is True:
                # Get Previous Run Info. File should contain one line only :  YYYYMMDD from storage.
                prev_dayr = self._getCtlFile()
                if prev_dayr is None : return 3
                
                pd,pr     = self._getDay(prev_dayr,DP_LEN)
                
                rc        = self._chkNextRun(cur_dayr,prev_dayr,pd,pr,RUN_PER_DAY)
                if rc != 0 : 
                    self.log.error("self._chkNextRun rc = %s" % rc)
                    return rc  

            procFiles = self.chkTrailer([fnp,],fn,cur_dayr)
            
            if len(procFiles) != self.FILE_SET_LEN : 
                self.log.error("chkTrailer Files that were OK ",  procFiles)
                return 4
                      
            t = '%s/%s' % (self.ib.workDir , self.ib.srcFile)
            rc = fu.moveFile(fnp, t)
            if rc != 0 : 
                self.log.error('Could not move File %s to %s' % (fnp,t))
                return 5
                                    
            # Invoke workflow.   
            rc = pi.runWkflWait(self.ib,self.log)  
            if rc != 0 : 
                self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                if self.exitOnError: 
                    self.log.debug('ExitOnError is TRUE rc = %s' % (rc))
                    return rc
            else : 
                self.log.info('Ran  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
            
            # PostCheck Every File, before updating flag:
            if self.postCheckFlg is True :
                rc = self._postCheck()
                if rc != 0 :
                    self.log.warn('Post Check Failed !!! Did not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
                    return rc
                
            # Loading Staging Succeeded. Update the control file.
            rc = fu.updFile(ctlFile,cur_dayr)               
            if rc == 0 :
                if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr,   ctlFile))
                else                   : self.log.info('Overwriting Cur Load Date to  %s , Control File %s' % (cur_dayr,   ctlFile))
            else       : 
                self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
                return rc 
                            
            # r = fu.delFile(t) 
            i+=1  
        
        return rc
    
    # This method works on a sanity check in order to ensure load completeness.
    # Sometimes record count in a file does not match the number of records with proc_dt in DB,
    # This is valid, since some times incoming records will be an update of a previous existing one.
    def _postCheck(self):
        rc = 0
                
        self.ib.schDayOff = 0   # Overwrite from config, since we are using the file name to seed the procdate.
                                # For files created on sunday is filename - 1 day = procdate.
        r = self.getDlyCnt()     
        
        lineCnt = self.srcCount['Ap_scuaphis.txt']
        if r == 0 and self.RowCnt > 0 :
            r = lineCnt - self.RowCnt                
            self.log.info('File Date = %s File LineCnt = %d\tDB RowCnt = %d rc = %s' % (self.fileDate,lineCnt,self.RowCnt,rc))
            if r != 0:
                self.log.warn('Load Date = %s does not match Lines - Rows = %d ' % (self.fileDate,lineCnt - self.RowCnt))
            else : self.log.info('Rec Number from src file matches Rec Number loaded to DB')

        else:    
            self.log.error('Error validating Records - DlyCnt: Rows loaded to DB %s' % self.RowCnt)
            rc = 3

        return rc 
    
    # This method should run standalone for verification/shout process, 
    # If running standalone make sure that it is sched after the actual main run, otherwise will fail !
    # If data has not been loaded, will send 1 otherwise 0.
    # self.ib.schDayOff is the number of day(s) diff between the actual processdt and script verification running time.
    # Since fileDate == '' , su.getDayofWeek will get current day !
    # Sets self.RowCnt with DB Count for day.
    # Beware that for files that are produced on Sundays evening filename (date - 1) is the process date, as oppose to the rest of the week.
    # For Sundays filedates we need to subtract 2 full runs, sinc ejobs do not run on Sundays. 
    # For verification only runs we need to check for Mondays, assumign that runs are started follwoing days that the files are produced
    # Format self.fileDate = '07/8/2012'
    
    def getDlyCnt(self):
        rc = 1
        
        dd = su.toInt(self.ib.schDayOff)
        if dd is not None :
            dow = su.getDayofWeek(self.fileDate)
            if self.fileDate == '' :                  # Verification only. Need to be based on run date.
                if dow == 'Mon':                       # Runs on Monday. (2 days lag for proc_dt)
                    dd = 2
                    self.log.info('Monday Run %s offset = %d' % (self.fileDate,dd))
            else                   :                  # It is coming from a full run, since self.fileDate is defined from file name.
                if dow == 'Sun':
                    dd = 1
                    self.log.info('Sunday Created file %s offset = %d' % (self.fileDate,dd))

            self.log.info('Schedule Day Offset = %d' % (dd))


        else:
            self.log.error("Schedule Day Offset = %s " % self.ib.schDayOff)
            return rc

        dt =  su.getDayMinusStr(dd,self.fileDate)
        rs =self._getNZDS(da.selInvAppDtlLoadDtQry % dt)
        self.log.debug('date=', dt,'rs=', rs)
        if rs is not None and len(rs) > 0:
            cnt = su.toInt(rs[0][0])
            if cnt is not None and cnt > 1:
                self.log.info('Count for %s = %d' % (dt,cnt))
                self.RowCnt = cnt
                rc = 0
            else: self.log.error("Count for %s = %s " % (dt, cnt))
            
        else: self.log.error('rs error : ',  rs)
        
        return rc
        
def main(Args):
    a = InvAppDtl()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
