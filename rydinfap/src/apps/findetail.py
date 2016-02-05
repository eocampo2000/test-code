'''
Created on Sep 5, 2012

@author: eocampo

This code will execute fin_detail logic.
Program can handle multiple filesets and it will process the filesets in order.
20130520 New Style class
20131114Added None check for rs

'''
__version__ = '20131114'

import sys
#import time
from datetime import datetime
import procjobs.procsched as psc
import utils.fileutils   as fu
import utils.strutils    as su
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp  import _InfaBaseApp
from infbaseutil import _WalkerFlatFile
# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

RUN_PER_DAY = 1     # Daily runs.
DP_LEN      = len('YYYYMMDD')  


SCH_FREQ = 'Cust'
sch = ('Mon','Tue','Wed','Thu','Fri','Sat')

cur_dayr   = su.getTodayDtStr('%Y%m%d')
   
class FinDetail(_InfaBaseApp,_WalkerFlatFile):  
    
    exitOnError = True

    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(FinDetail,self).__init__()
        self.landDir    = 'SrcFiles/finance'
        self.incFiles   = []                  # Incoming Files. Contains full path name.
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles). 
        self.badFiles   = []                  # Files with Errors. (trailer) 
        self.incFileSet = []                  # Holds set of files. Populated using getIncSetFiles. List with all pertinent files.
        
        self.ib.srcFile = ('Ap_sce5100.txt', 'Ap_scg4000.txt', 'Ap_scg4100.txt', 'Ap_scudrldn.txt', 'Ap_scusrctl.txt')  # Sorted Files that Informatica expects
     
        self.srcCount   = {  'Ap_sce5100.txt' : -1,         # Use to verify row counts from incoming files.
                             'Ap_scg4000.txt' : -1,
                             'Ap_scg4100.txt' : -1,
                             'Ap_scudrldn.txt': -1,
                             'Ap_scusrctl.txt': -1,
                          }

        self.ctlTbl    = {   'Ap_sce5100.txt' : 'SCE5100',   # Use to verify row counts from populated tables.
                             'Ap_scg4000.txt' : 'SCG4000',
                             'Ap_scg4100.txt' : 'SCG4100',
                             'Ap_scudrldn.txt': 'SCUDRLDN',
                             'Ap_scusrctl.txt': '',
                          }

        self.FILE_SET_LEN = len(self.ib.srcFile)
        
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getIncSetFiles  ,   # Sets self.incFileSet
                         'C' : self.chkNextRunFlg    ,  # To verify if no cycle is missed. 
                         'D' : self.rWkfFlowFlg      ,  # To execute wkfl.
                         'F' : self.procIncFiles     ,
                         'Z' : self.postCheck        ,
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
    
    # Sets a flag, to Run workflows.
    def rWkfFlowFlg(self) : 
        self.runWkfFlowFlg  = True
        return 0
    
    # This method gets the next schedule run date
    # pd is prev run date, from storage.
    
    def _getNextRunDate(self,pd):
        mdate   = datetime.strptime(pd,'%Y%m%d')
        dayOfWk = mdate.weekday()
        nxtDay  = 1
        
        if dayOfWk == 5:  nxtDay = 2  # Saturday.
        
        nrd = su.getDayPlusStr(nxtDay,pd,'%Y%m%d')
        self.log.debug('pd=%s dayOfWk=%s nxtDay=%s Next Run Date (nrd) =%s ' % (pd,su.DL[dayOfWk],nxtDay,nrd))
        
        return nrd
             
    # Run to populate staging.
    def wFinDetailStg(self):
        self.ib.fld     = 'Financial'
        self.ib.wkf     = 'wkf_FIN_DETAIL_STAGE'
        #self.log.info('UNCOMMENT ->  pi.runWkflWaitStg, self.ib = %s' % self.ib)
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wFinDetail(self):
        self.ib.fld     = 'Financial'
        self.ib.wkf     = 'wkf_FIN_DETAIL'
        #self.log.info('UNCOMMENT ->  pi.runWkflWait, self.ib = %s ' % self.ib)
        #rc=0 # TODO Remove
        rc = pi.runWkflWait(self.ib,self.log) 
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
             
    # This method is the main driver, 
    # 1- It will check incoming source file sets for completion. 
    # 2- Will Copy file dataset into working dir
    # 3- Archive file set.
    # 4- Check trailers in the data set.
    # 5- Will rename files for wkf
    # 6- Invoke workflows. (rWkfFlowFlg is TRUE)
    # 7- Update control file. (rWkfFlowFlg needs to be TRUE)
    
    def procIncFiles(self):

        # 'B' : self.getIncSetFiles, populates self.incFileSet, which is [filename][sets]  
        if len(self.incFileSet) != self.FILE_SET_LEN:
            self.log.error("Invalid Len for incFileSet = %d " % len(self.incFileSet))
            return 1
        
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)
        
        i = 0
        # Get complete number of fileset to process. It is an Array of 5 elem.
        # Each element is a bucket containing a list of similar files (n numebr of runs)  
        # FileSet[i] is a bucket that contains file(s) for the same table.
        # FileSet[i] is already sorted.
        # e.g.
        #[ $PATH:/Ap_sce5100_20120811_3.txt'
        #  $PATH:/Ap_sce5100_20120812_1.txt'
        #  $PATH:/Ap_sce5100_20120812_2.txt'
        #  $PATH:/Ap_sce5100_20120813_1.txt']           
        self.log.debug("self.incFileSet ",  self.incFileSet  , " len = " , len (self.incFileSet) )
  
        # Get the minimun len from each file(s) with the same pattern (self.incFileSet). It will tell us how many complete FS we have.
        setn = min(len(self.incFileSet[0]), len(self.incFileSet[1]),len(self.incFileSet[2]),len(self.incFileSet[3]),len(self.incFileSet[4]))
        self.log.info( ' ---- Starting Processing. Total of  %d iteration(s) ----' % setn)
        
        while  i < setn:
            
            self.incFiles = self.incFileSet[0][i], self.incFileSet[1][i],self.incFileSet[2][i],self.incFileSet[3][i],self.incFileSet[4][i]
            self.log.debug(' iter= ', i, '\tincFiles =', self.incFiles)
            
            i+=1
            
            if  len (self.incFiles) != self.FILE_SET_LEN : 
                self.log.error('Invalid FileSet len = %d should be = %d' % (len (self.incFiles),self.FILE_SET_LEN)) 
                return 1
            
            # Get date run  from 1st filename
            fn    = fu.getFileBaseName(self.incFiles[0])
            cur_dayr = self._getDateRunStr(fn)
            
            if cur_dayr is None : 
                self.log.error('No Date Run String %s ' % cur_dayr) 
                return 1
            
            rc = self.chkCompleteSet(cur_dayr,self.incFiles)
            if rc != 0 : 
                self.log.error("chkCompleteSet() rc = %s" % rc)
                return rc  
            
            self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
            if self.checkNextRunFlg is True:
                
                # Get Previous Run Info. File should contain one line only :  YYYYMMDD_R from storage.
                prev_dayr = self._getCtlFile()
                #pd,pr    = self._getDay(prev_dayr,DP_LEN)
                
                if prev_dayr is None or prev_dayr.strip() == '':
                    self.log.error("Could not find control file or No Data")
                    return -1

                #rc = self._chkNextRun(cur_dayr,prev_dayr,pd,pr,RUN_PER_DAY)
                rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log,sch)
                if rc != 0 : 
                    self.log.error("self._chkNextRun rc = %s" % rc)
                    return rc  
            
            rc = self.cpFileToWorkDir() 
            if rc != 0 : 
                self.log.error(" cpFileToWorkDir() rc = %s" % rc)
                return rc  
            
            rc = self.archFiles()
            if rc != 0 : 
                self.log.error(" archFiles() rc = %s" % rc)
                return rc 
            
            procFiles = self.chkTrailer(self.workFiles,fn,cur_dayr)
            if len(procFiles) != self.FILE_SET_LEN : 
                self.log.error("chkTrailer Files that were OK ",  procFiles)
                return 1
            
            # At this  point all files are valid for processing and filenames are sorted.                    
            for fnp in procFiles:
                fn = fu.getFileBaseName(fnp)
                f  = self._getFileProcName(fn)
                t  = '%s/%s' % (self.ib.workDir , f)
                rc = fu.moveFile(fnp, t)
                if rc == 0 :
                    self.log.info('Renaming File %s to %s' % (fnp,f))
                else: 
                    self.log.error('Could not rename File %s to %s' % (fnp,t))
                    continue
                        
            #rc = 0      # Remove after testing 
                  
            # Invoke workflow(s).    
            self.log.debug('self.runWkfFlowFlg is %s' %  self.runWkfFlowFlg)
            if self.runWkfFlowFlg == True:
                rc = self.wFinDetailStg()
                if rc != 0 : return rc 
                
                rc = self.wFinDetail()
                if rc != 0 : return rc 

                # End to End Loading Succeeded. Update the control file.
                rc = fu.updFile(ctlFile,cur_dayr)
                if rc == 0 : self.log.info('Updated Cur Load Date %s, Control File %s' % (cur_dayr,ctlFile))
                else       :
                    self.log.error('Could not Update Load Date %s, Control File rc = %s' % (cur_dayr,ctlFile,rc))
                    return rc
                    
            #r = fu.delFile(t)
            
        return rc   

    # This method works on a sanity check in order to ensure load completeness.
    # Also checks inserted rows in the database for the given run.
    # rcnt  flat file row count.
    # dbcnt db cnt table row count.
    def postCheck(self):
        rc = 0

        for fn,rcnt in self.srcCount.items():
            tblNm = self.ctlTbl[fn]
            if tblNm == '' : continue
            
            rs = self._getNZDS(da.selFinCntlRecCnt % (tblNm,tblNm))
            self.log.debug('tblNm=', tblNm ,'rs', rs)
            if rs is not None and len(rs) > 0:
                dbcnt = su.toInt(rs[0][0])
                self.log.debug ('File = %s Fcnt = %s Tbl = %s  Tcnt = %s' % (fn,rcnt,tblNm,dbcnt))
                          
                if dbcnt is not None and dbcnt >= 0:
                    if rcnt - dbcnt == 0 :
                        self.log.info('Count for %s  %d Matches' % (fn,rcnt))
                    else :
                        self.log.error('Count for %s = %d  Does Not Match Table %s = %d' % (fn,rcnt,tblNm,dbcnt))
                        rc+=1
                        
                else: 
                    self.log.error("Count for File %s = Table %s Mismatch" % (fn, tblNm))
                    rc+=1
                    
        return rc
    
        
def main(Args):
    a = FinDetail()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    
