'''
Created on May 2, 2013

@author: eocampo

Main driver for Lease Credit.

This new application will allow code to be re-runable and will process n number of files automatically !.

The following files are => control files :        CTRL_BUREGION.txt,
                                                  CTRL_CORTERA.txt
                        => list files to process  Cortera_Trx_Extract.txt.             

'''

__version__ = '20130502'

import sys
import os
#import time
from datetime import datetime
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp
from infbaseutil      import _LeaseCreditFile

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

RUN_PER_DAY =  1  # Daily runs.
DP_LEN      = len('YYYYMMDD')  
   
class LeaseCredit(_InfaBaseApp,_LeaseCreditFile):  
    exitOnError = True
    
    def __init__(self):
        super(LeaseCredit,self).__init__()
        self.landDir    = 'SrcFiles/lease_credit'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
        
        self.RowCnt     = -1
        self.ib.fld     = 'Lease_Credit'
        self.ib.wkf     = 'wkf_LEASE_CREDIT_PROCESS'    
        self.ib.srcFile = ( 'Bu_region.csv','Cortera_Trx_Extract.txt')    # File that Informatica expects. Alphabetical.
        #self.ib.srcFile = ('Bu_region.csv,Cortera_Trx_Extract.txt,CTRL_BUREGION.txt,CTRL_CORTERA.txt')  # Sorted Mandatory Files that Informatica expects.
             
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 4   
        self.procCort  = {}
        self.procBuReg = {}
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock          ,
                         'B' : self.getLCRftpFiles   ,  # Sets self.incFileSet
                         'C' : self.getIncSetFiles   ,  # Populates self.incSetFiles. Incoming Files.
                         'D' : self.chkNextRunFlg    ,   
                         'E' : self.procCtlFiles     ,           
                         'F' : self.procBuRegFile    ,     
                         'G' : self.procCortFiles    ,
                         #'H' : self.postTask         ,                 
                         'T' : self.updLeaseCrdTbl   , # Test reserved for Test stub
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
        
        # FTP is expecting the following env variables, which should not be in a config file.
        os.environ['FILE'     ] =  ('Bu_region.csv,Cortera_Trx_Extract.txt,CTRL_BUREGION.txt,CTRL_CORTERA.txt')  # Should match, this are expected files. Mosty control except for Bu_region.csv.
        os.environ['RXFILE'   ] =  r'Cortera_Trx_Extract_[0-9]*.txt'   # data file(s)  which depends on the number of files specified by Cortera_Trx_Extract.txt.
         
    def _getNextRunDate(self,pd):
        mdate   = datetime.strptime(pd,'%Y%m%d')
        dayOfWk = mdate.weekday()
        nxtDay  = 1    
        nrd = su.getDayPlusStr(nxtDay,pd,'%Y%m%d')
        self.log.debug('pd=%s dayOfWk=%s nxtDay=%s Next Run Date (nrd) =%s ' % (pd,su.DL[dayOfWk],nxtDay,nrd))
        
        return nrd
        
    # Test method only   
    def updLeaseCrdTbl(self):
        #pdate = '2012-09-29'  In the past
        pdate = '2013-05-07'
        rc = self._updLeaseCrdTbl(pdate)
        print "_updLeaseCrdTbl(pdate) RC = %s" % rc
        return rc
         
 
    # This method updates Lease Credit Ctrl table
    # pdate process date from data file (format 'MM-DD-YYYY').  
    # Due to current logic, data manipulation is done based on current_date as opposed to process date.
    # In the event that process date from file is different that current_date (when actual load is happening), then:
    # load_dt on  lcr_stg_cortera_extract needs to be update to process date.
    def _updLeaseCrdTbl(self,pdate):
        
        if pdate is None or len(pdate) != 10 : 
            self.log.debug('Invalid  pdate %s ' % (pdate))
            return 1  
        cdate = su.getTodayDtStr(fmt='%Y-%m-%d')
        if cdate == pdate:
            self.log.debug('curr_date %s = pdate %s. No need to update table.' % (cdate,pdate))
            return 0
        
        self.log.info("Updating table setting curr_date %s to pdate = %s " % (cdate, pdate))
        rc =self._getNZDS(da.updLCRStgCortExtQry % (pdate,cdate) , po='UPD')
        self.log.info('Update Qry returned %s' % rc)
        if rc > 0 : return 0
        
        return 2
    # This method reads the Lease Credit control file, removes any blank lines.
    # : FileName.txt,2013-05-02 05:40:01,130,0
    # Use only field 1 and field 3 
    # Returns a dictionary of the form : filename : {number of lines (3rd) field}
    def _procLeaseCredCtlFile(self,fn):
        procFiles = {}
        lines = fu.remBlankLineLst(fn)
        if  len(lines) < 1 : 
            self.log.error("remBlankLineLst Error for File %s" % fn)
            return procFiles
        i = 1
        for line in lines:
            ln = line.split(',')
            if len(ln) != 4 :
                self.log.error("File %s Invalid line No" % (fn,i))
                i+=1
                continue
            
            lc = su.toIntPos(ln[2])
            if lc < 1 :
                self.log.error("File %s line No %d Invalid record Count %s " % (fn,i,lc))
                i+=1
                continue
            
            # Take smallest file size len : Bu_region.csv
            if len(ln[0]) <  13:
                self.log.error("File %s line No %d Invalid File Size Name %s " % (fn,i,ln[0]))
                i+=1
                continue
                   
            procFiles[ln[0]] = lc
            self.log.debug("fn = %s line# = %d key = %s lc = %s" % (fn,i,ln[0],lc))
            i+=1
            
        return procFiles
        
    # Sets a flag, to check for next run.
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0    
    
    # Checks that number of lines in ctl matches the data file count.
    # Checks for Cortera and BU.
    # fn  -> File Name 
    # fr  -> Dict containing file.
    def _chkDataRecCnt(self,fnp,cl):
        
        tl = fu.getLines(fnp)
        rc = tl - cl
        if rc != 0 :
            self.log.error('Count on File = %d Count on Ctl File = %d Does not match diff = %d' % (tl,cl,rc))        
        return rc
                
    # Get the files from the remote host (source system).
    # Placed in the self.landDir in local host
    # Move files in remote host from . to inprocess/processed dir.
    def getLCRftpFiles(self):
        rxFiles = []       # List which contains the new file names.
        ftp = ft.Ftp("LeaseCredit",self.log)
        rc  = ftp.connect() 
        self.log.info("Connect rc = %s" % rc)
        if rc != 0 : return rc
        rc  = ftp.login() 
        self.log.info("Login  rc = %s" % rc)
        if rc != 0 : return rc
        ftp.ftpSetPasv(False)
        self.log.info("Setting to Active Mode")
        rc = ftp.ftpGet()
        self.log.info("Get Control Files rc = %s " % rc)
        if rc != 0 : return rc
        rc = ftp.ftpMGet()
        if rc != 0 : return rc
        self.log.info("MGet Data Files rc = %s " % rc)
        txFiles = ftp.getTXFiles()
        self.log.debug("TXF Files " , txFiles)
        # Build a list of tuples w src/tgt file names.
        for sfn in txFiles:
            tfn = 'inprocess/processed/%s%s' % (self.ts,sfn)
            self.log.debug('Adding to list to rename in remote host  %s to %s' %(sfn,tfn))
            rxFiles.append((sfn,tfn))
        
        rc = ftp.ftpRenFile(rxFiles)
        self.log.info("Rename r = %s " % rc)
        ftp.disconnect() 
        self.log.info("Disconnecting = %s" % rc)
        return rc
  
    # Workflow execution.
    def wkfLeaseCredProc(self):
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    # Get complete number of fileset to process. 
    # The control Files can have one or more file(s). One line per file.
    # There are 3 Control Files:  
    #   CTRL_BUREGION.txt       : Bu_region.csv,2013-05-02 05:40:01,32,0
    #   CTRL_CORTERA.txt        : Cortera_Trx_Extract_20130502.txt,2013-05-02 05:40:01,130,0
    #   Cortera_Trx_Extract.txt : Cortera_Trx_Extract_20130502.txt (Not needed)
    # Process Control Files.
    # If successful moves from landing dir to archive directory (ctl files only).
    # SIDE EFFECT : assigns self.procCort and self.procBuReg, that contains {filename:size}
    def procCtlFiles(self):
        
        # 'B' : self.getIncSetFiles, populates self.incFileSet, which is [filename][sets] 
        # self.ib.landir = C:/apps/infa_share/SrcFiles/lease_credit
        # each element in getIncSetFiles = 'C:/apps/infa_share/SrcFiles/lease_credit/Bu_region.csv'
        ctlFiles   = []    # Control Files full path.
        
        self.log.debug("self.incFileSet ",  self.incFileSet  , " len = " , len (self.incFileSet) )
        if  self.FILE_SET_LEN >= len(self.incFileSet): 
            self.log.error("Invalid Len for incFileSet = %d. " % len(self.incFileSet))
            return 1
     
        # Cortera TXT (Not used)
        cortTxt = '%s/%s' % (self.ib.landDir,'Cortera_Trx_Extract.txt')
        ctlFiles.append(cortTxt) 
        
        # Cortera Control
        cortFile = '%s/%s' % (self.ib.landDir,'CTRL_CORTERA.txt')
        self.procCort = self._procLeaseCredCtlFile(cortFile)
        # Move file to archive with TS
        if len(self.procCort) < 1:
            self.log.error('Nothing to Process. Please check file %s Validity!' % cortFile)
            return 2
        self.log.debug('file %s dict %s' %  (cortFile,self.procCort))
        ctlFiles.append(cortFile)
       
        # BU Region Control
        buregFile = '%s/%s' % (self.ib.landDir,'CTRL_BUREGION.txt')
        self.procBuReg = self._procLeaseCredCtlFile(buregFile)
        if len(self.procBuReg) < 1:
            self.log.error('Nothing to Process. Please check file %s Validity!' % buregFile)
            return 3
        # Based on current rules, 
        ctlFiles.append(buregFile)
        self.log.debug('file %s dict %s' %  (buregFile ,self.procBuReg))  

        rc = self.archGenFiles(ctlFiles,  self.ts)
        self.log.info('archCtlFiles rc = %s' %  rc)  
        return 0   


    # This method process BU region data file. 
    def procBuRegFile(self):
 
        butn = len(self.procBuReg)      
        if butn > 1:
            self.log.warn('BU_REGION ctl file records are %d. Should be one file only !' % butn)
        btfs =  self.procBuReg.keys()     
        btfs.sort()
        self.log.debug('Bu region files = ' , btfs)
        fn  =  self.ib.srcFile[0] 
        t   = '%s/%s' % (self.ib.workDir,fn)
        fnp = '%s/%s' % (self.ib.landDir,fn)
        
        cl = self.procBuReg[fn]
            
        rc = self._chkDataRecCnt(fnp,cl)
        if rc != 0 : return rc
        
        rc = fu.copyFile(fnp, t)
        if rc != 0 : 
            self.log.error('Could not copy File %s to %s' % (fnp,t))
            return 5
        
        # Archive the file
        rc = self.archGenFiles([fnp,], self.ts)                           
        self.log.info ('Archiving file rc =  %s' % rc)
  
        return 0
            
    # This method is the main driver,           
    # There are 2 or more data files, which depends on the control above.
    #   Bu_region.csv                     : This file gets overwritten does not have a time stamp. No iteration is needed, since it overwrites at the source system.
    #   Cortera_Trx_Extract_20130502.txt  : This file gets a timestamp so will need to iterate, based on number specified in CTRL_CORTERA.txt
    # 2- Will Copy file dataset into working dir
    # 3- Archive file set.
    # 4- Check #of records in the data set.
    # 5- Will rename files for wkf
    # 6- Invoke workflows.
    # 
    def procCortFiles(self):
            
        dataFile = []    
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)     
        
        setn  = len (self.procCort)     
        self.log.info( ' ---- Starting Processing. Total of  %d iteration(s) ----' % setn)            
        fileset = self.procCort.keys()
        fileset.sort()
        self.log.debug('Process Order = ' , fileset)

        for fn in fileset:
            f = '%s/%s' % (self.ib.landDir,fn)
            if not fu.fileExists(f):
                 self.log.error('File %s have not been downloaded!' % f)
                 return 1
       
        # Processing loop. 
        for fn in fileset:
            self.log.info( 'Processing file %s/%s ' % (self.ib.landDir,fn))
            cur_dayr = self._getDateRunStr(fn)
            if cur_dayr is None : 
                self.log.error('cur_dayr is None. No Date Run String ') 
                return 2
    
            # cur_dayr = YYYYMMDD from filename . eg. Cortera_Trx_Extract_20130502.txt
            # pdate format YYYY-MM-DD
            pdate = '%s-%s-%s' % (cur_dayr[:4],cur_dayr[4:6],cur_dayr[6:8])
           
            self.log.debug('cur_dayr = %s pdate = %s' % (cur_dayr,pdate))     
            rc = su.isValidDate(pdate,'%Y-%m-%d')
            if rc is False :
                self.log.error('Invalid Date %s on file %s ' % (pdate,fn))
                return 2
   

            self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
            if self.checkNextRunFlg is True:
                prev_dayr = self._getCtlFile()
                pd,pr     = self._getDay(prev_dayr,DP_LEN)
                rc        = self._chkNextRun(cur_dayr,prev_dayr,pd,pr,RUN_PER_DAY)
                if rc != 0 : 
                    self.log.error("self._chkNextRun rc = %s" % rc)
                    return rc

            # Cortera
            t   = '%s/%s' % (self.ib.workDir,self.ib.srcFile[1])
            fnp = '%s/%s' % (self.ib.landDir,fn)
            cl = self.procCort[fn]
            
            rc = self._chkDataRecCnt(fnp,cl)
            if rc != 0 : return rc
          
            rc = fu.copyFile(fnp, t)
            if rc != 0 : 
                self.log.error('Could not copy File %s to %s' % (fnp,t))
                return 5
            self.log.info ('Copying File from %s to %s' % (fnp,t))
 
            rc = self.archGenFiles([fnp,], self.ts)                           
            self.log.info ('Archiving file rc =  %s' % rc)
 
            # Invoke workflow.   
            rc = pi.runWkflWait(self.ib,self.log)  
            if rc != 0 : 
                self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
                if self.exitOnError: 
                    self.log.debug('ExitOnError is TRUE rc = %s' % (rc))
                    return rc
            else : 
                self.log.info('Ran  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
            
            # Update Control table in Netezza with date.  
            rc = self._updLeaseCrdTbl(pdate)
            if rc != 0 :
                self.log.error('No rows were updated for process date %s' % pdate)
                return rc
                 
            # Loading Staging Succeeded. Update the control file.
            rc = fu.updFile(ctlFile,cur_dayr)               
            if rc == 0 :
                if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr,   ctlFile))
                else                   : self.log.info('Overwriting Cur Load Date to  %s , Control File %s' % (cur_dayr,   ctlFile))
            else       : 
                self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
                return rc 
            
                     
            r = fu.delFile(t) 
            self.log.info('Removing %s rc = %s ' % (t,r))    
        
        # Delete BU Region. 
        fnp = '%s/%s' % (self.ib.workDir,self.ib.srcFile[0])    # BU_region       
        rc = fu.delFile(fnp)   
        self.log.info('Removing %s rc = %s ' % (fnp,rc))         
                  
        return rc
  
        
def main(Args):
    a = LeaseCredit()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
        
