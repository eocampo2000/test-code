'''
Created on Jun 10, 2012

@author: eocampo

This is a base class that encapsulates Infa App Modules.
It will implement methods that are common to all applications.
Following ENV VAR needs to point to share  dir : self.ib.dataDir = os.environ['INFA_SHARE']

from config : (sub-directories):

     ib.archDir =  ib.landDir/archive 
     ib.workDir =  ib.landDir/work 
            
Children (descendent) Classes will need to follow the paradigm:
1- setDataDir, override this function if you are not using data files.
2- Check for incoming file(s) or Incoming files(s) set(s)                    
3- When file(s) arrive, placing them in the working directory (original name) 
4- Archive will move files from landing to archive dir (Use with cpFileToWorkDir())


SIDE Effects : Sets the following instance variables  

1    setDataDir()   self.ib.landDir 
                    self.ib.archDir 
                    self.ib.workDir 
                    self.ib.badDir  

2    getIncFiles()      self.incFiles      Full path to Incoming Files in landing dir   fs = [,,]
2    getIncSetFiles()   self.incfileSet    Full path to Incoming Set Files in landing dir fs = [[,,],[,,],]
2    getTrigFiles()     self.trigFiles     Full path to Incoming Files in landing dir
3    cpFileToWorkDir()  self.workFiles     Full path to working dir (landing/input)
3    mvFileToWorkDir()  self.workFiles     Full path to working dir (landing/input)
4    archFiles()

Note : landing directory needs to be defined in derived class.
     Added new routines to handle trigger files.
     Removed workflow invocation, so its directly control by descendant class.
     New Style class
     
Created  dsetArgs(self,Argv) method, so children can overwrite and use n numbers of input paramters.
Added methods to support MF Fin_detail_run.
Added methods to support pwd encoding.
Added method to rename MF srcFiles.
Added Capability to override notification with RET_WARN
Added method to check trigger file modify date / RUN_DATE
20131007 Added Connector for MS SQL Native.
20131024 Added runSelQry ,  _checkPred  and checkPred stub. 
20131114 Modified DB related FX to accomodate changes on dbutil. Replaced  sys.exc_value with sys.exc_info()[1]
20131120 Added generic methods to manipulate filesets
20131213 Added Oracle driver.
20140104 Added method to support predecessors.
20140129 Added CheckFileCols method. Moved flag setter methods.
20140205 Added  rWkfFlowFlg method and crtTrigFile.
20140220 Replaced return for sys.exit
20140821 Added isLastWorkDayWarn
20140911 Modify existing workday logic to accept more then 1 wkdya for a given process.
10150112 Added method to set Env variables.
20150217 Added chkWkfDlyPred to check wkf dly predecessors. Also add handlers on getWorkDay methods for issues connecting to DB.
20150302 Added logic in _notifyAppUsers to cast to boolean.
20150530 Added generic Connect, Disconnnect DB methods.
20151006 Added remWorkFiles as step.
20151215 Added support for DB2 _getDB2CliDS

'''
__version__ = '20151215'

import os      #, os.path
import sys
import socket
import time


import common.log4py    as log4py 
import common.simpmail  as sm
import common.lockfile  as lck
import utils.fileutils  as fu
import utils.strutils   as su
import datastore.dbapp  as ds
import mainglob         as mg    # Do not remove.

from datastore.dbutil  import NZODBC, MSSQLODBC,MSSQLNat, DBOracle, getDSConnStr

from common.loghdl     import getLogHandler

RET_WARN = 101
RUN_DATE = su.getTodayDtStr(fmt='%Y%m%d')

# Empty Container   
class InfaBaseAppBean:
    pass

class _InfaBaseApp(object):
    '''
    classdocs
    '''
    # Set of diagnostics commands to run
    hostname = socket.gethostname()
    exitOnError = False   # default.
    
    def __init__(self):
        self.appName = self.__class__.__name__.lower()
        self.log = log4py.Logger().get_instance()       
        self.suf = 'cfg'
        self.ib           = InfaBaseAppBean
#       self.cmdStep    = {}                         # Empty for base class
        self.infaEnvVar   = {}                       # Empty for base class
        self.pLock        = None
        self.ib.fu        = 'infa@ryder.com'
        self.ib.touser    = None
        self.ib.pager     = None
        self.ib.pgOnErr   = 'False'                  # Need to be string by contract 
        self.ib.mailOnErr = 'False'                  # Need to be string by contract

        self.ib.xpwd_dbg   = False
        # pwd palceholders
        self.ib.rep_xpwd   = None
        self.ib.xpwd       = None
        self.ib.db_xpwd    = None
        self.ib.rep_dbxpwd = None
        self.ib.dom_dbxpwd = None
              
        self.runSeq = None 
        
        self.ib.FileColCnt = {}                     # Column verification
   
        # Flags
        self.checkNextRunFlg   = False
        self.runWkfFlowFlg     = False
        self.checkFileColsFlag = False
        self.checkFileRowsFlg  = False
        
        # Db handler
        self.dbh = None

    # Create Lock File for processes that could not have more than one instances running concurrently.   
    # Command Step.
    def getLock(self):
        self.lockFn = '%s/%s.lck' % (self.ib.lckDir, self.appName) 
        self.pLock = lck.LockFile(self.lockFn, self.log)
        rc = self.pLock.getLock()
        self.log.debug('Creating Lock: %s\t:rc = %s ' % (self.lockFn, rc))
        if rc is True : return 0
        else          : return 1
    
    #---------------------- File Operations (high level)  --------------------------------------
        
    # This method will create 0 bytes file(s). 
    # self.ib.crtFiles list w/Full path for files that need to be created.
    def _touchFiles(self):
        
        if len(self.ib.crtFiles) < 1 : 
            self.log.error('List to create files is empty !')
            return 1
        
        rc = 0
        for fn in self.ib.crtFiles:
            r = fu.crtEmpFile(fn)    
            if r == False :  
                self.log.error('Could not create file %s' % fn)
                rc += 1
            else          : self.log.info('Created file %s ' % fn)
        return rc
    
    # Check if the file is not growing !
    def _isFileStable(self, fn):
        # We know that file exist
        r = fu.chkFileModTime(fn)
        rc = (float(r) > float(self.ib.stableFile))
        self.log.debug('chkFileModTime r = %s > %s \trc=%s' % (r, self.ib.stableFile, rc))
        if rc is True: self.log.info('File %s is stable ' % (fn))
        else         : self.log.warn('File %s is Not stable.\n\tWill Sleep for %s secs' % (fn, self.ib.stableFile))

        if rc is False:
            i = 1
            wi = int(self.ib.stbFileIter)
            while (i <= wi):
                time.sleep(float(self.ib.stableFile))
                r = fu.chkFileModTime(fn)
                rc = (float(r) > float(self.ib.stableFile))
                self.log.debug('chkFileModTime r = %s > %s \trc=%s' % (r, self.ib.stableFile, rc))
                self.log.info('Iteration %d out of %d' % (i, wi))
                if rc is True: self.log.info('File %s is stable ' % (fn))
                else         : self.log.warn('File %s is Not stable.\n\tWill Sleep for %s secs' % (fn, self.ib.stableFile))

                if rc is True : break
                else          : i += 1
        
        self.log.info('rc = ', rc)
        if rc is False: return 1 
    
        return rc 
    
    def _chkInfaRdyDly(self,job):
        rs = self._chkInfaRdy(job)
        if rs is None or len(rs) != 1 : return -1
        dt = rs[0][0]

        todm  = su.getTodayDtStr('%Y%m%d')
        self.log.debug('dt = %s todm = %s' % (dt,todm))
        if  dt == todm : return 0
        return 1

    def _chkInfaRdyMthly(self,job):
        rs = self._chkInfaRdy(job)
        if rs is None or len(rs) != 1 : return -1
        d = rs[0][0]; dt = d[:-2]
        todm  = su.getTodayDtStr('%Y%m')
        self.log.debug('d = %s dt = %s todm = %s' % (d,dt,todm))
        if  dt == todm : return 0
        return 1
    
    # Check for Predecessor. Invoke this method only if child class overwrites chkPred Method.
    # Query is the table to query.
    # Job = 'SCH - 8th Workday - Tech Ratio Data Load'
    def _chkInfaRdy(self,job):
        self.ib.user = self.ib.ora_user; self.ib.pwd = self.ib.ora_pwd; self.ib.db = self.ib.ora_db; 
        qryStr = ds.selInfPredDlyQry % job
        self.log.debug('qryStr = %s' % qryStr)        
        rs = self._getOracleDS(qryStr)
        self.log.debug("rs = ",rs)
        return rs
     
    # Invoking Method : qryStr = ds.selMssqlPredDlyQry % job
    # Use this to get status from MS SQL Metadata  msdb.dbo.sysjobhistory    
    def chkMssqlRdyDly(self,job):
        qryStr = ds.selMssqlPredQry % job
        self.log.debug('qryStr = %s' % qryStr)
        rs = self._chkMssqlRdy(qryStr)
        if rs is None or len(rs) != 1 : return -1
        dt = rs[0][0]
        todm  = su.getTodayDtStr('%Y%m%d')
        self.log.debug('dt = %s todm = %s' % (dt,todm))
        if  dt == todm : return 0
        return 1
    # Invoking Method qryStr = ds.selMssqlPredDlyQry % job
    #Use this to get status from MS SQL Metadata  msdb.dbo.sysjobhistory 
    def chkMssqlRdyMthly(self,job):
        qryStr = ds.selMssqlPredQry % job
        self.log.debug('qryStr = %s' % qryStr)
        rs = self._chkMssqlRdy(qryStr)
        if rs is None or len(rs) != 1  : return -1
        d = rs[0][0]; dt = d[:-2]
        
        todm  = su.getTodayDtStr('%Y%m')
        self.log.debug('dt = %s todm = %s' % (dt,todm))
        if  dt == todm : return 0
        return 1
    
    
    def _chkMssqlRdy(self,qryStr):
        self.ib.user = self.ib.ms_user; self.ib.pwd = self.ib.ms_pwd;  self.ib.dbserver = self.ib.ms_dbserver ; self.ib.db = self.ib.ms_db   
        rs = self._getMSSQLNatDS(qryStr)
        self.log.debug('rs = ', rs)
        return rs
   
    # This method checks specifically for workflows that run daily.
    # prdLst = self.ib.infadlypred contains list 
    # prdLst workflowList . self.ib.infaFREQpred where FREQ = Dly, Wkly or Mthly
    # freq. Run freq : Dly, Wkly or Mthly
    def _chkWkfPred(self, prdLst, freq):  
        if len(prdLst) < 1 : 
            self.log.error("No predecessors to execute !")
            return 1        
        
        wnl  = prdLst.split(',')
        meth = 'self._chkInfaRdy%s(wkf)' % freq
        self.log.debug('Method = ',  meth, '\tPredecessor list : ', prdLst, "\tsplit : ", wnl)
        for w in wnl:
            wkf = w.strip()
            if len(wkf)  < 1 : continue
            #rc = self.chkInfaRdyDly(wkf)
            rc = eval(meth)
            if rc == 0 :
                self.log.info('Predecessor %s done.' % wkf)
                continue
            else :
                i = 1
                wi = int(self.ib.waitPredIter)
                while (i <= wi):
                    self.log.info('Iteration %d out of %d. Will wait for %s secs' % (i, wi,self.ib.waitPred))
                    time.sleep(float(self.ib.waitPred))
                    #rc = self.chkInfaRdyDly(wkf)
                    rc = eval(meth)
                    if rc == 0 : 
                        self.log.info('Predecessor %s done.' % wkf)
                        break
                    else      : i += 1  
                
                if rc != 0 : 
                    self.log.error("Predecessor %s was not met." % wkf)
                    return rc                     
        return rc
           
    def chkPred(self,pmet):
        
        if len(pmet) < 1 : 
            self.log.error("No predecessors to execute !")
            return 1
            
        self.log.debug('Predecessor list', pmet)
        for p in pmet:
            
            rc = eval('self.%s' % p)
            self.log.debug("%s rc = %s" % (p,rc))
            if rc == 0: 
                self.log.info('Predecessor %s completed.' % p)
                continue
            
            else:
                i = 1
                wi = int(self.ib.waitPredIter)
                while (i <= wi):
                    self.log.info('Iteration %d out of %d. Will wait for %s secs' % (i, wi,self.ib.waitPred))
                    time.sleep(float(self.ib.waitPred))
                    rc = eval('self.%s' % p)
                    self.log.debug(' %s\trc=%s' % (p,rc))
                    if rc == 0 : 
                        self.log.info('Predecessor %s completed.' % p)
                        break
                    else      : i += 1          
                
                if rc != 0 : 
                    self.log.error("Predecessor %s was not met." %  p)
                    return rc 
        return 0
    
    # File list needs to include the absolute path.
    # For this Use a list of files
    # fls List of files w/full path.
    # If all files are not present it returns a 1, otherwise a 0.
    def _chkFiles(self, fls):
        rc = 0
        
        if len(fls) < 1 : return 1
        
        for fnp in fls:
            if fu.fileExists(fnp):
                if fu.isDir(fnp):
                    self.log.error('%s is a directory' % fnp)
                    rc += 1
                else:
                    self.log.info('%s exists' % fnp)
            else : 
                self.log.error('%s does not exist' % fnp)
                rc += 1
                
        return rc

    # Use to check set file completion ! 
    # Command Step.
    def chkFiles(self):
        rc = self._chkFiles(self.ib.chkFiles)
        self.log.debug('file list ', self.ib.chkFiles, 'rc = ' , rc)  
        return rc
    
    # Command Step.
    # Check incoming trigger(s) file. This method does not check for file stability.
    # Use if trigger file is 0 bytes, otherwise consider to use checkIncFiles.
    # Fl is a string of the form 'fil1,file2, ...filen' w filenames.
    def chkTrigFiles(self, fls):
        filesInc = []
        
        # Check if complete
        if len(fls) < 1 : 
            self.log.error('Trigger File list to verify is empty !')
            return filesInc
        
        # Create the full path to send for check.
        fns = fls.split(',')
        for fn in fns:
            f = '%s/%s' % (self.ib.landDir, fn)
            filesInc.append(f)
        
        self.log.debug('Trigger Files to check', filesInc)
            
        i = 1
        wi = int(self.ib.waitFileIter)
                
        while (i <= wi):           
            rc = self._chkFiles(filesInc)
            if rc != 0 :
                self.log.info('Will check for trigger files in ', self.ib.waitFile, ' secs.')
                time.sleep(float(self.ib.waitFile))
                self.log.info('Iteration %d out of %d ' % (i, wi))
                i += 1
                
            elif rc == 0 :      # All Files are present.
                self.log.debug('Trigger Files: ' , filesInc)
                return filesInc
                 
        return []
    
    # This method checks if all trigger files are present and modified on runtime date. Otherwise might be an older trigger file !
    # Should iterate and wait just in case the new trigger file arrives !
    def chkTrigFilesDate(self,fls):
        filesInc = []

        # Check if complete
        if len(fls) < 1 : 
            self.log.error('Trigger File list to verify is empty !')
            return filesInc
        
        # Create the full path to send for check.
        fns = fls.split(',')
        for fn in fns:
            f = '%s/%s' % (self.ib.landDir, fn)
            filesInc.append(f)
        
        self.log.debug('Trigger Files to check', filesInc)
            
        i = 1
        wi = int(self.ib.waitFileIter)
                
        while (i <= wi):           
            rc = self._chkFiles(filesInc)
            if rc != 0 :
                self.log.info('Will check for trigger files in ', self.ib.waitFile, ' secs.')
            
            elif rc == 0 :   # All Files are present. Check for date.
                ct = 0
                self.log.debug('Trigger Files: ' , self.trigFiles)
                for fn in filesInc:
                    dt = fu.chkFileModDate(fn,fmt='%Y%m%d')
                    self.log.debug('fn = %s dt = %s RUN_DATE = %s' % (fn,dt,RUN_DATE))
                    if dt != RUN_DATE :       # Check File Process date.
                        self.log.info("Filename %s has been modified on %s. Will Continue to Iterate " % (fn,dt))
                        ct = 0
                        continue
                    else:
                        ct+=1
                        if ct == len(filesInc) :
                            self.log.debug('ct = %d total files = %d' % (ct,len(self.trigFiles)))
                            return filesInc
                  
            time.sleep(float(self.ib.waitFile))
            self.log.info('Iteration %d out of %d ' % (i, wi))
            i += 1
                     
        return []

    # Note Need to ensure that getTrigFilesDate had been invoked first !
    def verTrigFileDate(self):
        global RUN_DATE
        rc = 0
        
        rdayoffset = su.toInt(self.ib.rdayoffset)
        if rdayoffset is None or rdayoffset < 0:
            self.log.error('rdayoffset needs to be a number => 0. Current Value = %s' % self.ib.rdayoffset)
            
        if rdayoffset != 0 :
            RUN_DATE = su.getDayMinusStr(rdayoffset, RUN_DATE, '%Y%m%d')

        for fn in self.trigFiles:
            fdt = fu.readFile(fn).strip(' \t\n\r')
            self.log.debug('fn = %s fdt = %s RUN_DATE=%s rdayoffset = %s ' % (fn,fdt,RUN_DATE, rdayoffset))
            if fdt != RUN_DATE:
                self.log.error("%s date field %s does not match the process RUN_DATE %s." % (fn,fdt,RUN_DATE))
                rc = 1
            else :
                self.log.info("%s date field %s matches the process RUN_DATE %s." % (fn,fdt,RUN_DATE))
        return rc

    # check incoming file(s)
    # values need to be in config file.
    # return a list of stable files to process. 
    # Use only for data files. For Trigger File(s) use chkTrigFiles
    # This method  will find  one or more files at a given time based on filename wildcards 
    # e.g.  Ap_scusrctl_*[0-9]_*[1-9].txt -> Ap_scusrctl_20120812_1.txt,Ap_scusrctl_20120812_2.txt, etc
    # If you need TO ENFORCE a set of files need to do a chkFiles first ! 
    # This method will look for any file name in a pattern ( fu.getFileName ) and will capture 0 or more to process.
    def checkIncFiles(self, fn):
        filesInc = [] # Incoming file
        fl = fu.getFileName(self.ib.landDir, fn)
       
        if len(fl) == 0:
            i = 1
            self.log.info('File %s/%s does not exists. Will start in %s sec' % (self.ib.landDir, fn, self.ib.waitFile))
            wi = int(self.ib.waitFileIter)
            while (i <= wi):       
                time.sleep(float(self.ib.waitFile))
                fl = fu.getFileName(self.ib.landDir, fn)
                self.log.info('Iteration %d out of %d ' % (i, wi))
                
                if len(fl) > 0 : break
                else           : i += 1
                
        if len(fl) == 0 : return filesInc
        
        self.log.info('File(s) found ', fl)
        
        for f in fl: 
             
            rc = self._isFileStable(f)
            if rc is True : filesInc.append(f)
         
        return filesInc

    # This is a wrapper function so if file is not found will not send ERROR email
    def chkIncFilesWarn(self):
        rc = self.getIncFiles()
        if rc != 0 : return RET_WARN
        else       : return rc

    # Check if approval file has arrive in order to continue with load.
    # Command Step.
    def checkApprovFile(self):
        fl = fu.getFileName(self.ib.landDir, self.ib.fileName)
        
        if len(fl) == 0:
            i = 1
            self.log.info('Approval File %s/%s does not exists. Will start in %s sec' % (self.ib.landDir, self.ib.fileName, self.ib.waitAppFile))
            wi = int(self.ib.waitFileAppIter)
            while (i <= wi):       
                time.sleep(float(self.ib.waitAppFile))
                fl = fu.getFileName(self.ib.landDir, self.ib.fileName)
                self.log.info('Iteration %d out of %d ' % (i, wi))
                
                if len(fl) > 0 : break
                else           : i += 1
                
        if len(fl) == 0 : return 1
        else: 
            rc = fu.delFile(fl[0])
            self.log.info('Removing %s , rc = %s ' % (fl[0], rc))
            return 0
        
    # flst file path list to verify.
    # Returns 0 if success, otherwise numbr of bad files. 
    def checkFileCols(self,flst,FLD_SEP,strp=' \t\n\r'):
        rc = 0
        for fnp in flst:
            bfnm   = fu.getFileBaseName(fnp)  
            colc   = self.ib.FileColCnt.get(bfnm)
            self.log.debug('fkey = %s colc= %s fp = %s ' %(bfnm,colc,fnp))
            if colc is not None:
                    x,b = fu.readCSV(fnp,colc,FLD_SEP,strp)
                    if len(b) < 1 : 
                        self.log.debug('Columns number [%d] match on file %s ' % (colc,fnp)) 
                    else :
                        rc += 1         
                        badf = '%s/%s%s.bad' % (self.ib.badDir,bfnm,su.getTimeSTamp())
                        f = fu.createFile(badf,b)
                        if f == 0 : self.log.error('Columns number (%d) did not match on %s.\nPlease see Bad file %s .' % (colc,fnp,badf))
                        else      :
                            self.log.error('Columns number (%d) did not match on %s.\n.COULD NOT  create file  %s .' % (colc,fnp,badf))
                            self.log.error('BAD Rows===========\n', b)     
            else:
                rc += 1
                self.log.error("Did not find Column Count for %s. Unable to verify Column Numbers !" % bfnm)   

        return rc
    
    # Get Trigger File.    
    # S.E. Sets self.trigFiles
    # Command Step.   
    def getTrigFiles(self):
        self.trigFiles = self.chkTrigFiles(self.ib.trigFiles)
        if len(self.trigFiles) == 0 : return 1
        return 0
    
    # Get Trigger File Date. Use this method if the trigger file have been created/modifed on the day the process run.
    # This method should be used e.g. a daily process create a trigger file, but you need to wait that daily process before 
    # triggering.(a trigger file for a previous day will exist)   
    # S.E. Sets self.trigFiles
    # Command Step.  
    def getTrigFilesDate(self):
        self.trigFiles = self.chkTrigFilesDate(self.ib.trigFiles)
        if len(self.trigFiles) == 0 : return 1
        return 0   
           
    # Get Incoming Files to Process.    
    # S.E. Sets self.incFiles
    # Command Step.   
    def getIncFiles(self):
        self.incFiles = self.checkIncFiles(self.ib.fileName)
        if len(self.incFiles) == 0 : return 1
        return 0
    
    # Get Incoming File set. All files need to be present to load.
    # This method will return all qualifying files for a given set.
    # A set should have at least 2 file patterns, otherwise use the getIncFiles method. # 
    # self.fileSet is a 2 dim array. It sorts the filename for processing.
    #  
    #  self.incFileSet [['$LANDING/Ap_scusrctl_20120914_1.txt', '$LANDING/Ap_scusrctl_20121015_3.txt', '$LANDING/Ap_scusrctl_20121016_3.txt'],
    #                   ['$LANDING/Ap_scg4000_20120914_1.txt', '$LANDING/Ap_scg4000_20121016_3.txt'],
    #                   ['$LANDING/Ap_scg4100_20120914_1.txt', '$LANDING/Ap_scg4100_20121016_3.txt'],
    #                   ['$LANDING/Ap_scudrldn_20120914_1.txt', '$LANDING/Ap_scudrldn_20121016_3.txt'],
    #                   ['$LANDING/Ap_scg520e_20120914_1.txt'],
    #                   ['$LANDING/Ap_sce5100_20120914_1.txt','$LANDING/Ap_sce5100_20121016_3.txt']] len = 6
    #
    # S.E. self.fileSet is a 2 dim array.
    # Command Step. 
    def getIncSetFiles(self):
        # File sets
        fnl = self.ib.fileName.split(',')
        self.log.info('filename Size= %d List = %s ' % (len(fnl), fnl))
        for fn in fnl:
            fs = self.checkIncFiles(fn)
            if len(fs) == 0 : return 1
            self.log.debug('fs = ', fs)
            self.incFileSet.append(sorted(fs))
        
        return 0
                   
    # Copy files that are ready to process.
    # Other method should mv the files.
    # Command Step. 
    def cpFileToWorkDir(self):
        self.workFiles  = []
        for src in self.incFiles:
            fn = fu.getFileBaseName(src)
            d = '%s/%s' % (self.ib.workDir, fn) 
            rc = fu.copyFile(src, d)
            if rc == 0 : self.workFiles.append(d)
            self.log.info('Copying file %s to %s rc= %s' % (src, d, rc))
        
        if len(self.workFiles) < 1 : return 1
        else                       : return 0
    
    # Copy files that are ready to process.
    # srcFile and incFiles need to be in the same order !
    # Command Step. self.srcFile
    def cpSrcToTgtFiles(self):
        self.workFiles  = []
        srcLen = len(self.srcFile)
        incLen = len(self.incFiles)
        if srcLen != incLen:
            self.log.error('srcFiles len = %d Does not match IncFiles len  %d' % (srcLen,incLen))
            self.log.debug('srcFiles = ',self.srcFile, '\tincFiles = ', self.incFiles)
            return 1 
        
        for i in range (srcLen):
            #fn = self.incFiles[i]
            d = '%s/%s' % (self.ib.workDir, self.srcFile[i]) 
            rc = fu.copyFile(self.incFiles[i], d)
            if rc == 0 : self.workFiles.append(d)
            self.log.info('Copying file %s to %s rc= %s' % (self.incFiles[i], d, rc))
        
        if len(self.workFiles) < 1 : return 2
        else                       : return 0
           
    # Move files that are ready to process.
    # Command Step. 
    def mvFileToWorkDir(self):
        self.workFiles  = []
        for src in self.incFiles:
            fn = fu.getFileBaseName(src)
            d = '%s/%s' % (self.ib.workDir, fn) 
            rc = fu.moveFile(src, d)
            if rc == 0 : self.workFiles.append(d)
            self.log.info('Moving file %s to %s rc= %s' % (src, d, rc))
        
        if len(self.workFiles) < 1 : return 1
        else                       : return 0
        
    # This moves triggerFile and append timestamp to keep time history
    # There shoud only be one trigger file.
    # Command Step. 
    def mvTrigFileToArchDir(self):
        rc = 0 
        for src in self.trigFiles:
            fn = fu.getFileBaseName(src)
            d = '%s/%s.%s' % (self.ib.archDir, fn, su.getTimeSTamp()) 
            r = fu.moveFile(src, d)
            if r != 0 : 
                rc += 1
                self.log.error ("Error moving trigger file  %s r = %s. " % (src, r))
            
            else      : self.log.info('Moving Trigger file %s to %s rc= %s' % (src, d, r))
            
        return rc    # Only reach this line if trigger file had been removed externa to the program !

    # List of files to archive.
    # Command Step. 
    # EO TODO Change to archIncFiles
    def archFiles(self)    : 
        rc = 0
        for src in self.incFiles:
            fn = fu.getFileBaseName(src)
            d = '%s/%s' % (self.ib.archDir, fn)
            r = fu.moveFile(src, d)
            if r != 0 : rc += 1
            self.log.info('Moving file %s to %s rc= %s' % (src, d, rc))
            zf = '%s/%s.%s' % (self.ib.archDir , fn, self.ib.archSuff)
            r = fu.compressFile(zf, d)
            if r != 0 : self.log.warn ("Cannot compresss %s r = %s. " % (zf, r))
            else      : self.log.info ("Compressed %s r = %s " % (zf, r))
        return rc
    
    # This method renames control files with a timestamp and archives control files.
    # fls : List with full path for files to be archived.
    # ts Timestamp
    # cf compress flag.
    def archGenFiles(self,fls,ts,cf=False)    : 
        rc = 0
        for src in fls:
            fn = fu.getFileBaseName(src)
            if ts == '' : d = '%s/%s' % (self.ib.archDir, fn) 
            else        : d = '%s/%s.%s' % (self.ib.archDir,ts, fn)
            r = fu.moveFile(src, d)
            if r != 0 : rc += 1
            self.log.info('Moving file %s to %s rc= %s' % (src, d, rc))
            
            if cf is True:
                zf = '%s/%s.%s' % (self.ib.archDir , fn, self.ib.archSuff)
                r = fu.compressFile(zf, d)
                if r != 0 : self.log.warn ("Cannot compresss %s r = %s. " % (zf, r))
                else      : self.log.info ("Compressed %s r = %s " % (zf, r))
        return rc
    
    def _remFiles(self, flp):
        rc = 0
   
        if len(flp) < 1 : 
            self.log.warn('File list to delete is empty !')
            return 1
        
        #fns = fl.split(',')
        for fnp in flp:
            r = fu.delFile(fnp)
            if r != 0 : 
                rc += 1
                self.log.error ("Could not delete  %s r = %s " % (fnp, r))
            else      :
                self.log.info ("Removed  %s r = %d " % (fnp, r))
        return rc
    
    # Remove Incoming Files (Use for watch files)
    # Command Step. 
    def remIncFiles(self):
        rc = self._remFiles(self.incFiles)
        self.log.debug('Removing ', self.incFiles, 'rc = ', rc)
        return rc
    
    # Command Step. 
    def remChkFiles(self):
        rc = self._remFiles(self.ib.chkFiles)
        self.log.debug('Removing ', self.ib.chkFiles, 'rc = ', rc)
        return rc
    
    # Command Step. 
    def remTrigFiles(self):
        rc = self._remFiles(self.trigFiles)
        self.log.debug('Removing ', self.trigFiles, 'rc = ', rc)
        return rc
    
    # Command Step. 
    def remWorkFiles(self):
        rc = self._remFiles(self.workFiles)
        self.log.debug('Removing ', self.workFiles, 'rc = ', rc)
        return rc  
    
    # To remove Files from self.ib.landDir directory.
    def remFiles(self):
        fl = fu.getFileName(self.ib.landDir, self.ib.fileName)
        
        if len(fl) < 1 :
            self.log.warn('Nothing to remove %s/%s' % (self.ib.landDir, self.ib.fileName))
            return 0
        
        fls = ','.join(fl)
        self.log.debug('fls = %s ' % fls)
        rc = self._remFiles(fls)
        return rc
 
    # Flag Setters
    def rWkfFlowFlg(self) : 
        self.runWkfFlowFlg  = True
        return 0
    
    #Set flags to verify number of columns.
    def chkFileColsFlg(self):
        self.checkFileColsFlag = True
        return 0

    # Verify line counts.
    def chkFileRowsFlg(self):
        self.checkFileRowsFlg = True
        return
         
           
    # Sets a flag, to check for next run.
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg = True
        return 0    
    
    def crtTrigFile(self):
        fn = '%s/%s' % (self.ib.landDir ,self.ib.trigfile)
        rc = fu.crtEmpFile(fn)    
        if rc == False :  
            self.log.error('Could not create file %s' % fn)
            return 1
        else          : 
            self.log.info('Created file %s ' % fn)
        return 0
    
    # Command Step
    # Use this method to Pivot self.incFileSet and copy elements to self.incFile
    # It will get the first element [0] of the bucket and will copy to  self.incFile
    # SE re-initializes  self.incFile

    def setIncFilePvt(self):
        rc = 1; 
        self.incFiles = []; i = 0
        while i < len(self.incFileSet):
            self.incFiles.append(self.incFileSet[i][0])
            i += 1
        
        if len(self.incFiles) > 0 : rc = 0
   
        return rc       
          
    # Workday specific.    
    def getWorkDay(self):
        self.log.debug('qry = %s ' % ds.workDay)
        rs   = self._getNZDS(ds.workDay)
        self.log.debug('rs ' , rs)
        if rs is None      : return -5
        if len(rs)    != 1 : return -1
        if len(rs[0]) != 4 : return -4
        return rs[0][03] 
    
    # Use this method to get workdays count from end of month.    
    # rs[(datetime.date(2014, 8, 25), )]
    def getLastWorkDay(self,lwd):
        mtyr = su.getTodayDtStr(fmt='%m%Y')
        m = mtyr[:2] ; y = mtyr[2:]  
        qry = ds.lastworkDay % (m,y,m,y,lwd)
        self.log.debug('qry = %s ' % qry)
        rs   = self._getNZDS(qry)
        self.log.debug('rs' , rs)
        if rs is None      : return None
        if len(rs)    != 1 : return -1
        if len(rs[0]) != 1 : return -4
        return rs[0][0] 
    
    def isWorkDay(self):
        rc = -2
        w = self.getWorkDay() 
        if w < 1 :
            self.log.error(' wkday = %s : Please check DB connectivity table' % w)
            return w
        rwd =  self.ib.workday.split(',')
        self.log.debug('self.ib.workday = %s dbWkday = %s len rdw = %d' % (self.ib.workday,w,len(rwd)))
        for wd in rwd:
            if w == su.toIntPos(wd) : return 0
        return rc
    
    def isWorkDayWarn(self):
        rc  = -2
        w   = self.getWorkDay()
        if w < 1 :
            self.log.error(' wkday = %s : Please check DB connectivity or Table' % w)
            return w
        rwd =  self.ib.workday.split(',')
        self.log.debug('self.ib.workday = %s dbWkday = %s len rdw = %d' % (self.ib.workday,w,len(rwd)))
        for wd in rwd:
            if w == su.toIntPos(wd) : return  0
            else                    : rc = RET_WARN
        return rc
     
    def isLastWorkDayWarn(self):
        rc = -2
        w  = self.getLastWorkDay(self.ib.lastworkday)
        if w is None :
            self.log.error(' wkday = %s : Please check DB connectivity table' % w)
            return -5 

        wd = su.getDtStr(w,'%Y-%m-%d')
        d=su.getTodayDtStr('%Y-%m-%d')
        self.log.debug('self.ib.lastworkday = %s date_day = %s today = %s' % (self.ib.lastworkday,w,d))
        if d == wd : rc = 0
        else       : rc = RET_WARN
        return rc

    #---------------------- Database Operations (high level)  --------------------------------------
   
    # Sets self.dbh 
    # dbEng : Database Engine connect Strings
    # CAUTION : Cannot have 2 open connections. Use this method for 1 open connection otherwise override in child class.
    def _connToDB(self, dbEng):
        cs = getDSConnStr(dbEng, self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        self.dbh = NZODBC(cs, self.log)
        rc = self.dbh.connToDB () 
        if rc != 0 : 
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return 1
        return rc
    
    
        # Disconnects from DB 
    def _disFromDB(self): self.dbh.closeDBConn()
    
    #  This method is used for DB Connections. 
    #  sql statement to execute.
    #  bv bind variables
    #  po sql operation. SEL otherwise INS, UPD, DEL
    #  This method returns 
    #     SUCCESS : a list (runQry) or a positive number (exeQry)
    #     ERROR   : None . 
    def _getNZDS(self, sql, bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('NZODBC', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = NZODBC(cs, self.log)
        rc  = dbh.connToDB () 
        if rc != 0 : 
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
        
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)

        dbh.closeDBConn()
        return r
     
    def _getMSSQLDS(self, sql,bv=[], po='SEL'): 
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('MSSQLODBC', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = MSSQLODBC(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
    def _getMSSQLNatDS(self, sql,bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('MSSQLNAT', self.ib.user, self.ib.pwd, self.ib.dbserver, self.ib.db)
        dbh = MSSQLNat(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
    def _getOracleDS(self, sql,bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('ORADB', self.ib.user, self.ib.pwd, self.ib.db)
        dbh = DBOracle(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r
    
    def _getDB2CliDS(self, sql,bv=[], po='SEL'):
        self.log.debug('qry = %s' % sql)
        cs = getDSConnStr('DB2CLI', self.ib.user, self.ib.pwd, self.ib.dsn, self.ib.db)
        dbh = DBOracle(cs, self.log)
        rc  = dbh.connToDB ()
        if rc != 0 :
            self.log.error('Could not connect to the DB rc = %s ' % rc)
            return None
         
        if po == 'SEL': r = dbh.runQry(sql)
        else          : r = dbh.exeQry(sql,bv)
        dbh.closeDBConn()
        return r        
    #---------------------- App Config File / Environment variables settings  --------------------------------------
    # Process App config file.
    # by convention file will be className with .cfg
    def _getConfigFile(self):
        fn = '%s/%s.%s' % (self.ib.cfgDir, self.appName,self.suf)
        self.log.info('Loading config file:%s' % fn)
        return fu.loadConfigFile(fn, self.ib, self.log)
    
    def _setDataDir(self):
        self.ib.landDir = '%s/%s' % (self.ib.shareDir, self.landDir)
        self.ib.archDir = '%s/%s/%s' % (self.ib.shareDir, self.landDir, self.ib.archDirName)
        self.ib.workDir = '%s/%s/%s' % (self.ib.shareDir, self.landDir, self.ib.workDirName)
        self.ib.badDir  = '%s/%s/%s' % (self.ib.shareDir, self.landDir, self.ib.badDirName)
        return 0
        
    def printEnvBean(self):
        ibr = vars(self.ib)
        for k, v in ibr.items():
            self.log.debug("k = %s\tv = %s" % (k, v))
          
    # Form host:pwd. returns h,pwd
    def _dPWX(self,x,ev):
        st = su.dec64(x)
        if self.ib.xpwd_dbg is True : self.log.debug('st = ' , st)
        if len(st) < 2            : 
            self.log.error('%s rc=%s. 1-Invalid, 2 Corrupted' % (ev,st[0]))
            if self.ib.xpwd_dbg is True : self.log.debug('%s : Pwd 1) Invalid was not generated using h:p pattern !\n 2) pwd was tampered string is not complete. Regen' % ev)
            return None
        if self.hostname != st[0] : 
            self.log.error('%s check host %s' % (ev,st[0]))
            if self.ib.xpwd_dbg is True : self.log.debug('%s : Current host %s does not match pattern h:p. Wronly generated or copied form another host' % (ev,self.hostname))
            return None 
        return st[1]
   
    def _initEnvVar(self):     
        xpwd_dbg      = os.environ['XPWD_DBG'] if os.environ.has_key('XPWD_DBG')  else 'False'
        if xpwd_dbg == 'True' : self.ib.xpwd_dbg = True
        
        if self.ib.rep_xpwd   is not None: self.ib.rep_pwd   = self._dPWX(self.ib.rep_xpwd   ,'REP_XPWD')
        if self.ib.xpwd       is not None: self.ib.pwd       = self._dPWX(self.ib.xpwd       ,'XPWD')    
        if self.ib.db_xpwd    is not None: self.ib.db_pwd    = self._dPWX(self.ib.db_xpwd    ,'DB_XPWD')    
        if self.ib.rep_dbxpwd is not None: self.ib.rep_dbpwd = self._dPWX(self.ib.rep_dbxpwd ,'REP_DBXPWD')
        if self.ib.dom_dbxpwd is not None: self.ib.dom_dbpwd = self._dPWX(self.ib.dom_dbxpwd ,'DOM_DBXPWD')   
    
    # Environment Variables that need to be set. Key is the ENV and ELE the name of var.
    # Below are global variables. Env variables should be set based on env settings.    
    def _getEnvVars(self):
        ret = 0   
        for ev, v in  self.infaEnvVar.items():
            self.log.debug("ev =%s v = %s" % (ev, v))
            
        try:     
            for ev, v in  self.infaEnvVar.items():
                x = os.environ[ev]
                exec  "%s='%s'" % (v, x) 
                self.log.debug("%s='%s'" % (v, x))  
                
            self._initEnvVar()                 
             
        except:
                ret = 2
                self.log.error("ENV var not set %s %s\n" % (sys.exc_type, sys.exc_info()[1]))
    
        finally : return ret
    
    # Use this method to set an environemnt variable from another. E.g. when you have more than one ftp site per program.
    # envVars : dict {ENV_TO_SET:ENV_SRC, }
    def setEnvVars(self,envVars):
        ret = 0   
        try:
            for ev,v in envVars.items():
                os.environ[ev] = os.environ[v]
                self.log.debug("os.environ[%s] = os.environ[%s] " % (ev,v))
        
        except:
                ret = 2
                self.log.error("ENV var not set %s %s\n" % (sys.exc_type, sys.exc_info()[1]))      
    
        finally : return ret 

    #---------------------- Miscellaneous utility methods  --------------------------------------
    # Process App ctl file.
    # by convention file will be className with .ctl
    # Control file is use to store previous load run.
    def _getCtlFile(self):
        fn = '%s/%s.ctl' % (self.ib.ctlDir, self.appName)
        if fu.fileExists(fn) is False:
            self.log.error('Control File:%s does not exist' % fn)
            return None
        
        self.log.info('Loading Control File:%s' % fn)
        return fu.readFile(fn).strip()
    
    # This method will get State Files and return the string.
    def getStateFile(self):
        fn = '%s/%s.state' % (self.ib.ctlDir, self.appName)
        if fu.fileExists(fn) is False:
            self.log.error('State File:%s does not exist' % fn)
            return None
        
        self.log.info('Reading State File :%s' % fn)
        return fu.readFile(fn).strip()
              
    # Use this notification for application specific notifications. Do not use for admin users, as you should use the 
    # sm.sendNotif which get it values from ENV VAR.  
    # This method gets it mail list from app user, out of the configuration file.
    def _notifyAppUsers(self, rd='plain'):

        if self.ib.touser is None and self.ib.pager is None:
            self.log.info ('No notification is set mail_list or/and pager_list is/are not set')
            return 0
        
        # Implicit boolean casting. 
        try: 
            m = -1 ; p = -1
            m = eval(self.ib.mailOnErr) ; p = eval(self.ib.pgOnErr)
        except:
            self.log.error('Error with configuration value(s) self.ib.mailOnErr = %s self.ib.pgOnErr = %s' % (m,p) )
        
        finally:  
            self.ib.mailOnErr = m
            self.ib.pgOnErr   = p
            
        rc = sm.notify(self.rc, self.ib.touser, self.ib.pager, self.subj, self.text, self.ib.mailOnErr, self.ib.pgOnErr, self.ib.fu, self.log, self.files, rd)
        self.log.info('Sending notification rc = %s == to mail = %s\tpager = %s\tsubj=%s\trender %s' % (rc, self.ib.touser, self.ib.pager, self.subj, rd))
        return rc
          
    #---------------------- Main / Command execution.  --------------------------------------
    
    def _execSteps(self, runStep):
        
        self.log.debug('runStep = %s' % runStep)
        for s in runStep:
            if (not self.cmdStep.has_key(s)):
                self.log.error('Invalid step %s' % s)
                return 1
            
            rv = 1
            try:
                rv = self.cmdStep[s]()
                if rv != 0 and self.exitOnError : 
                    self.log.error('[%s]:%s()\trc\t= %d' % (s, self.cmdStep[s].__name__, rv))
                    return rv
                
                #self.log.debug('[%s]:%s()\trc\t= %s' % (s,self.cmdStep[s].__name__,rv))
                self.log.info('[%s]:%s()\trc\t= %d' % (s, self.cmdStep[s].__name__, rv))
            
            except AttributeError:
                self.log.error('[%s]:%s()' % (s, self.cmdStep[s].__name__))
                self.log.error(' %s ' % (sys.exc_info()[1]))
                if (self.exitOnError) : return rv
                
            except SyntaxError:
                self.log.error('[%s]:%s()' % (s, self.cmdStep[s].__name__))
                self.log.error(' %s ' % (sys.exc_info()[1]))
                if (self.exitOnError) : return rv
            
        return rv

    #Set Incoming arguments 
    def setArgs(self,Argv):
        
        if len(Argv) != 2 :
            self.log.error("USAGE : <%s> fx [runSeq] Incorrect Number of arguments (%d)" % (Argv[0], len(Argv)))
            return 1  
        self.runSeq = Argv[1] 
        
        return 0
    # Argv is a list of runnable commands, defined per class basis
    # 'C:\\Users\\eocampo\\workspace\\rydinfap\\src\\apps\\infbaseapp.py'    
    def main(self, Argv):
        rc = 1  # Failed
        logFile = getLogHandler(self.appName, self.log)
        self.log.info('logFile= %s' % logFile)
        
        # should NEVER get this programmatic error !!!!
        if self.cmdStep is None or len(self.cmdStep) == 0 :
            self.log.error("Program Error:self.cmdStep is ", self.cmdStep)
            return rc
        
        rc = self.setArgs(Argv)
        if rc != 0 : return rc
        
        rc = self._getEnvVars()
        if rc != 0 :
            self.log.error("Need to set all env vars:\n %s" % self.infaEnvVar.keys())
            return rc
        
        rc = self._getConfigFile()
        if rc != 0 :
            self.log.error("Error Loading Config File:\n")
            return rc
        
        self._setDataDir()
           
        if self.runSeq is not None and len(self.runSeq) > 0 : 
            rc = self._execSteps(self.runSeq)
            if rc == 0 : self.log.info ('Completed running execSteps rc = %s' % rc)
            else       : self.log.error('execSteps rc = %s' % rc)
        
        
        if rc != RET_WARN:
            text = 'Please see logFile %s' % logFile
            subj = "SUCCESS running %s on %s " % (self.appName, self.hostname) if rc == 0 else "ERROR running %s on %s" % (self.appName, self.hostname)
            r, msg = sm.sendNotif(rc, self.log, subj, text, [logFile, ])
            self.log.info('Sending Notification\t%s rc = %s' % (msg, r))    
        
        else:
            self.log.info('Notification Overrided. Not sending message (RET_WARN) rc = %s ' % rc)
            
        self.printEnvBean()
        sys.exit(rc)
    
    def __del___(self):
        self.log.debug('Base class cleaning')
# Use this for testing in windows env only. Should not use in UX
    
if __name__ == '__main__':
    from setwinenv import setEnvVars
    os.environ['LOG_LEVEL'] = 'DEBUG'
    setEnvVars()
    bd = r'C:/apps'
    a = _InfaBaseApp(bd)
    rc = a.main(sys.argv)
    #rc = a.main(['cfg','ABCD'])
    #rc = a.main(['cfg', 'ABCD'])
