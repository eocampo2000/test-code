'''
Created on Nov 19, 2013

@author: eocampo

Talent Map application


General Information:
--------------------
1.    In total following 7 files will be received on daily basis. File names are standardized and will be in following format:

a.   All_Training_Providers_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
b.   All_Training_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
c.   Transcripts_OpsRegion_Canada_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
d.   Transcripts_OpsRegion_Central_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
e.   Transcripts_OpsRegion_Northeast_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
f.   Transcripts_OpsRegion_Southeast_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT
g.   Transcripts_OpsRegion_West_<BATCHID>_<MM>_<DD>_<YYYY>_<HH>_<MI>.TXT

2.   Data in the files will be tab delimited. There is always an extra tab at the end.

3.   Each file will have first 2 lines in following format acting as header information:

<file name>
#Records: <number of records>

4.   3rd row from the source files will be blank and 4th row is the header record with column names for each column.

5.   EDW process needs to pull the files from vendor FTP location (ftp://ftp.ryder.csod.com/Reports/APS/edwprocessed) and once the pull is successful, 
move them to a sub-directory on FTP location (ftp://ftp.ryder.csod.com/Reports/APS/edwprocessed/archived).

Checks and Balances:
--------------------
a.   All 7 files must be received.
b.   File name should have date in <MM>_<DD>_<YYYY> format embedded in file name itself and it must match the date of the day on which the process needs to run.
c.   Number of columns for each record must match the number of columns from header record.
d.   Total number of records from line 5 to EOF must match total number of records from the header line (line 2).


Handling Files for Single Day:
-----------------------------
1.    After the files are pulled from source FTP site, combine Transcripts_OpsRegion_*.TXT files after the checks and balances are done, and just keep the header record in each file.

2.    Save the source files with following names after merging them and removing header information:

a.    Transcripts_OpsRegion_*.TXT merge and only keep header row with column name Transcripts_OpsRegion.TXT
b.    All_Training_Providers_*.TXT only keep header row with column name All_Training_Providers_*.TXT
c.    All_Training_*.TXT  only keep header row with column name All_Training.TXT

Handling Customer Files (optional):
------------------------

1.    In total following 2 files will be received from business 
  a.    FMS_Qualifications_<YYYYMMDD>.csv
  b.    Tops_Plan_Current_<YYYYMMDD>.csv
2.    Data in above 2 files will be comma delimited.
3.    1st row is the header record with column names for each column.

20140205 Moved crtTrigFile to base class.
20140409 Added logic to create 0 byte files for optional Customer files, if files are not present. 
20151119 Added ftp file renaming as a step.

'''

__version__ = '20151119'

import sys
import os
#import time
from datetime import datetime
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import procdata.procinfa as pi 

from infbaseapp       import _InfaBaseApp
from infbaseutil      import _TalentMapFile

# Mandatory to define self.cmdStep

FLD_SEP = '\t'           # Field separator for CSV file.
   
class TalentMap(_InfaBaseApp,_TalentMapFile):  
    exitOnError = True
    
    def __init__(self):
        super(TalentMap,self).__init__()
        self.landDir    = 'SrcFiles/talentmap'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incFiles). 
     
        
        self.RowCnt      = -1
        self.ib.fld      = 'TalentMap'
        self.ib.wkf      = 'wkf_talentMap'    
        self.ib.srcFile  = ( 'All_Training.txt','All_Training_Providers.txt','Transcripts_OpsRegion_Canada.txt','Transcripts_OpsRegion_Central.txt','Transcripts_OpsRegion_Northeast.txt','Transcripts_OpsRegion_Southeast.txt','Transcripts_OpsRegion_West.txt')    # File that Informatica expects. Alphabetical.
        self.ib.FileColCnt = {'All_Training.txt':12,
                              'All_Training_Providers.txt':2,
                              'Transcripts_OpsRegion_Canada.txt':18,
                              'Transcripts_OpsRegion_Central.txt':18,
                              'Transcripts_OpsRegion_Northeast.txt':18,
                              'Transcripts_OpsRegion_Southeast.txt':18,
                              'Transcripts_OpsRegion_West.txt':18,
                              'Tops_Plan_Current.csv':13,
                              'FMS_Qualifications.csv':8, 
                               }
        self.ib.opsRegHdrRow  = ''    
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 4   
        self.procCort  = {}
        self.procBuReg = {}
        
        self.renameFtpFilesTgtFlg = False
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock            ,
                         'B' : self.renFtpFilesTgtFlg  ,                   
                         'C' : self.getTMapftpFiles    ,  # FTP Files based on regex.
                         'D' : self.getIncSetFiles     ,  # Populates self.incSetFiles. Incoming Files.  
                         'E' : self.setIncFilePvt      ,  # Run after setting self.incFiles
                         'F' : self.cpFileToWorkDir    ,  # Copies FileSet and sets self.workFiles (full path)
                         'G' : self.archFiles          ,
                         'H' : self.chkFileColsFlg     ,
                         'I' : self.preProcTMapFiles   ,  # Check that headers/record count, number of fields.
                         'J' : self.procTMapFiles      ,
                         'K' : self.procCustFiles      ,
                         'L' : self.crtTrigFile        ,
                         'M' : self.wkfTalentMapProc   ,                 
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
        
        
        self.fnd = su.getTodayDtStr('%m_%d_%Y')
        # FTP is expecting the following env variables, which should not be in a config file.
        #os.environ['RXFILE'   ] =  '.*_[0-9]*_[0-9]*_[0-9]*_[0-9]*_[0-9]*_[0-9]*.TXT'  # data file(s). Use for MGET
        os.environ['FILE'     ] =  ('Name of control file')                            # data file(s). Use for GET
        os.environ['RXFILE'   ] = 'Transcripts_OpsRegion_.*_[0-9]*_%s_[0-9]*_[0-9]*.TXT' % self.fnd  # data file(s). Use for MGET

    # Get the files from the remote host (source system).
    # Placed in the self.landDir in local host
    # Move files in remote host from . to edwprocess dir.
    def getTMapftpFiles(self):

        rxFiles = []                              # List which contains the new file names.
        ftp = ft.Ftp("TalentMap",self.log)
        rc  = ftp.connect()
        self.log.info("Connect rc = %s" % rc)
        if rc != 0 : return rc
        rc  = ftp.login()
        self.log.info("Login  rc = %s" % rc)
        if rc != 0 : return rc
        ftp.ftpSetPasv(False)
        self.log.info("Setting to Active Mode")
        rc = ftp.ftpServCwd()
        self.log.info("CWD  rc = %s" % rc)
        if rc != 0 : return rc
        # Get Transcripts_OpsRegion*
        rc = ftp.ftpMGet()
        if rc != 0 : return rc
        self.log.info("MGet Data Files rc = %s " % rc)
        # Get All_Training*
        ftp.rxfile='All_Training.*_[0-9]*_%s_[0-9]*_[0-9]*.TXT' % self.fnd
        rc = ftp.ftpMGet()
        if rc != 0 : return rc
        self.log.info("MGet Data Files rc = %s " % rc)

        if self.renameFtpFilesTgtFlg is True:
            txFiles = ftp.getTXFiles()
            self.log.debug("TXF Files " , txFiles)
            
            # Build a list of tuples w src/tgt file names.
            for sfn in txFiles:
                tfn = 'edwprocessed/%s%s' % (self.ts,sfn)
                self.log.debug('Adding to list to rename in remote host  %s to %s' %(sfn,tfn))
                rxFiles.append((sfn,tfn))
    
            if len(rxFiles) < 1 :
                self.log.error('No files to FTP were found in remote host !')
                return 1
        
            rc = ftp.ftpRenFile(rxFiles)
            self.log.info("Rename r = %s " % rc)
        
        ftp.disconnect() 
        self.log.info("Disconnecting = %s" % rc)
        return rc
          
    # This method will:
    # Ensure that all files exist and will check for number of records. 
    # dataFiles contains full filename in working dir.
    # Arbitrarily getting the hdr from 2nd file in list. All should be identical by contract.
    # SE sets self.ib.opsRegHdrRow for Common Header Row
    def preProcTMapFiles(self):
        
        rc = 0 ; tgtFiles = []                          
        dataFile = self.chkHeader(self.workFiles)
        dfl      = len(dataFile)
        if dfl != len(self.ib.srcFile):
            self.log.error('File Set to process (%d) is not complete. Expecting a total of %d. Check config file!' % (dfl,len(self.ib.srcFile)))
            self.log.debug('datafiles to process = ', dataFile)
            return 1

        # Set Header row for OPS Transcripts File(s).                 
        self.log.debug('Getting hdr from file %s' % self.workFiles[2])
        self.ib.opsRegHdrRow = self.getHdrRow(self.workFiles[2])
        if self.ib.opsRegHdrRow is None or len(self.ib.opsRegHdrRow) < 50 :
            self.log.error('File %s does not exists or Hdr is too small hdr = %s'  % (self.workFiles[2],self.ib.opsRegHdrRow))
            return 1
        
        i = 0
        while i < dfl:
            
            tgt = '%s/%s' % (self.ib.workDir,self.ib.srcFile[i])
            
            rc = fu.delFile(tgt)
            self.log.debug("Removing %s\trc=%s" %(tgt,rc))
            
            rc = fu.moveFile(dataFile[i],tgt)
            self.log.info('Moving File From %s\t\t\n TO %s' %  (dataFile[i],tgt))
            if rc != 0 :
                self.log.error("cannot move File from %s to %s" % (dataFile[i],tgt))
                return rc
            
            if i < 2: offset = _TalentMapFile.HDR_ROW - 1     # Remove first 3 rows for all Transcript Files. Keep HDR
            else    : offset = _TalentMapFile.HDR_ROW         # Remove first 4 rows for all OPS Training Files
            
            rc = fu.remFileLine(tgt,offset)
            self.log.debug('Removing %d lines from %s' % (offset,tgt))
            
            if rc != 0 :
                self.log.error('Issue removing lines on %s ' % tgt)      
                return rc  
            
            tgtFiles.append(tgt)
            
            i+=1
        
        #Verify Column Numbers:
#         if self.checkFileColsFlag is True:
#             rc = self.checkFileCols(tgtFiles,FLD_SEP,' \n\r')
#             if rc != 0:
#                 self.log.error('Issue with column number. PLease check bad directory under %s' % self.ib.badDir)
#                 return rc    
        
        return rc
    
    # Process Transcripts_OpsRegion Files.
    def _procOpsReg(self):

        concatFls = ('Transcripts_OpsRegion.txt','Transcripts_OpsRegion_Canada.txt','Transcripts_OpsRegion_Central.txt','Transcripts_OpsRegion_Northeast.txt','Transcripts_OpsRegion_Southeast.txt','Transcripts_OpsRegion_West.txt')
        concatFlsFP = []
        for fn in concatFls: concatFlsFP.append('%s/%s' % (self.ib.workDir,fn))
        self.log.debug('concatFlsFP = ', concatFlsFP)
        
        # Remove Concatenated file:
        rc = fu.delFile(concatFlsFP[0])
        self.log.debug("Removing %s\trc=%s" %(concatFlsFP[0],rc)) 
        
        self.log.debug('Creating HDR file %s' % concatFlsFP[0])
        rc   = fu.createFile(concatFlsFP[0], self.ib.opsRegHdrRow)
        if rc != 0 :
            self.log.error('Could not create file %s'  % (concatFlsFP[0]))
            return 2
                    
        # Concatenate the files
        rc = fu.concatFile(concatFlsFP)
        self.log.info('concatFile rc = %s' % rc)
            
        return rc
    
    # At this point everything matches, will need to remove 3 first lines in everyfile.    
    def procTMapFiles(self):
        
        rc = self._procOpsReg()
        if rc != 0: return rc 
        return rc
    
    #This method process optional files. If all columns do not match then will remove file from input directory,
    def procCustFiles(self):
        tgtFiles = [] 
        
        fnl = self.ib.custFileName.split(',')
        for fnp in fnl:
            fs  = self.checkIncFiles(fnp)
            if len(fs) == 1:
                fnOff = len('_YYYYMMDD.csv')
                self.log.info('Processing %s ' % fs[0])
                fn = fu.getFileBaseName(fs[0])
                d = '%s/%s' % (self.ib.archDir, fn)
                rc = fu.copyFile(fs[0], d) 
                self.log.info('Archiving %s to %s\trc=%s' % (fs[0],d,rc))   
                d = '%s/%s.csv' % (self.ib.workDir,fn[:-fnOff])   
                rc = fu.moveFile(fs[0], d)
                self.log.info('Moving %s to %s\trc=%s' % (fs[0],d,rc))                   
                tgtFiles.append(d)            
            else:
                fnOff = len('_*[0-9].csv')
                fn = fu.getFileBaseName(fnp)
                d   = '%s/%s.csv' % (self.ib.workDir,fn[:-fnOff]) 
                self.log.debug('fn = %s, dest = %s' % (fn,d))
                self.log.warn('File %s was not found' % fnp)
                rc = fu.crtEmpFile(d)
                self.log.info('Creating 0 byte file %s\trc=%s' % (d,rc) )

        #Verify Column Numbers:
        if self.checkFileColsFlag is True:
            rc = self.checkFileCols(tgtFiles,',')
            if rc != 0:
                self.log.error('Issue with column number. PLease check bad directory under %s' % self.ib.badDir)
                # Implement email notification.
                # return rc                  

        return 0
    
    # Workflow execution.
    def wkfTalentMapProc(self):
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
                
def main(Args):
    a = TalentMap()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()      
    rc=  main(sys.argv)
    sys.exit(rc)
        



